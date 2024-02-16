use crate::{
    db_state::ScndIdxNum,
    ds_n_a::{
        atomic_linked_list::{ListNode, ListSnapshot},
        send_ptr::NonNullSendPtr,
    },
    lsm::{
        entryset::merging,
        unit::{
            CommitDataType, CommitInfo, CommitVer, CommittedUnit, CompactedUnit, ReplacementNum,
        },
        LsmElem,
    },
    opers::sicr::{ScndIdxCreationJob, ScndIdxCreationJobDir},
};
use anyhow::Result;
use pancake_engine_common::{fs_utils, merging as common_merging, Entry, SSTable};
use pancake_types::{
    iters::KeyValueReader,
    serde::OptDatum,
    types::{PKShared, PVShared, PrimaryKey, SVPKShared, Ser, SubValueSpec},
};
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::Arc;

/// The period is exaggeratedly small, so as to be helpful with debugging.
/// In the future, we'll allow setting it from an env var.
const MEMTABLE_FLUSH_PERIOD_ITEM_COUNT: usize = 5;

impl<'job> ScndIdxCreationJob<'job> {
    pub(super) fn create_unit(
        &mut self,
        snap_head: NonNullSendPtr<ListNode<LsmElem>>,
        sv_spec: &SubValueSpec,
        si_num: ScndIdxNum,
        output_commit_ver: CommitVer,
    ) -> Result<Option<CommittedUnit>> {
        let snap = ListSnapshot::new_tailless(snap_head);

        let prim_entries = Self::derive_prim_entries(&snap);

        let scnd_entries = Self::derive_scnd_entries(prim_entries, sv_spec);

        let interm_file_paths = self.create_all_intermediary_files(scnd_entries)?;

        let compacted_unit = self.merge_intermediary_files(interm_file_paths, si_num)?;

        self.remove_intermediary_files()?;

        if let Some(compacted_unit) = compacted_unit {
            let committed_unit =
                Self::convert_output_to_committed_unit(compacted_unit, output_commit_ver)?;
            return Ok(Some(committed_unit));
        }

        return Ok(None);
    }

    fn derive_prim_entries<'snap>(
        snap: &'snap ListSnapshot<LsmElem>,
    ) -> impl Iterator<Item = Entry<'snap, PKShared, PVShared>> {
        let prim_entrysets = snap
            .iter_excluding_head_and_tail()
            .filter_map(|elem| match elem {
                LsmElem::Dummy { .. } => None,
                LsmElem::CommittedUnit(unit) => unit.prim.as_ref(),
            });
        let prim_entries = merging::merge_committed_entrysets(
            prim_entrysets,
            None::<&PrimaryKey>,
            None::<&PrimaryKey>,
        );
        let nontomb_prim_entries = prim_entries.filter_map(|entry| entry.to_option_entry());
        nontomb_prim_entries
    }

    fn derive_scnd_entries<'snap>(
        prim_entries: impl 'snap + Iterator<Item = Entry<'snap, PKShared, PVShared>>,
        sv_spec: &'snap SubValueSpec,
    ) -> impl 'snap + Iterator<Item = Result<(SVPKShared, PVShared)>> {
        let nontomb_scnd_entries =
            prim_entries.filter_map(|prim_entry| match prim_entry.try_borrow() {
                Err(e) => return Some(Err(e)),
                Ok((pk, pv)) => match sv_spec.extract(pv) {
                    None => return None,
                    Some(sv) => {
                        let pk = Arc::clone(pk);
                        let pv = Arc::clone(pv);
                        let svpk = SVPKShared { sv, pk };
                        return Some(Ok((svpk, pv)));
                    }
                },
            });
        nontomb_scnd_entries
    }

    fn create_all_intermediary_files<'a>(
        &mut self,
        scnd_entries: impl 'a + Iterator<Item = Result<(SVPKShared, PVShared)>>,
    ) -> Result<Vec<PathBuf>> {
        let mut memtable = BTreeMap::new();

        let mut interm_file_paths = vec![];

        for res_scnd in scnd_entries {
            let (svpk, pv) = res_scnd?;

            memtable.insert(svpk, pv);

            if memtable.len() >= MEMTABLE_FLUSH_PERIOD_ITEM_COUNT {
                let interm_file_path = self.create_one_intermediary_file(&memtable)?;
                interm_file_paths.push(interm_file_path);

                memtable.clear();
            }
        }

        if memtable.len() > 0 {
            let interm_file_path = self.create_one_intermediary_file(&memtable)?;
            interm_file_paths.push(interm_file_path);
        }

        Ok(interm_file_paths)
    }

    fn create_one_intermediary_file(
        &mut self,
        memtable: &BTreeMap<SVPKShared, PVShared>,
    ) -> Result<PathBuf> {
        let job_dir = self.ensure_create_job_dir()?;
        let interm_file_path = job_dir.format_new_kv_file_path();
        let interm_file = fs_utils::open_file(
            &interm_file_path,
            OpenOptions::new().create(true).write(true),
        )?;
        let mut w = BufWriter::new(interm_file);

        for (svpk, pv) in memtable.iter() {
            svpk.ser(&mut w)?;
            pv.ser(&mut w)?;
        }

        Ok(interm_file_path)
    }

    fn ensure_create_job_dir(&mut self) -> Result<&ScndIdxCreationJobDir> {
        if self.job_dir.is_none() {
            let job_dir = self.db.si_cr_dir().create_new_job_dir()?;
            self.job_dir = Some(job_dir);
        }
        let job_dir = self.job_dir.as_ref().unwrap();
        Ok(job_dir)
    }

    fn merge_intermediary_files(
        &mut self,
        interm_file_paths: Vec<PathBuf>,
        si_num: ScndIdxNum,
    ) -> Result<Option<CompactedUnit>> {
        if interm_file_paths.len() > 0 {
            let unit_dir = self.db.lsm_dir().format_new_unit_dir_path();
            let mut compacted_unit = CompactedUnit::new_empty(unit_dir)?;

            let sstable_path = compacted_unit.dir.format_scnd_file_path(si_num);

            /* Note, we wrote as (svpk, pv), and
            now we're reading as (svpk, optdat<pv>). This is valid. */
            let sstable;
            if interm_file_paths.len() == 1 {
                fs_utils::rename_file(interm_file_paths.first().unwrap(), &sstable_path)?;
                sstable = SSTable::<SVPKShared, OptDatum<PVShared>>::load(sstable_path)?;
            } else {
                let entry_iters = interm_file_paths
                    .into_iter()
                    .map(|path| {
                        let interm_file = fs_utils::open_file(path, OpenOptions::new().read(true))?;
                        let iter =
                            KeyValueReader::<_, SVPKShared, OptDatum<PVShared>>::from(interm_file)
                                .into_iter_kv();
                        Ok(iter)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let entries = common_merging::merge_entry_iters(entry_iters.into_iter());
                let entries = entries.map(Entry::Own);
                sstable = SSTable::new(entries, sstable_path)?;
            }

            compacted_unit.scnds.insert(si_num, sstable);

            return Ok(Some(compacted_unit));
        } else {
            return Ok(None);
        }
    }

    fn remove_intermediary_files(&mut self) -> Result<()> {
        if let Some(job_dir) = self.job_dir.take() {
            job_dir.remove_dir()?;
        }
        Ok(())
    }

    fn convert_output_to_committed_unit(
        compacted_unit: CompactedUnit,
        output_commit_ver: CommitVer,
    ) -> Result<CommittedUnit> {
        let commit_info = CommitInfo {
            commit_ver_hi_incl: output_commit_ver,
            commit_ver_lo_incl: output_commit_ver,
            replacement_num: ReplacementNum::FOR_NEW_COMMIT_VER_INTERVAL,
            data_type: CommitDataType::SSTable,
        };

        let committed_unit = CommittedUnit::from_compacted(compacted_unit, commit_info)?;

        Ok(committed_unit)
    }
}
