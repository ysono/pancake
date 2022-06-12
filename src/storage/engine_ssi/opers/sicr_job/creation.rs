use crate::ds_n_a::atomic_linked_list::{AtomicLinkedListSnapshot, ListNode};
use crate::ds_n_a::send_ptr::SendPtr;
use crate::storage::engine_ssi::{
    db_state::ScndIdxNum,
    lsm_state::{
        entryset::{merging, CommittedEntrySet},
        unit::{
            unit_utils, CommitDataType, CommitInfo, CommitVer, CommittedUnit, CompactedUnit,
            TimestampNum,
        },
        LsmElem, LsmElemContent, LIST_VER_PLACEHOLDER,
    },
    opers::sicr_job::{ScndIdxCreationJob, ScndIdxCreationRequest, ScndIdxCreationWork},
};
use crate::storage::engines_common::{
    fs_utils::{self, PathNameNum},
    Entry, SSTable,
};
use crate::storage::serde::OptDatum;
use crate::storage::types::{PKShared, PVShared, PrimaryKey, SVPKShared, SubValueSpec};
use anyhow::Result;
use itertools::Itertools;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicPtr, Ordering};

impl ScndIdxCreationJob {
    pub(super) async fn create(&self, work: ScndIdxCreationWork) {
        let res = self.do_create(&work).await;

        self.notify_end_of_work(work.snap_head_excl);

        work.req.response_to_client.send(res).ok();
    }

    async fn do_create(&self, work: &ScndIdxCreationWork) -> Result<()> {
        let ScndIdxCreationWork {
            snap_head_excl,
            output_commit_ver,
            req,
        } = work;
        let ScndIdxCreationRequest {
            sv_spec,
            scnd_idx_num,
            response_to_client: _,
        } = req;

        let prim_entries = Self::derive_prim_entries(*snap_head_excl);
        let scnd_entries = Self::derive_scnd_entries(prim_entries, &sv_spec);

        self.reset_working_dir()?;
        let intermediary_sstables = self.create_intermediary_sstables(scnd_entries)?;

        if !intermediary_sstables.is_empty() {
            let compacted_unit =
                self.create_compacted_unit(intermediary_sstables, *scnd_idx_num)?;
            let committed_unit =
                Self::convert_to_committed_unit(compacted_unit, *output_commit_ver)?;
            let node = unit_utils::new_unit_node(committed_unit, LIST_VER_PLACEHOLDER);
            self.insert_node(*snap_head_excl, node).await;

            self.reset_working_dir()?;
        }

        self.mark_completion(&sv_spec).await?;

        Ok(())
    }

    fn derive_prim_entries<'a>(
        snap_head_excl: SendPtr<ListNode<LsmElem>>,
    ) -> impl Iterator<Item = Entry<'a, PKShared, PVShared>> {
        let snap = AtomicLinkedListSnapshot {
            head_excl_ptr: snap_head_excl,
            tail_excl_ptr: None,
        };
        let prim_entrysets = snap
            .iter()
            .filter_map(|elem| match &elem.content {
                LsmElemContent::Dummy { .. } => None,
                LsmElemContent::Unit(unit) => Some(unit),
            })
            .filter_map(|unit| unit.prim.as_ref());
        let merged_prim_entries = merging::merge_committed_entrysets(
            prim_entrysets,
            None::<&PrimaryKey>,
            None::<&PrimaryKey>,
        );
        let nontomb_prim_entries = merged_prim_entries.filter_map(|entry| entry.to_option_entry());
        nontomb_prim_entries
    }

    fn derive_scnd_entries<'a>(
        prim_entries: impl 'a + Iterator<Item = Entry<'a, PKShared, PVShared>>,
        sv_spec: &'a SubValueSpec,
    ) -> impl 'a + Iterator<Item = Entry<'a, SVPKShared, PVShared>> {
        let scnd_entries = prim_entries.filter_map(|prim_entry| match prim_entry.try_borrow() {
            Err(e) => return Some(Entry::Own(Err(e))),
            Ok((_, pv)) => match sv_spec.extract(pv) {
                None => return None,
                Some(sv) => match prim_entry.take_kv() {
                    Err(e) => return Some(Entry::Own(Err(e))),
                    Ok((pk, pv)) => {
                        let svpk = SVPKShared { sv, pk };
                        return Some(Entry::Own(Ok((svpk, pv))));
                    }
                },
            },
        });
        scnd_entries
    }

    fn reset_working_dir(&self) -> Result<()> {
        fs::create_dir_all(&self.working_dir)?;
        for res_sub in fs_utils::read_dir(&self.working_dir)? {
            let sub = res_sub?;
            let meta = fs::metadata(&sub)?;
            if meta.is_file() {
                fs::remove_file(sub)?;
            } else {
                fs::remove_dir_all(sub)?;
            }
        }
        Ok(())
    }

    fn create_intermediary_sstables<'a>(
        &'a self,
        scnd_entries: impl Iterator<Item = Entry<'a, SVPKShared, PVShared>>,
    ) -> Result<Vec<SSTable<SVPKShared, PVShared>>> {
        const FLUSH_PERIOD: usize = 5;

        let memtable = RefCell::new(BTreeMap::<SVPKShared, PVShared>::new());
        let mut sstables = vec![];

        let mut flush = || -> Result<()> {
            let file_num = PathNameNum::from(sstables.len() as u64);
            let file_path = self.working_dir.join(file_num.format_hex());
            let mt = memtable.replace(BTreeMap::new());
            let scnd_entries = mt
                .into_iter()
                .map(|(svpk, pv)| Entry::Own(Ok((svpk, OptDatum::Some(pv)))));
            let sstable = SSTable::new(scnd_entries, file_path)?;
            sstables.push(sstable);
            Ok(())
        };

        for (i, scnd_entry) in scnd_entries.enumerate() {
            let (svpk, pv) = scnd_entry.take_kv()?;
            memtable.borrow_mut().insert(svpk, pv);

            if i % FLUSH_PERIOD == FLUSH_PERIOD - 1 {
                flush()?;
            }
        }
        if !memtable.borrow().is_empty() {
            flush()?;
        }

        Ok(sstables)
    }

    fn create_combined_sstable(
        intermediary_sstables: Vec<SSTable<SVPKShared, PVShared>>,
        out_file_path: PathBuf,
    ) -> Result<SSTable<SVPKShared, PVShared>> {
        let entries_iters = intermediary_sstables
            .iter()
            .map(|sstable| sstable.get_range(None::<&SVPKShared>, None::<&SVPKShared>));
        let merged_res_kvs = entries_iters.kmerge_by(|a_res, b_res| {
            match (a_res, b_res) {
                (Err(_), _) => true,
                (_, Err(_)) => false,
                (Ok((a_svpk, _)), Ok((b_svpk, _))) => {
                    a_svpk < b_svpk
                    // There is no duplicate svpk, hence equality never happens.
                }
            }
        });
        let merged_entries = merged_res_kvs.map(Entry::Own);

        let combined_sstable = SSTable::new(merged_entries, out_file_path)?;

        Ok(combined_sstable)
    }

    fn create_compacted_unit(
        &self,
        intermediary_sstables: Vec<SSTable<SVPKShared, PVShared>>,
        scnd_idx_num: ScndIdxNum,
    ) -> Result<CompactedUnit> {
        let new_unit_dir = self.db.lsm_dir().format_new_unit_dir_path();
        let mut compacted_unit = CompactedUnit::new_empty(new_unit_dir)?;
        let combined_sstable_path = compacted_unit.dir.format_scnd_path(scnd_idx_num);
        let combined_sstable =
            Self::create_combined_sstable(intermediary_sstables, combined_sstable_path)?;
        let out_entryset = CommittedEntrySet::SSTable(combined_sstable);
        compacted_unit
            .scnds
            .entry(scnd_idx_num)
            .or_insert(out_entryset);
        Ok(compacted_unit)
    }

    fn convert_to_committed_unit(
        compacted_unit: CompactedUnit,
        commit_ver: CommitVer,
    ) -> Result<CommittedUnit> {
        let commit_info = CommitInfo {
            commit_ver_hi_incl: commit_ver,
            commit_ver_lo_incl: commit_ver,
            timestamp_num: TimestampNum::from(0),
            data_type: CommitDataType::SSTable,
        };

        let committed_unit = CommittedUnit::from_compacted(compacted_unit, commit_info)?;
        Ok(committed_unit)
    }

    async fn insert_node(
        &self,
        snap_head_excl: SendPtr<ListNode<LsmElem>>,
        mut node_own: Box<ListNode<LsmElem>>,
    ) {
        {
            let lsm_state = self.db.lsm_state().lock().await;

            node_own.elem.traversable_list_ver_lo_incl = lsm_state.curr_list_ver;
        }

        let snap_head_excl = unsafe { snap_head_excl.as_ref() };
        let x_ptr = snap_head_excl.next.load(Ordering::SeqCst);
        node_own.next = AtomicPtr::new(x_ptr);
        let node_ptr = Box::into_raw(node_own);
        snap_head_excl.next.store(node_ptr, Ordering::SeqCst);
    }

    async fn mark_completion(&self, sv_spec: &SubValueSpec) -> Result<()> {
        let mut db_state = self.db.db_state().write().await;

        db_state.set_scnd_idx_as_readable(sv_spec)?;

        Ok(())
    }

    fn notify_end_of_work(&self, snap_head_excl: SendPtr<ListNode<LsmElem>>) {
        let snap_head_excl = unsafe { snap_head_excl.as_ref() };
        if let LsmElemContent::Dummy { is_fence, .. } = &snap_head_excl.elem.content {
            is_fence.store(false, Ordering::SeqCst);
        }

        self.db.replace_avail_tx().send(()).ok();
        /* Even if snap_head_excl's hold_count is not zero, notify F+C anyway, because some dummies in SICr's snapshot might have become non-held. For such dummies, notification would have been sent to F+C but F+C would not have worked across those dummies. */
    }
}
