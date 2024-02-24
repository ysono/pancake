use crate::opers::sicr::ScndIdxCreationJob;
use anyhow::Result;
use pancake_engine_common::{fs_utils, merging};
use pancake_types::{
    iters::KeyValueReader,
    serde::OptDatum,
    types::{PKShared, PVShared, SVPKShared, Ser},
};
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::BufWriter;
use std::path::PathBuf;

/// The period is exaggeratedly small, so as to be helpful with debugging.
/// In the future, we'll allow setting it from an env var.
const MEMTABLE_FLUSH_PERIOD_ITEM_COUNT: usize = 5;

impl<'job> ScndIdxCreationJob<'job> {
    pub(super) fn create_unit(&mut self) -> Result<Option<PathBuf>> {
        let scnd_entries = self.derive_scnd_entries()?;

        let interm_file_paths = self.create_all_intermediary_files(scnd_entries)?;

        let merged_file_path = self.merge_intermediary_files(interm_file_paths)?;

        Ok(merged_file_path)
    }

    fn derive_scnd_entries<'snap>(
        &'snap self,
    ) -> Result<impl 'snap + Iterator<Item = Result<(SVPKShared, PVShared)>>> {
        let mut prim_entrysets = vec![];
        for pi_file_path in self.prim_entryset_file_paths.iter() {
            let pi_file = fs_utils::open_file(pi_file_path, OpenOptions::new().read(true))?;
            let reader = KeyValueReader::<_, PKShared, OptDatum<PVShared>>::from(pi_file);
            let iter = reader.into_iter_kv();
            prim_entrysets.push(iter);
        }
        let prim_entries = merging::merge_entry_iters(prim_entrysets.into_iter());
        let nontomb_scnd_entries = prim_entries.filter_map(|res_pk_pv| match res_pk_pv {
            Err(e) => Some(Err(e)),
            Ok((_pk, OptDatum::Tombstone)) => None,
            Ok((pk, OptDatum::Some(pv))) => match self.sv_spec.extract(&pv) {
                None => None,
                Some(sv) => {
                    let svpk = SVPKShared { sv, pk };
                    Some(Ok((svpk, pv)))
                }
            },
        });
        Ok(nontomb_scnd_entries)
    }

    fn create_all_intermediary_files<'a>(
        &self,
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
        &self,
        memtable: &BTreeMap<SVPKShared, PVShared>,
    ) -> Result<PathBuf> {
        let interm_file_path = self.job_dir.format_new_kv_file_path();
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

    fn merge_intermediary_files(
        &self,
        mut interm_file_paths: Vec<PathBuf>,
    ) -> Result<Option<PathBuf>> {
        if interm_file_paths.len() > 0 {
            let merged_file_path;
            if interm_file_paths.len() == 1 {
                merged_file_path = interm_file_paths.pop().unwrap();
            } else {
                let entry_iters = interm_file_paths
                    .into_iter()
                    .map(|path| {
                        let interm_file = fs_utils::open_file(path, OpenOptions::new().read(true))?;
                        let iter = KeyValueReader::<_, SVPKShared, PVShared>::from(interm_file)
                            .into_iter_kv();
                        Ok(iter)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let entries = merging::merge_entry_iters(entry_iters.into_iter());

                merged_file_path = self.job_dir.format_new_kv_file_path();
                let mut merged_file = fs_utils::open_file(
                    &merged_file_path,
                    OpenOptions::new().create(true).write(true),
                )?;
                for entry in entries {
                    let (svpk, pv) = entry?;
                    svpk.ser(&mut merged_file)?;
                    pv.ser(&mut merged_file)?;
                }
            };

            Ok(Some(merged_file_path))
        } else {
            Ok(None)
        }
    }
}
