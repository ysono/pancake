use crate::{
    db_state::ScndIdxState,
    lsm::{
        entryset::{merging, CommittedEntrySet},
        unit::{CommitDataType, CommitInfo, CommittedUnit, CompactedUnit, TimestampNum},
    },
    opers::fc_job::FlushingAndCompactionJob,
};
use anyhow::Result;
use pancake_engine_common::{Entry, SSTable};
use pancake_types::{serde::OptDatum, types::Deser};
use std::cmp;

impl FlushingAndCompactionJob {
    /// A given slice should be compacted iff any of:
    /// - The slice contains 1+ MemLogs.
    /// - The slice contains 2+ SSTables.
    fn should_slice_be_compacted<'a>(units: &Vec<&'a CommittedUnit>) -> bool {
        (units.len() >= 2)
            || (units
                .iter()
                .any(|unit| unit.commit_info.data_type() == &CommitDataType::MemLog))
    }

    /// Returns `Some(_)` iff all of:
    /// - It was decided to go ahead with compaction
    /// - The compaction resulted in a non-empty CommittedUnit, ie non-empty SSTable for at least one index.
    ///
    /// If the returned value is None, it does not mean the given units contained no data.
    /// Therefore, do _not_ assume that the give units can be cut!
    pub(super) async fn do_flush_and_compact<'a>(
        &'a self,
        units: Vec<&'a CommittedUnit>,
        skip_tombstones: bool,
    ) -> Result<Option<CommittedUnit>> {
        if !Self::should_slice_be_compacted(&units) {
            return Ok(None);
        }

        let new_unit_dir = self.db.lsm_dir().format_new_unit_dir_path();
        let mut compacted_unit = CompactedUnit::new_empty(new_unit_dir)?;

        let db_state = self.db.db_state().read().await;

        for (_, ScndIdxState { scnd_idx_num, .. }) in db_state.scnd_idxs().iter() {
            let existing_entrysets = units.iter().filter_map(|unit| unit.scnds.get(scnd_idx_num));
            let compacted_entries = Self::derive_kmerged_iter(existing_entrysets, skip_tombstones);

            let mut compacted_entries = compacted_entries.peekable();
            if let Some(_) = compacted_entries.peek() {
                let out_path = compacted_unit.dir.format_scnd_file_path(*scnd_idx_num);

                let out_sstable = SSTable::new(compacted_entries, out_path)?;

                let out_entryset = CommittedEntrySet::SSTable(out_sstable);

                compacted_unit
                    .scnds
                    .entry(*scnd_idx_num)
                    .or_insert(out_entryset);
            }
        }

        {
            let existing_entries = units.iter().filter_map(|unit| unit.prim.as_ref());
            let compacted_entries = Self::derive_kmerged_iter(existing_entries, skip_tombstones);

            let mut compacted_entries = compacted_entries.peekable();
            if let Some(_) = compacted_entries.peek() {
                let out_path = compacted_unit.dir.format_prim_file_path();

                let out_sstable = SSTable::new(compacted_entries, out_path)?;

                let out_entryset = CommittedEntrySet::SSTable(out_sstable);

                compacted_unit.prim = Some(out_entryset);
            }
        }

        if compacted_unit.is_empty() {
            compacted_unit.remove_dir()?;
            return Ok(None);
        }

        let commit_info = Self::derive_commit_info(&units);
        let committed_unit = CommittedUnit::from_compacted(compacted_unit, commit_info)?;
        return Ok(Some(committed_unit));
    }

    fn derive_kmerged_iter<'a, K, V>(
        entrysets: impl Iterator<Item = &'a CommittedEntrySet<K, OptDatum<V>>>,
        skip_tombstones: bool,
    ) -> impl Iterator<Item = Entry<'a, K, OptDatum<V>>>
    where
        K: 'a + Deser + Ord,
        OptDatum<V>: 'a + Deser,
    {
        let compacted_entries =
            merging::merge_committed_entrysets(entrysets, None::<&K>, None::<&K>);
        let compacted_entries = compacted_entries.filter(move |entry| {
            if skip_tombstones {
                match entry.try_borrow() {
                    Err(_) => true,
                    Ok((_, optdat_v)) => match optdat_v {
                        OptDatum::Tombstone => false,
                        OptDatum::Some(_) => true,
                    },
                }
            } else {
                true
            }
        });
        compacted_entries
    }

    fn derive_commit_info<'a>(units: &Vec<&'a CommittedUnit>) -> CommitInfo {
        #[rustfmt::skip]
        let commit_ver_hi_incl = units.clone().first().unwrap().commit_info.commit_ver_hi_incl().clone();
        #[rustfmt::skip]
        let commit_ver_lo_incl = units.clone().last().unwrap().commit_info.commit_ver_lo_incl().clone();

        let mut timestamp_num = TimestampNum::from(0);
        for unit in units {
            let ts = unit.commit_info.timestamp_num();
            timestamp_num = cmp::max(timestamp_num, *ts);
        }
        *timestamp_num += 1;

        CommitInfo {
            commit_ver_hi_incl,
            commit_ver_lo_incl,
            timestamp_num,
            data_type: CommitDataType::SSTable,
        }
    }
}
