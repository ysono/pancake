use crate::{
    db_state::ScndIdxState,
    lsm::{
        entryset::{merging, CommittedEntrySet},
        unit::{CommitDataType, CommitInfo, CommittedUnit, CompactedUnit, ReplacementNum},
    },
    opers::fc::fc_traversal::FCJob,
};
use anyhow::Result;
use pancake_engine_common::{Entry, SSTable};
use pancake_types::{serde::OptDatum, types::Deser};

impl<'job> FCJob<'job> {
    pub(super) fn do_flush_and_compact<'data>(
        &self,
        units: Vec<&'data CommittedUnit>,
        skip_tombstones: bool,
    ) -> Result<CompactionResult> {
        if Self::should_slice_be_compacted(&units) == false {
            return Ok(CompactionResult::NoChange);
        }

        let maybe_compacted_unit =
            self.potentially_create_compacted_unit(&units, skip_tombstones)?;

        if let Some(compacted_unit) = maybe_compacted_unit {
            let commit_info = Self::derive_commit_info(&units);
            let committed_unit = CommittedUnit::from_compacted(compacted_unit, commit_info)?;
            return Ok(CompactionResult::Some(committed_unit));
        } else {
            return Ok(CompactionResult::Empty);
        }
    }

    /// A given slice of Units should be compacted iff any of:
    /// - The slice contains 1+ MemLogs.
    /// - The slice contains 2+ Units (regardless of MemLogs or SSTables).
    ///
    /// This is an arbitrary policy, and can be tuned in the future.
    fn should_slice_be_compacted<'data>(units: &Vec<&'data CommittedUnit>) -> bool {
        (units.len() >= 2)
            || (units
                .iter()
                .any(|unit| unit.commit_info.data_type() == &CommitDataType::MemLog))
    }

    fn potentially_create_compacted_unit<'data>(
        &self,
        existing_units: &Vec<&'data CommittedUnit>,
        skip_tombstones: bool,
    ) -> Result<Option<CompactedUnit>> {
        let mut maybe_output_unit = None;
        let ensure_create_output_unit = |arg: &mut Option<CompactedUnit>| -> Result<()> {
            if arg.is_none() {
                let new_unit_dir = self.db.lsm_dir().format_new_unit_dir_path();
                let compacted_unit = CompactedUnit::new_empty(new_unit_dir)?;
                *arg = Some(compacted_unit);
            }
            Ok(())
        };

        for (_, ScndIdxState { scnd_idx_num, .. }) in self.db_state_guard.scnd_idxs().iter() {
            let existing_entrysets = existing_units
                .iter()
                .filter_map(|unit| unit.scnds.get(scnd_idx_num));
            let compacted_entries = Self::derive_kmerged_iter(existing_entrysets, skip_tombstones);
            let mut compacted_entries = compacted_entries.peekable();
            if let Some(_) = compacted_entries.peek() {
                ensure_create_output_unit(&mut maybe_output_unit)?;
                let out_unit = maybe_output_unit.as_mut().unwrap();

                let out_path = out_unit.dir.format_scnd_file_path(*scnd_idx_num);
                let out_sstable = SSTable::new(compacted_entries, out_path)?;

                out_unit.scnds.insert(*scnd_idx_num, out_sstable);
            }
        }

        {
            let existing_entrysets = existing_units.iter().filter_map(|unit| unit.prim.as_ref());
            let compacted_entries = Self::derive_kmerged_iter(existing_entrysets, skip_tombstones);
            let mut compacted_entries = compacted_entries.peekable();
            if let Some(_) = compacted_entries.peek() {
                ensure_create_output_unit(&mut maybe_output_unit)?;
                let out_unit = maybe_output_unit.as_mut().unwrap();

                let out_path = out_unit.dir.format_prim_file_path();
                let out_sstable = SSTable::new(compacted_entries, out_path)?;

                out_unit.prim = Some(out_sstable);
            }
        }

        Ok(maybe_output_unit)
    }

    fn derive_kmerged_iter<'data, K, V>(
        entrysets: impl Iterator<Item = &'data CommittedEntrySet<K, OptDatum<V>>>,
        skip_tombstones: bool,
    ) -> impl Iterator<Item = Entry<'data, K, OptDatum<V>>>
    where
        K: 'data + Deser + Ord,
        OptDatum<V>: 'data + Deser,
    {
        let compacted_entries =
            merging::merge_committed_entrysets(entrysets, None::<&K>, None::<&K>);
        let compacted_entries = compacted_entries.filter(move |entry| {
            if skip_tombstones == true {
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

    fn derive_commit_info<'data>(units: &Vec<&'data CommittedUnit>) -> CommitInfo {
        #[rustfmt::skip]
        let commit_ver_hi_incl = units.first().unwrap().commit_info.commit_ver_hi_incl().clone();
        #[rustfmt::skip]
        let commit_ver_lo_incl = units.last().unwrap().commit_info.commit_ver_lo_incl().clone();

        let replc_nums = units
            .iter()
            .map(|unit| unit.commit_info.replacement_num().clone());
        let replacement_num = ReplacementNum::new_larger_than_all_of(replc_nums);

        CommitInfo {
            commit_ver_hi_incl,
            commit_ver_lo_incl,
            replacement_num,
            data_type: CommitDataType::SSTable,
        }
    }
}

pub(super) enum CompactionResult {
    /// Flushing+compaction would not have changed the given slice of Units; hence was not executed.
    NoChange,

    /// Flushing+compaction was executed, and resulted in an empty Unit.
    Empty,

    /// Flushing+compaction was executed, and resulted in one Unit containing 1+ non-empty SSTables.
    Some(CommittedUnit),
}
