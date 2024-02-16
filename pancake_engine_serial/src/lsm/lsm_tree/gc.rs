use super::LSMTree;
use crate::lsm::merging;
use anyhow::Result;
use pancake_engine_common::{Entry, SSTable};
use pancake_types::{serde::OptDatum, types::Serializable};
use std::mem;

/// These thresholds are exaggeratedly small, so as to be helpful with debugging.
/// In the future, we'll allow setting them from env vars.
static MEMTABLE_FLUSH_SIZE_THRESH: usize = 7;
static SSTABLE_COMPACT_COUNT_THRESH: usize = 4;

impl<K, V> LSMTree<K, V>
where
    K: Serializable + Ord + Clone,
    OptDatum<V>: Serializable,
{
    pub fn maybe_run_gc(&mut self) -> Result<()> {
        if self.memlog.r_memlog().mem_len() >= MEMTABLE_FLUSH_SIZE_THRESH {
            self.flush_memtable()?;
        }
        if self.sstables.len() >= SSTABLE_COMPACT_COUNT_THRESH {
            self.compact_sstables()?;
        }
        Ok(())
    }

    fn flush_memtable(&mut self) -> Result<()> {
        let sst_path = self.sstables_dir.format_new_child_path();

        let entries = self.memlog.r_memlog().get_whole_range().map(Entry::Ref);

        let new_sst = SSTable::new(entries, sst_path)?;

        self.sstables.push(new_sst);

        self.memlog.clear()?;

        Ok(())
    }

    /// For now, always compact all SSTables into one SSTable.
    fn compact_sstables(&mut self) -> Result<()> {
        let sst_path = self.sstables_dir.format_new_child_path();

        let entries = merging::merge_sstables(&self.sstables[..], None, None)
            // skip tombstones
            .filter(|res| match res {
                Err(_) => true,
                Ok((_k, optdat_v)) => match optdat_v {
                    OptDatum::Tombstone => false,
                    OptDatum::Some(_) => true,
                },
            })
            .map(Entry::Own);

        let new_sst = SSTable::new(entries, sst_path)?;

        let new_ssts = vec![new_sst];
        let old_ssts = mem::replace(&mut self.sstables, new_ssts);
        for sst in old_ssts {
            sst.remove_file()?;
        }

        Ok(())
    }
}
