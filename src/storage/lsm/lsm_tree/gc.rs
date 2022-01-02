use super::{LSMTree, SSTABLES_DIR_NAME};
use crate::storage::lsm::{merging, Entry, SSTable};
use crate::storage::serde::{OptDatum, Serializable};
use crate::storage::utils;
use anyhow::Result;
use std::mem;
use std::path::PathBuf;

static MEMTABLE_FLUSH_SIZE_THRESH: usize = 7;
static SSTABLE_COMPACT_COUNT_THRESH: usize = 4;

impl<K, V> LSMTree<K, V>
where
    K: Serializable + Ord + Clone,
    V: Serializable + Clone,
{
    pub fn maybe_run_gc(&mut self) -> Result<()> {
        if self.memlog.mem_len() >= MEMTABLE_FLUSH_SIZE_THRESH {
            self.flush_memtable()?;
        }
        if self.sstables.len() >= SSTABLE_COMPACT_COUNT_THRESH {
            self.compact_sstables()?;
        }
        Ok(())
    }

    fn flush_memtable(&mut self) -> Result<()> {
        let entries = self.memlog.get_whole_range().map(Entry::Ref);
        let path = self.format_new_sstable_file_path();
        let new_sst = SSTable::new(entries, path)?;

        self.sstables.push(new_sst);

        self.memlog.clear()?;

        Ok(())
    }

    /// For now, always compact all SSTables into one SSTable.
    fn compact_sstables(&mut self) -> Result<()> {
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
        let path = self.format_new_sstable_file_path();
        let new_sst = SSTable::new(entries, path)?;

        let new_ssts = vec![new_sst];
        let old_ssts = mem::replace(&mut self.sstables, new_ssts);
        for sst in old_ssts {
            sst.remove_file()?;
        }

        Ok(())
    }

    fn format_new_sstable_file_path(&self) -> PathBuf {
        let ssts_dir_path = self.lsm_dir_path.join(SSTABLES_DIR_NAME);
        utils::new_timestamped_path(ssts_dir_path, "kv")
    }
}
