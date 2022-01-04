use super::LSMTree;
use crate::storage::lsm::{merging, Entry, SSTable};
use crate::storage::serde::{OptDatum, Serializable};
use anyhow::Result;
use std::mem;
use std::path::PathBuf;

static MEMTABLE_FLUSH_SIZE_THRESH: usize = 7;
static SSTABLE_COMPACT_COUNT_THRESH: usize = 4;

impl<K, V> LSMTree<K, V>
where
    K: Serializable + Ord + Clone,
    OptDatum<V>: Serializable,
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
        let sst_path = self.format_new_sstable_file_path()?;

        let entries = self.memlog.get_whole_range().map(Entry::Ref);

        let new_sst = SSTable::new(entries, sst_path)?;

        self.sstables.push(new_sst);

        self.memlog.clear()?;

        Ok(())
    }

    /// For now, always compact all SSTables into one SSTable.
    fn compact_sstables(&mut self) -> Result<()> {
        let sst_path = self.format_new_sstable_file_path()?;

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

    fn format_new_sstable_file_path(&mut self) -> Result<PathBuf> {
        let id = self.unique_id.get_and_inc()?;
        let sst_path = self
            .sstables_dir_path
            .join(format!("{}.kv", id.to_alphanum_orderable_string()));
        Ok(sst_path)
    }
}
