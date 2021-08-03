use crate::storage::api::{Key, Value};
use crate::storage::lsm::Memtable;
use crate::storage::serde::KeyValueIterator;
use crate::storage::{serde, utils};
use anyhow::Result;
use core::option::Option;
use core::option::Option::{None, Some};
use core::result::Result::Ok;
use itertools::Itertools;
use std::collections::BTreeMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::Seek;
use std::io::SeekFrom;
use std::path::PathBuf;

type FileOffset = u64;

static SSTABLES_DIR_PATH: &'static str = "/tmp/pancake/sstables";
static SSTABLE_IDX_SPARSENESS: usize = 3;

/// One SS Table. It consists of a file on disk and an in-memory sparse indexing of the file.
#[derive(Debug)]
pub struct SSTable {
    path: PathBuf,
    sparse_index: SparseIndex,
}

impl SSTable {
    fn is_kv_in_mem(kv_i: usize) -> bool {
        kv_i % SSTABLE_IDX_SPARSENESS == SSTABLE_IDX_SPARSENESS - 1
    }

    pub fn write_from_memtable(memtable: &Memtable, path: PathBuf) -> Result<SSTable> {
        let mut sparse_index = SparseIndex::new();
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;
        let mut offset = 0usize;
        for (kv_i, (k, v)) in memtable.iter().enumerate() {
            let delta_offset = serde::serialize_kv(k, v, &mut file)?;

            if SSTable::is_kv_in_mem(kv_i) {
                sparse_index.insert((*k).clone(), offset as FileOffset);
            }

            offset += delta_offset;
        }

        Ok(SSTable { path, sparse_index })
    }

    pub fn read_from_file(path: PathBuf) -> Result<SSTable> {
        let mut sparse_index = SparseIndex::new();
        let mut file = File::open(&path)?;
        let mut offset = 0usize;
        for kv_i in 0usize.. {
            let deser_key = SSTable::is_kv_in_mem(kv_i);
            match serde::read_kv(&mut file, deser_key, |_| false)? {
                serde::FileKeyValue::EOF => break,
                serde::FileKeyValue::KV(delta_offset, maybe_key, _) => {
                    if let Some(key) = maybe_key {
                        sparse_index.insert(key, offset as FileOffset);
                    }

                    offset += delta_offset;
                }
            }
        }

        Ok(SSTable { path, sparse_index })
    }

    /// Both the in-memory index and the file are sorted by key.
    /// The index maps { key (sparse) => file offset }.
    /// 1. Bisect in the in-memory sparse index, to find the lower-bound file offset.
    /// 1. Seek the offset in the file. Then read linearlly in file until either EOF or the last-read key is greater than the sought key.
    ///
    /// @return
    ///     If found within this sstable, then return Some. The content of the Some may be a tombstone: i.e. Some(Value(None)).
    ///     If not found within this sstable, then return None.
    pub fn get(&self, k: &Key) -> Result<Option<Value>> {
        let file_offset = self.sparse_index.nearest_preceding_file_offset(k);

        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(file_offset))?;

        loop {
            match serde::read_kv(&mut file, true, |read_key| read_key == k)? {
                serde::FileKeyValue::EOF => break,
                serde::FileKeyValue::KV(_, _, found @ Some(_)) => return Ok(found),
                _ => continue,
            }
        }
        Ok(None)
    }

    pub fn remove_file(&self) -> Result<()> {
        fs::remove_file(&self.path)?;
        Ok(())
    }

    pub fn compact<'a, I: Iterator<Item = &'a Self>>(tables: I) -> Result<Vec<Self>> {
        let path = utils::new_timestamped_path(SSTABLES_DIR_PATH);
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;

        let mut key_value_iterators = Vec::new();
        for (index, table) in tables.into_iter().enumerate() {
            let p = &table.path;
            let file = File::open(p)?;

            // NB: the index/position of the sstable is included for the purpose of breaking ties
            // on duplicate keys.
            let iter = KeyValueIterator::from(file).zip(std::iter::repeat(index));
            key_value_iterators.push(iter);
        }

        let compacted = key_value_iterators
            .into_iter()
            .kmerge_by(|(a, index_a), (b, index_b)| {
                /*
                the comparator contract dictates we return true iff |a| is ordered before |b|
                    or said differently: |a| < |b|.

                for equal keys, we define |a| < |b| iff |a| is more recent.
                    note: |a| is more recent when index_a > index_b.

                by defining the ordering in this way,
                    we only keep the first instance of key |k| in the compacted iterator.
                    duplicate items with key |k| must be discarded.
                 */

                // guide results to the front of the iterator for early termination
                if a.is_err() {
                    return true;
                }
                if b.is_err() {
                    return false;
                }

                let key_a = &a.as_ref().unwrap().0;
                let key_b = &b.as_ref().unwrap().0;

                let a_is_equal_but_more_recent = key_a == key_b && index_a > index_b;
                return key_a < key_b || a_is_equal_but_more_recent;
            })
            .map(|a| a.0); // tables[i] is no longer needed

        // Manual impl of:
        // .unique_by(|(k, _)| k.0.clone()); // keep first instance of |k|
        let mut prev = None;
        for result in compacted {
            let (k, v) = result?;
            if prev.is_some() && &k == prev.as_ref().unwrap() {
                continue;
            }
            serde::serialize_kv(&k, &v, &mut file)?;
            prev = Some(k);
        }

        file.sync_all()?;

        // TODO(btc): instead of |read_from_file|, create SSTable index in streaming fashion
        let t = Self::read_from_file(path)?;

        Ok(vec![t])
    }
}

#[derive(Debug)]
struct SparseIndex {
    // this version of the index is backed by an ordered map.
    map: BTreeMap<Key, FileOffset>,
}

impl SparseIndex {
    fn new() -> Self {
        Self {
            map: Default::default(),
        }
    }

    fn insert(&mut self, key: Key, offset: FileOffset) {
        self.map.insert(key, offset);
    }

    fn nearest_preceding_file_offset(&self, key: &Key) -> FileOffset {
        // TODO what's the best way to bisect a BTreeMap? this appears to have O(n) cost
        let idx_pos = self.map.iter().rposition(|kv| kv.0 <= key);
        match idx_pos {
            None => 0u64,
            Some(idx_pos) => {
                let (_, file_offset) = self.map.iter().nth(idx_pos).unwrap();
                *file_offset
            }
        }
        // TODO/FIXME: iter().nth appears to incur a O(n) cost
    }
}
