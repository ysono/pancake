use super::api::{Key, Value};
use super::serde;
use super::utils;
use crate::storage::serde::KeyValueIterator;
use anyhow::{anyhow, Result};
use derive_more::{Deref, DerefMut};
use itertools::Itertools;
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom};
use std::mem;
use std::path::PathBuf;

static COMMIT_LOGS_DIR_PATH: &'static str = "/tmp/pancake/commit_logs";
static SSTABLES_DIR_PATH: &'static str = "/tmp/pancake/sstables";
static SSTABLE_IDX_SPARSENESS: usize = 3;
static MEMTABLE_FLUSH_SIZE_THRESH: usize = 7;
static SSTABLE_COMPACT_COUNT_THRESH: usize = 4;

/// The memtable: in-memory sorted map of the most recently put items.
/// Its content corresponds to the append-only commit log.
/// The memtable and commit log will be flushed to a (on-disk SSTable, in-memory sparse seeks of this SSTable) pair, at a later time.
#[derive(Default, Debug, Deref, DerefMut)]
struct Memtable(BTreeMap<Key, Value>);

impl Memtable {
    fn update_from_commit_log(&mut self, path: &PathBuf) -> Result<()> {
        let file = File::open(path)?;
        let iter = serde::KeyValueIterator::from(file);
        for file_data in iter {
            let (key, val) = file_data?;
            self.insert(key, val);
        }
        Ok(())
    }
}

/// One SS Table. It consists of a file on disk and an in-memory sparse indexing of the file.
#[derive(Debug)]
struct SSTable {
    path: PathBuf,
    idx: BTreeMap<Key, u64>,
}

impl SSTable {
    fn is_kv_in_mem(kv_i: usize) -> bool {
        kv_i % SSTABLE_IDX_SPARSENESS == SSTABLE_IDX_SPARSENESS - 1
    }

    fn write_from_memtable(memtable: &Memtable, path: PathBuf) -> Result<SSTable> {
        let mut idx = BTreeMap::<Key, u64>::new();
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;
        let mut offset = 0usize;
        for (kv_i, (k, v)) in memtable.iter().enumerate() {
            let delta_offset = serde::serialize_kv(k, v, &mut file)?;

            if SSTable::is_kv_in_mem(kv_i) {
                idx.insert((*k).clone(), offset as u64);
            }

            offset += delta_offset;
        }

        Ok(SSTable { path, idx })
    }

    fn read_from_file(path: PathBuf) -> Result<SSTable> {
        let mut idx = BTreeMap::<Key, u64>::new();
        let mut file = File::open(&path)?;
        let mut offset = 0usize;
        for kv_i in 0usize.. {
            let deser_key = SSTable::is_kv_in_mem(kv_i);
            match serde::read_kv(&mut file, deser_key, |_| false)? {
                serde::FileKeyValue::EOF => break,
                serde::FileKeyValue::KV(delta_offset, maybe_key, _) => {
                    if let Some(key) = maybe_key {
                        idx.insert(key, offset as u64);
                    }

                    offset += delta_offset;
                }
            }
        }

        Ok(SSTable { path, idx })
    }

    /// Both the in-memory index and the file are sorted by key.
    /// The index maps { key (sparse) => file offset }.
    /// 1. Bisect in the in-memory sparse index, to find the lower-bound file offset.
    /// 1. Seek the offset in the file. Then read linearlly in file until either EOF or the last-read key is greater than the sought key.
    fn search(&self, k: &Key) -> Result<Option<Value>> {
        // TODO what's the best way to bisect a BTreeMap?
        let idx_pos = self.idx.iter().rposition(|kv| kv.0 <= k);
        let file_offset = match idx_pos {
            None => 0u64,
            Some(idx_pos) => {
                let (_, file_offset) = self.idx.iter().nth(idx_pos).unwrap();
                *file_offset
            }
        };

        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(file_offset))?;

        loop {
            match serde::read_kv(&mut file, true, |read_key| read_key == k)? {
                serde::FileKeyValue::EOF => break,
                serde::FileKeyValue::KV(_, _, Some(val)) => return Ok(Some(val)),
                _ => continue,
            }
        }
        Ok(None)
    }

    fn remove_file(&self) -> Result<()> {
        fs::remove_file(&self.path)?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct LSM {
    memtable: Memtable,
    commit_log_path: PathBuf,
    commit_log: File,
    memtable_in_flush: Option<Memtable>,
    sstables: Vec<SSTable>,
}

impl LSM {
    pub fn init() -> Result<LSM> {
        std::fs::create_dir_all(COMMIT_LOGS_DIR_PATH)?;
        std::fs::create_dir_all(SSTABLES_DIR_PATH)?;

        let mut memtable = Memtable::default();
        let mut commit_log_path = None;
        for path in utils::read_dir_sorted(COMMIT_LOGS_DIR_PATH)? {
            memtable.update_from_commit_log(&path)?;
            commit_log_path = Some(path);
        }

        let commit_log_path =
            commit_log_path.unwrap_or(utils::timestamped_path(COMMIT_LOGS_DIR_PATH));
        let commit_log = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&commit_log_path)?;

        let sstables: Result<Vec<SSTable>> = utils::read_dir_sorted(SSTABLES_DIR_PATH)?
            .into_iter()
            .map(SSTable::read_from_file)
            .collect();
        let sstables = sstables?;

        let ret = LSM {
            memtable,
            commit_log_path,
            commit_log,
            memtable_in_flush: None,
            sstables,
        };
        Ok(ret)
    }

    fn flush_memtable(&mut self) -> Result<()> {
        let new_cl_path = utils::timestamped_path(COMMIT_LOGS_DIR_PATH);
        let new_cl = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&new_cl_path)?;
        let old_cl_path: PathBuf;
        {
            // TODO MutexGuard here
            let old_mt = mem::replace(&mut self.memtable, Memtable::default());
            self.memtable_in_flush = Some(old_mt);

            self.commit_log = new_cl;
            old_cl_path = mem::replace(&mut self.commit_log_path, new_cl_path);
        }

        let mtf = self
            .memtable_in_flush
            .as_ref()
            .ok_or(anyhow!("Unexpected error: no memtable being flushed"))?;
        let new_sst =
            SSTable::write_from_memtable(mtf, utils::timestamped_path(SSTABLES_DIR_PATH))?;

        {
            // TODO MutexGuard here
            self.sstables.push(new_sst);
            self.memtable_in_flush.take();
        }
        fs::remove_file(old_cl_path)?;

        if self.sstables.len() >= SSTABLE_COMPACT_COUNT_THRESH {
            self.compact_sstables()?;
        }

        Ok(())
    }

    fn compact_sstables(&mut self) -> Result<()> {
        let path = utils::timestamped_path(SSTABLES_DIR_PATH);
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;

        let mut key_value_iterators = Vec::new();
        for (index, table) in self.sstables.iter().enumerate() {
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
        let new_tables = vec![SSTable::read_from_file(path)?];

        // TODO MutexGuard here
        // In async version, we will have to assume that new sstables may have been created while we were compacting, so we won't be able to just swap.
        let old_tables = mem::replace(&mut self.sstables, new_tables);
        for table in old_tables {
            table.remove_file()?;
        }

        Ok(())
    }

    pub fn put(&mut self, k: Key, v: Value) -> Result<()> {
        serde::serialize_kv(&k, &v, &mut self.commit_log)?;

        self.memtable.insert(k, v);

        if self.memtable.len() >= MEMTABLE_FLUSH_SIZE_THRESH {
            self.flush_memtable()?;
        }

        Ok(())
    }

    pub fn get(&self, k: Key) -> Result<Option<Value>> {
        if let Some(v) = self.memtable.get(&k) {
            return Ok(Some(v.clone()));
        }
        if let Some(mtf) = &self.memtable_in_flush {
            if let Some(v) = mtf.get(&k) {
                return Ok(Some(v.clone()));
            }
        }
        // TODO bloom filter here
        for ss in self.sstables.iter().rev() {
            let v = ss.search(&k)?;
            if v.is_some() {
                return Ok(v);
            }
        }
        Ok(None)
    }
}

// TODO
// background job: flush
//   1. flush memtable to sstable
//   1. swap new memtable and commit log
//   This is to run also when quitting.
// background job: compact
//   1. read multiple ss tables
//   1. compact
//   1. flush new ss table(s)
//   1. swap new ss table(s)' in-mem idx and files
// handle requests in multi threads
// tests
