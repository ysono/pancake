use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::mem;
use std::path::PathBuf;

use anyhow::{anyhow, Result};
use derive_more::{Deref, DerefMut};

use super::api::{Key, Value};
use super::serde;
use super::utils;
use crate::storage::sstable::SSTable;

static COMMIT_LOGS_DIR_PATH: &'static str = "/tmp/pancake/commit_logs";
static SSTABLES_DIR_PATH: &'static str = "/tmp/pancake/sstables";
static MEMTABLE_FLUSH_SIZE_THRESH: usize = 7;
static SSTABLE_COMPACT_COUNT_THRESH: usize = 4;

/// The memtable: in-memory sorted map of the most recently put items.
/// Its content corresponds to the append-only commit log.
/// The memtable and commit log will be flushed to a (on-disk SSTable, in-memory sparse seeks of this SSTable) pair, at a later time.
#[derive(Default, Deref, DerefMut)]
pub struct Memtable(BTreeMap<Key, Value>);

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
        let new_tables = SSTable::compact(self.sstables.iter())?;

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

    pub fn get(&self, k: Key) -> Result<Value> {
        if let Some(v) = self.memtable.get(&k) {
            return Ok(v.clone());
        }
        if let Some(mtf) = &self.memtable_in_flush {
            if let Some(v) = mtf.get(&k) {
                return Ok(v.clone());
            }
        }
        // TODO bloom filter here
        for ss in self.sstables.iter().rev() {
            let v = ss.get(&k)?;
            if let Some(v) = v {
                return Ok(v);
            }
        }
        Ok(Value(None))
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