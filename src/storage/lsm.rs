use super::api::{Key, Value};
use super::serde;
use anyhow::Result;
use derive_more::{Deref, DerefMut};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom};
use std::mem;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

static COMMIT_LOGS_DIR_PATH: &'static str = "/tmp/pancake/commit_logs";
static SSTABLES_DIR_PATH: &'static str = "/tmp/pancake/sstables";
static SSTABLE_IDX_SPARSENESS: usize = 3;
static MEMTABLE_FLUSH_SIZE_THRESH: usize = 7;
static SSTABLE_COMPACT_COUNT_THRESH: usize = 4;

fn new_path(parent_path: &str) -> PathBuf {
    let mut path = PathBuf::from(parent_path);
    path.push(format!(
        "{}.data",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros()
    ));
    path
}

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
            let (key, maybe_val) = file_data;
            match maybe_val {
                None => {
                    self.remove(&key);
                }
                Some(val) => {
                    self.insert(key, val);
                }
            }
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
    fn write_from_memtable(memtable: &Memtable, path: PathBuf) -> Result<SSTable> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;

        for kv in memtable.iter() {
            serde::write_kv(kv.0, Some(&kv.1), &mut file)?;
        }

        // (It would be more efficient to create idx as we write, rather than reread.)
        SSTable::read_from_file(path)
    }

    fn read_from_file(path: PathBuf) -> Result<SSTable> {
        let mut idx = BTreeMap::<Key, u64>::new();
        let mut file = File::open(&path)?;
        let mut offset = 0usize;
        for item_ct in 0usize.. {
            let deser_key = item_ct % SSTABLE_IDX_SPARSENESS == SSTABLE_IDX_SPARSENESS - 1;
            match serde::read_kv(&mut file, deser_key, |_| false)? {
                serde::FileKeyValue::EOF => break,
                serde::FileKeyValue::KeyValue(delta_offset, maybe_key, _) => {
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
                serde::FileKeyValue::KeyValue(_, _, Some(val)) => return Ok(Some(val)),
                _ => continue,
            }
        }
        Ok(None)
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
        let dir_iter = std::fs::read_dir(COMMIT_LOGS_DIR_PATH)?;
        // Assume alphabetical order.
        for dir_entry in dir_iter {
            let path = dir_entry?.path();
            memtable.update_from_commit_log(&path)?;
            commit_log_path = Some(path);
        }

        let commit_log_path = commit_log_path.unwrap_or(new_path(COMMIT_LOGS_DIR_PATH));
        let commit_log = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&commit_log_path)?;

        // Assume alphabetical order.
        let sstables: Vec<SSTable> = fs::read_dir(SSTABLES_DIR_PATH)?
            .map(|entry_result| {
                let path = entry_result.unwrap().path();
                SSTable::read_from_file(path).unwrap()
            })
            .collect();

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
        let new_cl_path = new_path(COMMIT_LOGS_DIR_PATH);
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

        let new_sst = SSTable::write_from_memtable(
            self.memtable_in_flush.as_ref().unwrap(),
            new_path(SSTABLES_DIR_PATH),
        )?;
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
        let mut dense_idx = Memtable::default();
        for old_sst in self.sstables.iter() {
            dense_idx.update_from_commit_log(&old_sst.path)?;
        }
        let new_sst = SSTable::write_from_memtable(&dense_idx, new_path(SSTABLES_DIR_PATH))?;

        let old_sst_list: Vec<SSTable>;
        {
            // TODO MutexGuard here
            // In async version, we will have to assume that new sstables may have been created while we were compacting, so we won't be able to just swap.
            old_sst_list = mem::replace(&mut self.sstables, vec![new_sst]);
        }
        for old_sst in old_sst_list {
            fs::remove_file(&old_sst.path)?;
        }

        Ok(())
    }

    pub fn put(&mut self, k: Key, v: Option<Value>) -> Result<()> {
        serde::write_kv(&k, v.as_ref(), &mut self.commit_log)?;

        match v {
            Some(v) => {
                self.memtable.insert(k, v);
            }
            None => {
                self.memtable.remove(&k);
            }
        }

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
        for ss in self.sstables.iter().rev() {
            let v = ss.search(&k)?;
            if v.is_some() {
                return Ok(v);
            }
            // TODO bloom filter
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
