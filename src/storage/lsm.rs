use super::api::{Key, Value};
use super::serde;
use anyhow::Result;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

static COMMIT_LOG_PATH: &'static str = "/tmp/pancake/commit_log.data";
static SSTABLE_DIR_PATH: &'static str = "/tmp/pancake/sstables";
static SSTABLE_IDX_SPARSENESS: usize = 4;
static MEMTABLE_FLUSH_SIZE_THRESH: usize = 3;
static SSTABLE_COMPACT_COUNT_THRESH: usize = 4;

/// The memtable: in-memory sorted map of the most recently put items.
/// Its content corresponds to the append-only commit log.
/// The memtable and commit log will be flushed to a (on-disk SSTable, in-memory sparse seeks of this SSTable) pair, at a later time.
#[derive(Default, Debug)]
struct Memtable(BTreeMap<Key, Value>);

impl Memtable {
    fn read_from_commit_log(path: &PathBuf) -> Result<Memtable> {
        let mut memtable = Memtable::default();

        if !path.exists() {
            return Ok(memtable);
        }

        let mut file = File::open(path)?;
        let iter = serde::KeyValueIterator { file: &mut file };
        for file_data in iter {
            let (_, key, maybe_val) = file_data?;
            match maybe_val {
                None => {
                    memtable.0.remove(&key);
                }
                Some(val) => {
                    memtable.0.insert(key, val);
                }
            }
        }

        Ok(memtable)
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

        for kv in memtable.0.iter() {
            serde::write_kv(kv.0, Some(&kv.1), &mut file)?;
        }

        Ok(SSTable::read_from_file(path)?)
    }

    fn read_from_file(path: PathBuf) -> Result<SSTable> {
        let mut idx = BTreeMap::<Key, u64>::new();

        let mut offset = 0usize;

        let mut file = File::open(&path)?;
        let iter = serde::KeyValueIterator { file: &mut file };
        for file_data in iter.step_by(SSTABLE_IDX_SPARSENESS) {
            let (delta_offset, key, _) = file_data?;
            idx.insert(key, offset as u64);
            offset += delta_offset;
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

        // TODO Create a second kind of iterator so that we're not unnecessarily reading value items from file into heap.
        let ss_iter = serde::KeyValueIterator { file: &mut file };
        for file_data in ss_iter {
            let (_, key, maybe_val) = file_data?;
            if &key == k {
                return Ok(maybe_val);
            }
            if &key > k {
                break;
            }
        }
        Ok(None)
    }
}

#[derive(Debug)]
pub struct State {
    memtable: Memtable,
    commit_log: Option<File>,
    sstables: Vec<SSTable>,
}

impl State {
    pub fn init() -> Result<State> {
        std::fs::create_dir_all(SSTABLE_DIR_PATH)?;

        let memtable = Memtable::read_from_commit_log(&PathBuf::from(COMMIT_LOG_PATH))?;

        let commit_log = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(COMMIT_LOG_PATH)?;

        let sstables: Vec<SSTable> = std::fs::read_dir(SSTABLE_DIR_PATH)?
            .map(|res| res.map(|e| e.path()))
            .map(|path| SSTable::read_from_file(path.unwrap()).unwrap())
            .collect();

        let ret = State {
            memtable,
            commit_log: Some(commit_log),
            sstables,
        };
        Ok(ret)
    }

    pub fn flush_memtable(&mut self) -> Result<()> {
        let path = PathBuf::from(format!(
            "/tmp/pancake/sstables/{}.data",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros()
        ));
        let sstable = SSTable::write_from_memtable(&self.memtable, path)?;
        self.sstables.push(sstable);
        self.memtable.0.clear();
        self.commit_log.take(); // Close the file.
        self.commit_log = Some(
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(COMMIT_LOG_PATH)?,
        );

        if self.sstables.len() >= SSTABLE_COMPACT_COUNT_THRESH {
            // TODO compact
        }

        Ok(())
    }
}

pub fn put(s: &mut State, k: Key, v: Option<Value>) -> Result<()> {
    // TODO(btc): maybe change return type to return a Result (perhaps not anyhow though)
    serde::write_kv(&k, v.as_ref(), s.commit_log.as_mut().unwrap())?;

    match v {
        Some(v) => {
            s.memtable.0.insert(k, v);
        }
        None => {
            s.memtable.0.remove(&k);
        }
    }

    if s.memtable.0.len() >= MEMTABLE_FLUSH_SIZE_THRESH {
        s.flush_memtable()?;
    }

    Ok(())
}

pub fn get(s: &State, k: Key) -> Result<Option<Value>> {
    match s.memtable.0.get(&k) {
        Some(v) => Ok(Some(v.clone())),
        None => {
            let mut found = None;
            for ss in s.sstables.iter() {
                let v = ss.search(&k)?;
                if v.is_some() {
                    found = v;
                    break;
                }
                // TODO bloom filter
            }
            Ok(found)
        }
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
