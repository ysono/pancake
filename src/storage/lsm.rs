use std::collections::BTreeMap;
use std::fs::{OpenOptions, File};
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;
use anyhow::Result;
use super::api::{Key, Value};
use super::serde;

static COMMIT_LOG_PATH: &'static str = "/tmp/pancake/commit_log.data";
static SSTABLE_DIR_PATH: &'static str = "/tmp/pancake/sstables";
static SSTABLE_IDX_SPARSENESS: usize = 4;
static MEMTABLE_COMPACTION_SIZE_THRESH: usize = 3;

/// The memtable: in-memory sorted map of the most recently put items.
/// Its content corresponds to the append-only commit log.
/// The memtable and commit log will be flushed to a (on-disk SSTable, in-memory sparse seeks of this SSTable) pair, at a later time.
#[derive(Default)]
struct Memtable (BTreeMap<Key, Value>);

/// One SS Table. It consists of a file on disk and an in-memory sparse indexing of the file.
struct SSTable {
    path: PathBuf,
    idx: BTreeMap<Key, u64>,
}

pub struct State {
    memtable: Memtable,
    commit_log: Option<File>,
    sstables: Vec<SSTable>,
}

impl State {
    pub fn init() -> Result<State> {
        std::fs::create_dir_all(SSTABLE_DIR_PATH)?;
        
        let memtable = read_commit_log(&PathBuf::from(COMMIT_LOG_PATH));

        let commit_log = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(COMMIT_LOG_PATH)?;

        let sstables: Vec<SSTable> =
            std::fs::read_dir(SSTABLE_DIR_PATH)?
            .map(|res| res.map(|e| e.path()))
            .map(|path| read_sstable(path.unwrap()).unwrap())
            .collect();

        let ret = State {
            memtable,
            commit_log: Some(commit_log),
            sstables,
        };
        Ok(ret)
    }

    pub fn flush_memtable(&mut self) -> Result<()> {
        let path = PathBuf::from("/tmp/pancake/sstables/1.data");
        let sstable = write_sstable(&self.memtable, path)?;
        self.sstables.push(sstable);
        self.memtable.0.clear();
        self.commit_log.take(); // Close the file.
        self.commit_log = Some(
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(COMMIT_LOG_PATH)?
        );

        Ok(())
    }
}

/// Read all key-value pairs.
fn read_commit_log(path: &PathBuf) -> Memtable {
    let mut memtable = Memtable::default();

    let file_result = File::open(path);
    if let Ok(mut file) = file_result {
        let iter = serde::KeyValueIterator { file: &mut file };
        for (_, key, maybe_val) in iter {
            match maybe_val {
                None => {
                    memtable.0.remove(&key);
                }
                Some(val) => {
                    memtable.0.insert(key, val);
                }
            }
        }
    }
    // Else, ignore if commit log does not exist.

    memtable
}

fn write_sstable(memtable: &Memtable, path: PathBuf) -> Result<SSTable> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&path)?;

    for (i, kv) in memtable.0.iter().enumerate() {
        if i % SSTABLE_IDX_SPARSENESS == 0 {
            serde::write_kv(kv.0, Some(&kv.1), &mut file)?;
        }
    }

    Ok(read_sstable(path)?)
}

fn read_sstable(path: PathBuf) -> Result<SSTable> {
    let mut idx = BTreeMap::<Key, u64>::new();

    let mut offset = 0usize;
    
    let mut file = File::open(&path)?;
    let iter = serde::KeyValueIterator { file: &mut file };
    for (delta_offset, key, _) in iter {
        idx.insert(key, offset as u64);
        offset += delta_offset;
    }

    Ok(SSTable {
        path,
        idx
    })
}

/// 1. Bisect in the in-memory sparse index.
/// 1. Seek linearlly in file.
fn search_in_sstable(ss: &SSTable, k: &Key) -> Result<Option<Value>> {
    // TODO what's the best way to bisect a BTreeMap?
    let mut iter = ss.idx.iter();
    let pos = iter.rposition(|kv| kv.0 <= k);
    let (lo, hi) = match pos {
        None => {
            (None, ss.idx.iter().next())
        }
        Some(pos) => {
            let mut iter = ss.idx.iter();
            let lo = iter.nth(pos);
            (lo, iter.next())
        }
    };

    if let (None, None) = (lo, hi) {
        // sstable is empty.
        return Ok(None);
    }

    let lo = lo.map(|kv| kv.1).unwrap_or(&0u64);
    let hi = hi.map(|kv| kv.1 as &u64);

    let mut file = File::open(&ss.path)?;
    file.seek(SeekFrom::Start(*lo))?;
    
    let ss_iter = serde::KeyValueIterator { file : &mut file };
    let mut offset = 0u64;
    for (delta_offset, key, maybe_val) in ss_iter {
        offset += delta_offset as u64;
        if hi.is_some() && hi.unwrap() <= &offset {
            break;
        }
        if &key == k {
            return Ok(maybe_val);
        }
    }
    Ok(None)
}

pub fn put(s: &mut State, k: Key, v: Option<Value>) -> Result<()> {
    // TODO(btc): maybe change return type to return a Result (perhaps not anyhow though)
    serde::write_kv(&k, v.as_ref(), s.commit_log.as_mut().unwrap())?;
    
    match v {
        Some(v) => { s.memtable.0.insert(k, v); }
        None => { s.memtable.0.remove(&k); }
    }

    if s.memtable.0.len() >= MEMTABLE_COMPACTION_SIZE_THRESH {
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
                let v = search_in_sstable(ss, &k)?;
                if v.is_some() {
                    found = v;
                    break;
                }
                // TODO bloom filter
            };
            Ok(found)
        },
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