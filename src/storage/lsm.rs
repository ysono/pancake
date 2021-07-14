use std::collections::BTreeMap;
use std::prelude::*;
use super::api::*;
use std::fs::{OpenOptions, File};
use std::io::{BufRead, Read, SeekFrom, Write};
use std::error::Error;
use std::path::PathBuf;
use anyhow::Result;
use std::mem::size_of;


static DATA_DIR: &'static str = "/tmp/pancake/";


/// The memtable: in-memory sorted map of the most recently put items.
/// Its content corresponds to the append-only commit log.
/// The memtable and commit log will be flushed to a (on-disk SSTable, in-memory sparse seeks of this SSTable) pair, at a later time.
#[derive(Default)]
struct Memtable (BTreeMap<Key, Value>);

/// One SS Table. It consists of a file on disk and an in-memory sparse indexing of the file.
struct SSTable {
    file: File,
    idx: BTreeMap<Key, SeekFrom>,
}

// #[derive(Default)]
pub struct State {
    memtable: Memtable,
    commit_log: Option<File>,
    sstables: Vec<SSTable>,
}

impl State {
    pub fn init() -> Result<State, Box<dyn Error>> {
        let mut data_path = PathBuf::new();
        data_path.push(DATA_DIR);

        std::fs::create_dir_all(&data_path)?;
        
        data_path.push("commit_log");

        let memtable = match File::open(&data_path) {
            Ok(commit_log) => read_commit_log(commit_log)?,
            Err(_) => Memtable::default(),
        };

        let commit_log = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&data_path)?;

        data_path.pop();
        data_path.push("sstables");
        std::fs::create_dir_all(&data_path)?;

        let sstables: Vec<SSTable> =
            std::fs::read_dir(&data_path)?
            .map(|res| res.map(|e| e.path()))
            .map(|path| File::open(path.unwrap()).unwrap())
            .map(|file| read_sstable(file))
            .collect();

        let ret = State {
            memtable,
            commit_log: Some(commit_log),
            sstables,
        };
        Ok(ret)
    }
}

fn read_commit_log(mut file: File) -> Result<Memtable> {
    let mut get_data = || -> Result<Vec<u8>> {
        let mut buf=[0u8; size_of::<usize>()];
        &file.read_exact(&mut buf)?;
        let sz = usize::from_le_bytes(buf);
    
        let mut buf = vec![0u8; sz];
        &file.read_exact(&mut buf)?;
        Ok(buf)
    };
    let mut get_key_value = || -> Result<(Key, Value)> {
        let key_bytes = get_data()?;
        let value_bytes = get_data()?;
        let key = Key(String::from_utf8(key_bytes)?);
        let val = Value::Bytes(value_bytes);
        Ok((key, val))
    };

    let mut memtable = Memtable::default();
    loop {
        if let Ok((key, val)) = get_key_value() {
            memtable.0.insert(key, val);
        } else {
            break;
        }
    }
    Ok(memtable)
}

fn append_to_commit_log(file: &mut File, k: &Key, v: &Option<Value>) -> Result<()> {
    file.write(&k.0.len().to_le_bytes())?;
    file.write(k.0.as_bytes())?;
    match v {
        Some(Value::Bytes(v)) => {
            file.write(&v.len().to_le_bytes())?;
            file.write(v)?;
        }
        _ => {
            let zero: usize = 0;
            file.write(&zero.to_le_bytes())?;
        }
    }
    Ok(())
}

fn read_sstable(file: File) -> SSTable {
    // TODO
    let idx = BTreeMap::<Key, SeekFrom>::new();
    SSTable {
        file,
        idx
    }
}

fn search_in_sstable(ss: &SSTable, k: &Key) -> Option<Value> {
    // TODO
    // 1. bisect in ss.idx
    // 1. Seek linearlly in file
    None
}

pub fn put(s: &mut State, k: Key, v: Option<Value>) {
    // TODO(btc): maybe change return type to return a Result (perhaps not anyhow though)
    append_to_commit_log(s.commit_log.as_mut().unwrap(), &k, &v).unwrap();
    match v {
        Some(v) => { s.memtable.0.insert(k, v); },
        None => { s.memtable.0.remove(&k); },
    }
}

pub fn get(s: &State, k: Key) -> Option<Value> {
    match s.memtable.0.get(&k) {
        Some(v) => Some(v.clone()),
        None => {
            let mut found = None;
            for ss in s.sstables.iter() {
                let v = search_in_sstable(ss, &k);
                if v.is_some() {
                    found = v;
                    break;
                }
                // TODO bloom filter
            };
            found
        },
    }
}

// TODO
// file format
// background job: flush
//   1. flush memtable to sstable
//   1. swap new memtable and commit log
//   This is to run also when quitting.
// background job: compact
//   1. read multiple ss tables
//   1. compact
//   1. flush new ss table(s)
//   1. swap new ss table(s)' in-mem idx and files
// multithread
// tests