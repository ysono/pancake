use std::collections::BTreeMap;
use std::prelude::*;
use super::api::*;
use std::fs::{OpenOptions, File};
use std::io::{SeekFrom, Write};
use std::error::Error;
use std::path::PathBuf;


static DATA_DIR: &'static str = "/tmp/pancakes/";


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
            Ok(commit_log) => read_commit_log(commit_log),
            Err(_) => Memtable::default(),
        };

        let commit_log = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&data_path)
            .unwrap();

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

fn read_commit_log(file: File) -> Memtable {
    // TODO
    Memtable::default()
}

fn append_to_commit_log(file: &mut File, k: &Key, v: &Option<Value>) {
    let fmt = format!(
        "{}\0",
        k as &String
    );
    file.write(fmt.as_bytes()).unwrap();
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
    append_to_commit_log(s.commit_log.as_mut().unwrap(), &k, &v);
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