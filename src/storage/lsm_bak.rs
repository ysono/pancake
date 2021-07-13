use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use super::state::State;

type Key = u32;
type Val = String;
type Memtable = BTreeMap<Key, Val>;
type SSTableSeek = BTreeMap<Key, usize>;

/// Read an SSTable file and return a sparse mem pointer.
fn read_ss_table(path: PathBuf) -> SSTableSeek {
    SSTableSeek::new()
}

fn flush_ss_table(idx: &Memtable, path: PathBuf) -> Result<(), Box<dyn Error>> {
    Ok(())
}

fn append_to_commit_log(s: &mut State) -> Result<(), Box<dyn Error>> {
    Ok(())
}

fn clear_commit_log(s: &mut State) -> Result<(), Box<dyn Error>> {
    Ok(())
}

/// Insert or delete.
pub fn put(s: &mut State, k: Key, v: Option<Val>) -> Result<(), Box<dyn Error>> {
    append_to_commit_log(s)?;
    match v {
        Some(v) => {
            s.memtable.insert(k, v);
        }
        None => {
            s.memtable.remove(&k);
        }
    }
    Ok(())
}

pub fn get(s: &State, k: Key) -> Option<&Val> {
    s.memtable.get(&k)
    // match s.memtable.get(&k) {
    //     Some(v) => Some(v)
    //     None => {
    //         if bloom_filter::BloomFilter::isMissing() {
    //             None
    //         } else {
    //             for p in s.ss_table_idxs {
    //                 // 1. bisect
    //                 // 1. crawl file linearlly
    //                 // 1. if found, return Some(v)
    //             }
    //             None
    //         }
    //     }
    // )
    None
}

pub fn flush(s: &mut State) -> Result<(), Box<dyn Error>> {
    // let ssPath = Path::new(format!("ss-{:?}", SystemTime::now()));
    // flush_ss_table(&s.memtable, ssPath);
    // clear_commit_log(s)?;
    // s.memtable.clear();
    Ok(())
}

pub fn compact(s: &mut State, ) -> Result<(), Box<dyn Error>> {
    Ok(())
}

pub fn init() -> Result<State, Box<dyn Error>> {
    let mut state = State::default();

    let ss_paths = fs::read_dir("./data/sstables")?;
    // for ss_path in ss_paths {
    //     state.ss_table_paths.push(ss_path);
    //     state.ss_table_idxs.push(read_ss_table(ss_path));
    // }

    Ok(state)
}






pub fn hello() {
    println!("{}", "aloha");
}