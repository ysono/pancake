use crate::storage::api::{Key, Value};
use crate::storage::lsm::Memtable;
use crate::storage::serde;
use anyhow::Result;
use core::option::Option;
use core::option::Option::{None, Some};
use core::result::Result::Ok;
use std::collections::BTreeMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::Seek;
use std::io::SeekFrom;
use std::path::PathBuf;

type FileOffset = u64;

static SSTABLE_IDX_SPARSENESS: usize = 3;

/// One SS Table. It consists of a file on disk and an in-memory sparse indexing of the file.
#[derive(Debug)]
pub struct SSTable {
    pub path: PathBuf,
    idx: BTreeMap<Key, FileOffset>,
}

impl SSTable {
    fn is_kv_in_mem(kv_i: usize) -> bool {
        kv_i % SSTABLE_IDX_SPARSENESS == SSTABLE_IDX_SPARSENESS - 1
    }

    pub fn write_from_memtable(memtable: &Memtable, path: PathBuf) -> Result<SSTable> {
        let mut idx = BTreeMap::<Key, FileOffset>::new();
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;
        let mut offset = 0usize;
        for (kv_i, (k, v)) in memtable.iter().enumerate() {
            let delta_offset = serde::serialize_kv(k, v, &mut file)?;

            if SSTable::is_kv_in_mem(kv_i) {
                idx.insert((*k).clone(), offset as FileOffset);
            }

            offset += delta_offset;
        }

        Ok(SSTable { path, idx })
    }

    pub fn read_from_file(path: PathBuf) -> Result<SSTable> {
        let mut idx = BTreeMap::<Key, FileOffset>::new();
        let mut file = File::open(&path)?;
        let mut offset = 0usize;
        for kv_i in 0usize.. {
            let deser_key = SSTable::is_kv_in_mem(kv_i);
            match serde::read_kv(&mut file, deser_key, |_| false)? {
                serde::FileKeyValue::EOF => break,
                serde::FileKeyValue::KV(delta_offset, maybe_key, _) => {
                    if let Some(key) = maybe_key {
                        idx.insert(key, offset as FileOffset);
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
    ///
    /// @return
    ///     If found within this sstable, then return Some. The content of the Some may be a tombstone: i.e. Some(Value(None)).
    ///     If not found within this sstable, then return None.
    pub fn search(&self, k: &Key) -> Result<Option<Value>> {
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
}