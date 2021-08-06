//! An LSM-Tree is an abstraction of a sorted key-value dictionary.
//!
//! ### API:
//!
//! The exposed operations are: `put one`, `get one`, `get range`.
//!
//! Values are immutable. They cannot be modified in-place, and must be replaced.
//!
//! ### Internals:
//!
//! An in-memory sorted structure holds the most recently inserted `{key: value}` mapping.
//!
//! The in-memory structure is occasionally flushed into an SSTable.
//!
//! Multiple SSTables are occasionally compacted into one SSTable.
//!
//! ![](https://user-images.githubusercontent.com/5148696/128642691-55eea319-05a4-43bf-a2f9-13e9f5132a74.png)
//!
//! ### Querying:
//!
//! A `put` operation accesses the in-memory head structure only.
//!
//! A `get` operation generally accesses the in-memory head and all SSTables.
//!
//! When the same key exists in multiple sources, only the result from the newest source is retrieved.
//!
//! ![](https://user-images.githubusercontent.com/5148696/128660102-e6da6e45-b6a1-4a2b-b038-66af51f212c7.png)

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::hash::Hash;
use std::mem;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};
use itertools::Itertools;

use crate::storage::serde::{self, KeyValueIterator, Serializable};
use crate::storage::sstable::SSTable;
use crate::storage::utils;

static COMMIT_LOGS_DIR_PATH: &'static str = "commit_logs";
static SSTABLES_DIR_PATH: &'static str = "sstables";
static MEMTABLE_FLUSH_SIZE_THRESH: usize = 7;
static SSTABLE_COMPACT_COUNT_THRESH: usize = 4;

pub struct LSMTree<K, V> {
    path: PathBuf,
    memtable: BTreeMap<K, V>,
    commit_log_path: PathBuf,
    commit_log: File,
    memtable_in_flush: Option<BTreeMap<K, V>>,
    sstables: Vec<SSTable<K, V>>,
}

impl<K, V> LSMTree<K, V>
where
    K: Serializable + Ord + Hash + Clone,
    V: Serializable + Clone,
{
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        std::fs::create_dir_all(path.as_ref().join(COMMIT_LOGS_DIR_PATH))?;
        std::fs::create_dir_all(path.as_ref().join(SSTABLES_DIR_PATH))?;

        let mut memtable = BTreeMap::<K, V>::new();
        let mut commit_log_path = None;
        for path in utils::read_dir_sorted(path.as_ref().join(COMMIT_LOGS_DIR_PATH))? {
            Self::read_commit_log(&path, &mut memtable)?;
            commit_log_path = Some(path);
        }

        let commit_log_path = commit_log_path.unwrap_or(utils::new_timestamped_path(
            path.as_ref().join(COMMIT_LOGS_DIR_PATH),
            "data",
        ));
        let commit_log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&commit_log_path)?;

        let sstables = utils::read_dir_sorted(path.as_ref().join(SSTABLES_DIR_PATH))?
            .into_iter()
            .map(SSTable::read_from_file)
            .collect::<Result<Vec<_>>>()?;

        let ret = Self {
            path: path.as_ref().into(),
            memtable,
            commit_log_path,
            commit_log,
            memtable_in_flush: None,
            sstables,
        };
        Ok(ret)
    }

    fn read_commit_log(path: &PathBuf, memtable: &mut BTreeMap<K, V>) -> Result<()> {
        let file = File::open(path)?;
        let iter = KeyValueIterator::<K, V>::from(file);
        for file_data in iter {
            let (key, val) = file_data?;
            memtable.insert(key, val);
        }
        Ok(())
    }

    fn flush_memtable(&mut self) -> Result<()> {
        let new_cl_path = utils::new_timestamped_path(self.path.join(COMMIT_LOGS_DIR_PATH), "data");
        let new_cl = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&new_cl_path)?;
        let old_cl_path: PathBuf;
        {
            // TODO MutexGuard here
            let old_mt = mem::replace(&mut self.memtable, Default::default());
            self.memtable_in_flush = Some(old_mt);

            self.commit_log = new_cl;
            old_cl_path = mem::replace(&mut self.commit_log_path, new_cl_path);
        }

        let mtf = self
            .memtable_in_flush
            .as_ref()
            .ok_or(anyhow!("Unexpected error: no memtable being flushed"))?;
        let new_sst = SSTable::write_from_mem(
            mtf,
            utils::new_timestamped_path(self.path.join(SSTABLES_DIR_PATH), "data"),
        )?;

        {
            // TODO MutexGuard here
            self.sstables.push(new_sst);
            self.memtable_in_flush.take();
        }
        fs::remove_file(old_cl_path)?;

        Ok(())
    }

    fn compact_sstables(&mut self) -> Result<()> {
        let new_table_path = utils::new_timestamped_path(self.path.join(SSTABLES_DIR_PATH), "data");
        let new_table = SSTable::compact(new_table_path, &self.sstables)?;
        let new_tables = vec![new_table];

        // TODO MutexGuard here
        // In async version, we will have to assume that new sstables may have been created while we were compacting, so we won't be able to just swap.
        let old_tables = mem::replace(&mut self.sstables, new_tables);
        for table in old_tables {
            table.remove_file()?;
        }

        Ok(())
    }

    fn check_start_job(&mut self) -> Result<()> {
        if self.memtable.len() >= MEMTABLE_FLUSH_SIZE_THRESH {
            self.flush_memtable()?;
        }
        if self.sstables.len() >= SSTABLE_COMPACT_COUNT_THRESH {
            self.compact_sstables()?;
        }
        Ok(())
    }

    pub fn put(&mut self, k: K, v: V) -> Result<()> {
        serde::serialize_kv(&k, &v, &mut self.commit_log)?;

        self.memtable.insert(k, v);

        self.check_start_job()?;

        Ok(())
    }

    pub fn get(&self, k: &K) -> Result<Option<V>> {
        if let Some(v) = self.memtable.get(k) {
            return Ok(Some(v.clone()));
        }
        if let Some(mtf) = &self.memtable_in_flush {
            if let Some(v) = mtf.get(k) {
                return Ok(Some(v.clone()));
            }
        }
        // TODO bloom filter here
        for ss in self.sstables.iter().rev() {
            let v = ss.get(k)?;
            if v.is_some() {
                return Ok(v);
            }
        }
        Ok(None)
    }

    pub fn get_range<'a, Flo, Fhi>(
        &'a self,
        k_lo_cmp: Option<&'a Flo>,
        k_hi_cmp: Option<&'a Fhi>,
    ) -> Result<impl Iterator<Item = Result<(K, V)>> + 'a>
    where
        Flo: Fn(&K) -> Ordering,
        Fhi: Fn(&K) -> Ordering,
    {
        let ssts_iter = SSTable::merge_range(&self.sstables, k_lo_cmp, k_hi_cmp)?;

        let mts_iter = [self.memtable_in_flush.as_ref(), Some(&self.memtable)]
            .iter()
            .filter_map(|mt| *mt)
            .enumerate()
            .map(|(mt_i, mt)| {
                let mut iter = mt.iter();

                if let Some(k_lo_cmp) = k_lo_cmp {
                    // Find the max key less than the desired key. Not equal to it, b/c
                    // `.nth()` takes the item at the provided position.
                    if let Some(iter_pos) = mt.iter().rposition(|(k, _v)| k_lo_cmp(k).is_lt()) {
                        iter.nth(iter_pos);
                    }
                }

                let k_hi_cmp = k_hi_cmp.clone();
                iter.take_while(move |(k, _v)| {
                    // This closure moves k_hi_cmp.
                    if let Some(k_hi_cmp) = k_hi_cmp {
                        k_hi_cmp(k).is_le()
                    } else {
                        true
                    }
                })
                .zip(std::iter::repeat(mt_i))
            })
            .kmerge_by(|((a_k, _a_v), a_i), ((b_k, _b_v), b_i)| {
                a_k < b_k || (a_k == b_k && a_i > b_i)
            })
            .unique_by(|((k, _v), _mt_i)| k.clone())
            .map(|(kv, _mt_i)| kv);

        let mut ssts_iter = ssts_iter.peekable();
        let mut mts_iter = mts_iter.peekable();

        let out_iter_fn = move || -> Option<Result<(K, V)>> {
            let next_is_sst = match (ssts_iter.peek(), mts_iter.peek()) {
                (None, None) => return None,
                (None, Some(_)) => false,
                (Some(_), None) => true,
                (Some(Err(_)), _) => true,
                (Some(Ok((sst_k, _sst_v))), Some((mt_k, _mt_v))) => {
                    if &sst_k < mt_k {
                        true
                    } else if &sst_k > mt_k {
                        false
                    } else {
                        ssts_iter.next();
                        false
                    }
                }
            };

            if next_is_sst {
                return ssts_iter.next();
            } else {
                let (k, v) = mts_iter.next().unwrap();
                let kv = (k.clone(), v.clone());
                return Some(Ok(kv));
            }
        };
        let out_iter = std::iter::from_fn(out_iter_fn);
        Ok(out_iter)
    }
}
