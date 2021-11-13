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

mod sstable;

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::hash::Hash;
use std::mem;
use std::path::{Path, PathBuf};

use anyhow::Result;

use crate::storage::serde::{self, KeyValueIterator, OptDatum, Serializable};
use crate::storage::utils;
use sstable::SSTable;

static COMMIT_LOGS_DIR_PATH: &'static str = "commit_logs";
static SSTABLES_DIR_PATH: &'static str = "sstables";
static MEMTABLE_FLUSH_SIZE_THRESH: usize = 7;
static SSTABLE_COMPACT_COUNT_THRESH: usize = 4;

pub struct LSMTree<K, V>
where
    V: Serializable + Clone,
{
    path: PathBuf,
    memtable: BTreeMap<K, OptDatum<V>>,
    commit_log: File,
    sstables: Vec<SSTable<K, OptDatum<V>>>,
}

impl<K, V> LSMTree<K, V>
where
    K: Serializable + Ord + Hash + Clone,
    V: Serializable + Clone,
{
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        std::fs::create_dir_all(path.as_ref().join(COMMIT_LOGS_DIR_PATH))?;
        std::fs::create_dir_all(path.as_ref().join(SSTABLES_DIR_PATH))?;

        let mut memtable = Default::default();
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
            commit_log,
            sstables,
        };
        Ok(ret)
    }

    fn read_commit_log(path: &PathBuf, memtable: &mut BTreeMap<K, OptDatum<V>>) -> Result<()> {
        let file = File::open(path)?;
        let iter = KeyValueIterator::<K, OptDatum<V>>::from(file);
        for file_data in iter {
            let (key, val) = file_data?;
            memtable.insert(key, val);
        }
        Ok(())
    }

    fn flush_memtable(&mut self) -> Result<()> {
        let new_sst = SSTable::write_from_mem(
            &self.memtable,
            utils::new_timestamped_path(self.path.join(SSTABLES_DIR_PATH), "data"),
        )?;
        self.sstables.push(new_sst);

        self.memtable.clear();
        self.commit_log.set_len(0)?;

        Ok(())
    }

    fn compact_sstables(&mut self) -> Result<()> {
        let new_table_path = utils::new_timestamped_path(self.path.join(SSTABLES_DIR_PATH), "data");
        let new_table = SSTable::compact(new_table_path, &self.sstables)?;
        let new_tables = vec![new_table];

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

    fn do_put(&mut self, k: K, v: OptDatum<V>) -> Result<()> {
        serde::serialize_kv(&k, &v, &mut self.commit_log)?;

        self.memtable.insert(k, v);

        self.check_start_job()?;

        Ok(())
    }

    pub fn put(&mut self, k: K, v: V) -> Result<()> {
        self.do_put(k, OptDatum::Some(v))
    }

    pub fn del(&mut self, k: K) -> Result<()> {
        self.do_put(k, OptDatum::Tombstone)
    }

    fn do_get(&self, k: &K) -> Result<Option<OptDatum<V>>> {
        if let Some(v) = self.memtable.get(k) {
            return Ok(Some(v.clone()));
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
    pub fn get(&self, k: &K) -> Result<Option<V>> {
        match self.do_get(k)? {
            Some(OptDatum::Tombstone) | None => Ok(None),
            Some(OptDatum::Some(v)) => Ok(Some(v)),
        }
    }

    pub fn get_range<Flo, Fhi>(
        &self,
        k_lo_cmp: Option<&Flo>,
        k_hi_cmp: Option<&Fhi>,
    ) -> Result<Vec<(K, V)>>
    where
        Flo: Fn(&K) -> Ordering,
        Fhi: Fn(&K) -> Ordering,
    {
        let ssts_iter = SSTable::merge_range(&self.sstables, k_lo_cmp, k_hi_cmp)?;

        let mut mt_iter = self.memtable.iter();

        if let Some(k_lo_cmp) = k_lo_cmp {
            // Find the max key less than the desired key. Not equal to it, b/c
            // `.nth()` takes the item at the provided position.
            if let Some(iter_pos) = self
                .memtable
                .iter()
                .rposition(|(k, _v)| k_lo_cmp(k).is_lt())
            {
                mt_iter.nth(iter_pos);
            }
        }

        let k_hi_cmp = k_hi_cmp.clone();
        let mt_iter = mt_iter.take_while(move |(k, _v)| {
            // This closure moves k_hi_cmp.
            if let Some(k_hi_cmp) = k_hi_cmp {
                k_hi_cmp(k).is_le()
            } else {
                true
            }
        });

        let mut ssts_iter = ssts_iter.peekable();
        let mut mt_iter = mt_iter.peekable();

        /*
        Here we're doing k-merge between (the iterator of all sstables) and (the iterator of all memtables).
        We have to do this manually due to type difference.
        An sstable iterator yields Item = Result<(K, V)>.
        A memtable iterator yields Item = (&K, &V).
        */
        let out_iter_fn = move || -> Option<Result<(K, OptDatum<V>)>> {
            let next_is_sst = match (ssts_iter.peek(), mt_iter.peek()) {
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
                let (k, v) = mt_iter.next().unwrap();
                let kv = (k.clone(), v.clone());
                return Some(Ok(kv));
            }
        };
        std::iter::from_fn(out_iter_fn)
            .filter_map(|res_kv| match res_kv {
                Err(e) => Some(Err(e)),
                Ok((_k, OptDatum::Tombstone)) => None,
                Ok((k, OptDatum::Some(v))) => Some(Ok((k, v))),
            })
            .collect::<Result<Vec<_>>>()
    }
}
