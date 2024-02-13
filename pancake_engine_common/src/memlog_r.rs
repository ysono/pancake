use crate::fs_utils;
use anyhow::Result;
use pancake_types::{iters::KeyValueIterator, types::Deser};
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

/// A MemLog is a sorted dictionary (called Memtable), backed up by a write-ahead log file.
pub struct ReadonlyMemLog<K, V> {
    pub memtable: BTreeMap<K, V>,
    pub log_path: PathBuf,
}

impl<K, V> ReadonlyMemLog<K, V>
where
    K: Deser + Ord,
    V: Deser,
{
    pub fn load<P: AsRef<Path>>(log_path: P) -> Result<Self> {
        let log_path = log_path.as_ref();

        let mut memtable = BTreeMap::default();
        if log_path.exists() {
            let log_file = fs_utils::open_file(log_path, OpenOptions::new().read(true))?;
            let iter = KeyValueIterator::<K, V>::from(log_file);
            for res_kv in iter {
                let (k, v) = res_kv?;
                memtable.insert(k, v);
            }
        }

        Ok(Self {
            memtable,
            log_path: log_path.into(),
        })
    }

    pub fn mem_len(&self) -> usize {
        self.memtable.len()
    }

    pub fn get_one<Q>(&self, k: &Q) -> Option<(&K, &V)>
    where
        Q: Ord,
        K: Borrow<Q>,
    {
        self.memtable.get_key_value(k)
    }

    pub fn get_range<'a, Q>(
        &'a self,
        k_lo: Option<&'a Q>,
        k_hi: Option<&'a Q>,
    ) -> impl Iterator<Item = (&K, &V)>
    where
        K: PartialOrd<Q>,
    {
        self.memtable
            .iter()
            .skip_while(move |(sample_k, _v)| match k_lo {
                None => false,
                Some(k_lo) => sample_k
                    .partial_cmp(&k_lo)
                    .unwrap_or(Ordering::Greater)
                    .is_lt(),
            })
            .take_while(move |(sample_k, _v)| match k_hi {
                None => true,
                Some(k_hi) => sample_k
                    .partial_cmp(&k_hi)
                    .unwrap_or(Ordering::Less)
                    .is_le(),
            })
    }

    pub fn get_whole_range(&self) -> impl Iterator<Item = (&K, &V)> {
        self.memtable.iter()
    }
}
