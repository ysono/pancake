use crate::storage::serde::{Deser, KeyValueIterator, OptDatum};
use anyhow::Result;
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};

/// A MemLog is a sorted dictionary (called Memtable), backed up by a write-ahead log file.
pub struct ReadonlyMemLog<K, V> {
    pub memtable: BTreeMap<K, OptDatum<V>>,
    pub log_path: PathBuf,
}

impl<K, V> ReadonlyMemLog<K, V>
where
    K: Deser + Ord,
    OptDatum<V>: Deser,
{
    pub fn load<P: AsRef<Path>>(log_path: P) -> Result<Self> {
        let log_path = log_path.as_ref();

        let mut memtable = BTreeMap::default();
        if log_path.exists() {
            let log_file = File::open(log_path)?;
            let iter = KeyValueIterator::<K, OptDatum<V>>::from(log_file);
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

    pub fn get_one<Q>(&self, k: &Q) -> Option<(&K, &OptDatum<V>)>
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
    ) -> impl Iterator<Item = (&K, &OptDatum<V>)>
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

    pub fn get_whole_range(&self) -> impl Iterator<Item = (&K, &OptDatum<V>)> {
        self.memtable.iter()
    }
}
