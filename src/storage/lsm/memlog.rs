use crate::storage::serde::{self, KeyValueIterator, OptDatum, Serializable};
use anyhow::Result;
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::path::Path;

pub struct MemLog<K, V> {
    memtable: BTreeMap<K, OptDatum<V>>,
    commit_log: File,
}

impl<K, V> MemLog<K, V>
where
    K: Serializable + Ord,
    V: Serializable,
{
    pub fn load_or_new<P: AsRef<Path>>(cl_path: P) -> Result<Self> {
        let mut memtable = BTreeMap::default();
        if cl_path.as_ref().exists() {
            let cl_read = File::open(&cl_path)?;
            let iter = KeyValueIterator::<K, OptDatum<V>>::from(cl_read);
            for res_kv in iter {
                let (k, v) = res_kv?;
                memtable.insert(k, v);
            }
        }

        let commit_log = OpenOptions::new()
            .create(true)
            .append(true) // *Not* write(true)
            .open(&cl_path)?;

        Ok(Self {
            memtable,
            commit_log,
        })
    }

    pub fn clear(&mut self) -> Result<()> {
        self.memtable.clear();
        self.commit_log.set_len(0)?;
        Ok(())
    }

    pub fn mem_len(&self) -> usize {
        self.memtable.len()
    }

    pub fn put(&mut self, k: K, v: OptDatum<V>) -> Result<()> {
        serde::serialize_kv(&k, &v, &mut self.commit_log)?;

        self.memtable.insert(k, v);

        Ok(())
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
        /*
        The intent here is to first search for tree node in O(log(n)), then iterate from there.
        If this is not possible, then iterating twice in this fashion is obviously wasteful.
        */

        let mut iter = self.memtable.iter();

        if let Some(k_lo) = k_lo {
            /*
            Find the max key less than the desired key. Not equal to it, b/c
                `.nth()` takes the item at the provided position.
            */
            if let Some(iter_pos) = self.memtable.iter().rposition(|(sample_k, _v)| {
                sample_k
                    .partial_cmp(k_lo)
                    .unwrap_or(Ordering::Greater)
                    .is_lt()
            }) {
                iter.nth(iter_pos);
            }
        }

        iter.take_while(move |(sample_k, _v)| {
            // This closure moves k_hi.
            if let Some(k_hi) = k_hi {
                return sample_k
                    .partial_cmp(&k_hi)
                    .unwrap_or(Ordering::Less)
                    .is_le();
            }
            true
        })
    }

    pub fn get_whole_range(&self) -> impl Iterator<Item = (&K, &OptDatum<V>)> {
        self.memtable.iter()
    }
}
