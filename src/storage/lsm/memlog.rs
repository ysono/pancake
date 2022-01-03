use crate::storage::serde::{DatumWriter, KeyValueIterator, OptDatum, Ser, Serializable};
use anyhow::Result;
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

pub struct MemLog<K, V> {
    memtable: BTreeMap<K, OptDatum<V>>,
    log_path: PathBuf,
    log_writer: DatumWriter<File>,
}

impl<K, V> MemLog<K, V>
where
    K: Serializable + Ord,
    OptDatum<V>: Serializable,
{
    pub fn load_or_new<P: AsRef<Path>>(log_path: P) -> Result<Self> {
        let mut memtable = BTreeMap::default();
        if log_path.as_ref().exists() {
            let log_file = File::open(&log_path)?;
            let iter = KeyValueIterator::<K, OptDatum<V>>::from(log_file);
            for res_kv in iter {
                let (k, v) = res_kv?;
                memtable.insert(k, v);
            }
        }

        let log_file = OpenOptions::new()
            .create(true)
            .append(true) // *Not* write(true)
            .open(&log_path)?;
        let log_writer = DatumWriter::from(BufWriter::new(log_file));

        Ok(Self {
            memtable,
            log_path: log_path.as_ref().into(),
            log_writer,
        })
    }

    pub fn clear(&mut self) -> Result<()> {
        self.memtable.clear();
        self.log_writer.flush()?;
        let log_file = OpenOptions::new().write(true).open(&self.log_path)?;
        log_file.set_len(0)?;
        self.log_writer = DatumWriter::from(BufWriter::new(log_file));
        Ok(())
    }

    pub fn mem_len(&self) -> usize {
        self.memtable.len()
    }

    pub fn put(&mut self, k: K, v: OptDatum<V>) -> Result<()> {
        k.ser(&mut self.log_writer)?;
        v.ser(&mut self.log_writer)?;
        self.log_writer.flush()?;

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
