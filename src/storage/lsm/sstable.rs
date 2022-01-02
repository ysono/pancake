//! An SSTable is an abstraction of a sorted key-value dictionary.
//!
//! Its components are:
//! - A file which stores `(key, value)` pairs, sorted by key, containing distinct keys.
//! - An in-memory sorted structure that maps `{key: file_offset}` on sparsely captured keys. The offsets point to locations within the above file.

use crate::storage::serde::{self, KeyValueIterator, ReadItem, Serializable, SkipItem};
use anyhow::{anyhow, Result};
use derive_more::{Deref, DerefMut};
use itertools::Itertools;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::hash::Hash;
use std::io::{Seek, SeekFrom};
use std::marker::PhantomData;
use std::path::PathBuf;

type FileOffset = u64;

static SSTABLE_IDX_SPARSENESS: usize = 3;

fn is_kv_sparsely_captured(kv_i: usize) -> bool {
    kv_i % SSTABLE_IDX_SPARSENESS == SSTABLE_IDX_SPARSENESS - 1
}

/// One SS Table. It consists of a file on disk and an in-memory sparse indexing of the file.
#[derive(Debug)]
pub struct SSTable<K, V> {
    path: PathBuf,
    sparse_index: SparseIndex<K>,
    phantom: PhantomData<V>,
}

impl<K, V> SSTable<K, V>
where
    K: Serializable + Ord + Hash + Clone,
    V: Serializable + Clone,
{
    pub fn write_from_mem(mem: &BTreeMap<K, V>, path: PathBuf) -> Result<Self> {
        let mut sparse_index = SparseIndex::<K>::new();
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)?;
        let mut offset = 0usize;
        for (kv_i, (k, v)) in mem.iter().enumerate() {
            let delta_offset = serde::serialize_kv(k, v, &mut file)?;

            if is_kv_sparsely_captured(kv_i) {
                sparse_index.push((k.clone(), offset as FileOffset));
            }

            offset += delta_offset;
        }

        Ok(Self {
            path,
            sparse_index,
            phantom: PhantomData,
        })
    }

    pub fn read_from_file(path: PathBuf) -> Result<Self> {
        let mut sparse_index = SparseIndex::<K>::new();
        let mut file = File::open(&path)?;
        let mut offset = 0usize;
        for kv_i in 0usize.. {
            // Key
            if is_kv_sparsely_captured(kv_i) {
                match serde::read_item::<K>(&mut file)? {
                    ReadItem::EOF => break,
                    ReadItem::Some { read_size, obj } => {
                        sparse_index.push((obj, offset as FileOffset));
                        offset += read_size;
                    }
                }
            } else {
                match serde::skip_item(&mut file)? {
                    SkipItem::EOF => break,
                    SkipItem::Some { read_size } => {
                        offset += read_size;
                    }
                }
            }

            // Value
            match serde::skip_item(&mut file)? {
                SkipItem::EOF => return Err(anyhow!("Unexpected EOF while reading a value.")),
                SkipItem::Some { read_size } => {
                    offset += read_size;
                }
            }
        }

        Ok(Self {
            path,
            sparse_index,
            phantom: PhantomData,
        })
    }

    /// Both the in-memory index and the file are sorted by key.
    /// The index maps { key : file offset } for a sparse subsequence of keys.
    /// 1. Bisect in the in-memory sparse index, to find the lower-bound file offset.
    /// 1. Seek the offset in the file. Then read linearlly in file until either EOF or the last-read key is greater than the sought key.
    ///
    /// @return
    ///     `None` if not found within this sstable.
    ///     `Some(_: V)` if found.
    pub fn get<Q>(&self, k: &Q) -> Result<Option<V>>
    where
        K: PartialOrd<Q>,
    {
        let mut iter = self.get_range(Some(k), None)?.take(1);
        match iter.next() {
            None => Ok(None),
            Some(Err(e)) => Err(e),
            Some(Ok((sample_k, sample_v))) => {
                if sample_k.partial_cmp(k).unwrap_or(Ordering::Equal).is_eq() {
                    Ok(Some(sample_v))
                } else {
                    Ok(None)
                }
            }
        }
    }

    fn get_range<'a, Q>(
        &'a self,
        k_lo: Option<&'a Q>,
        k_hi: Option<&'a Q>,
    ) -> Result<impl Iterator<Item = Result<(K, V)>> + 'a>
    where
        K: PartialOrd<Q>,
    {
        let file_offset = self.sparse_index.nearest_preceding_file_offset(k_lo);

        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(file_offset))?;

        let iter = KeyValueIterator::<K, V>::from(file)
            .skip_while(move |res| {
                // This closure moves k_lo.
                if let Some(k_lo) = k_lo {
                    if let Ok((sample_k, _v)) = res {
                        return sample_k
                            .partial_cmp(k_lo)
                            .unwrap_or(Ordering::Greater)
                            .is_lt();
                    }
                }
                false
            })
            .take_while(move |res| {
                // This closure moves k_hi.
                if let Some(k_hi) = k_hi {
                    if let Ok((sample_k, _v)) = res {
                        return sample_k.partial_cmp(k_hi).unwrap_or(Ordering::Less).is_le();
                    }
                }
                true
            });

        Ok(iter)
    }

    pub fn remove_file(&self) -> Result<()> {
        fs::remove_file(&self.path)?;
        Ok(())
    }

    pub fn merge_range<'a, Q>(
        tables: &'a [Self],
        k_lo: Option<&'a Q>,
        k_hi: Option<&'a Q>,
    ) -> Result<impl Iterator<Item = Result<(K, V)>> + 'a>
    where
        K: PartialOrd<Q>,
    {
        let per_table_iters = tables
            .iter()
            .enumerate()
            .map(|(table_i, sst)| {
                // NB: the index/position of the sstable is included for the purpose of breaking ties
                // on duplicate keys.
                sst.get_range(k_lo, k_hi)
                    .map(|iter| iter.zip(std::iter::repeat(table_i)))
            })
            .collect::<Result<Vec<_>>>()?;

        let merged_iter = per_table_iters
            .into_iter()
            .kmerge_by(|(a_res_kv, a_i), (b_res_kv, b_i)| {
                /*
                the comparator contract dictates we return true iff |a| is ordered before |b|
                    or said differently: |a| < |b|.

                for equal keys, we define |a| < |b| iff |a| is more recent.
                    note: |a| is more recent when index_a > index_b.

                by defining the ordering in this way,
                    we only keep the first instance of key |k| in the compacted iterator.
                    duplicate items with key |k| must be discarded.

                In case of any error, mark it as the lesser item, for early termination.
                 */
                match (a_res_kv, b_res_kv) {
                    (Err(_), _) => true,
                    (_, Err(_)) => false,
                    (Ok((a_k, _a_v)), Ok((b_k, _b_v))) => a_k < b_k || (a_k == b_k && a_i > b_i),
                }
            })
            .unique_by(|(res_kv, _table_i)| {
                // `anyhow::Error` cannot be compared, so convert to `Option`.
                res_kv.as_ref().ok().map(|(k, _v)| k.clone())
            })
            .map(|(res_kv, _table_i)|
                // The table index is no longer needed.
                res_kv);

        Ok(merged_iter)
    }

    pub fn compact(path: PathBuf, tables: &[Self]) -> Result<Self> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)?;

        let mut sparse_index = SparseIndex::new();
        let mut offset = 0 as FileOffset;

        let merged_iter = Self::merge_range(tables, None, None)?;
        for (kv_i, res_kv) in merged_iter.enumerate() {
            let (k, v) = res_kv?;

            let delta_offset = serde::serialize_kv(&k, &v, &mut file)?;

            if is_kv_sparsely_captured(kv_i) {
                sparse_index.push((k, offset));
            }

            offset += delta_offset as FileOffset;
        }

        file.sync_all()?;

        let compacted = Self {
            path,
            sparse_index,
            phantom: PhantomData,
        };

        Ok(compacted)
    }
}

#[derive(Deref, DerefMut, Debug)]
struct SparseIndex<K> {
    ptrs: Vec<(K, FileOffset)>,
}

impl<K: Serializable + Ord> SparseIndex<K> {
    fn new() -> Self {
        Self {
            ptrs: Default::default(),
        }
    }

    fn nearest_preceding_file_offset<Q>(&self, k_lo: Option<&Q>) -> FileOffset
    where
        K: PartialOrd<Q>,
    {
        if k_lo.is_none() {
            return 0;
        }
        let k_lo = k_lo.unwrap();

        // TODO Bisect. Currently this has O(n) cost.

        // Find the max key less than or equal to the desired key.
        let idx_pos = self.ptrs.iter().rposition(|(sample_k, _off)| {
            sample_k
                .partial_cmp(k_lo)
                .unwrap_or(Ordering::Greater)
                .is_le()
        });
        match idx_pos {
            None => 0u64,
            Some(idx_pos) => {
                let (_, file_offset) = self.ptrs[idx_pos];
                file_offset
            }
        }
    }
}
