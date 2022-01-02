use crate::storage::lsm::Entry;
use crate::storage::serde::{self, KeyValueIterator, OptDatum, ReadItem, Serializable, SkipItem};
use anyhow::{anyhow, Result};
use derive_more::{Deref, DerefMut, From};
use std::cmp::{Ord, Ordering, PartialOrd};
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom};
use std::iter;
use std::marker::PhantomData;
use std::path::PathBuf;

#[derive(Clone, Copy)]
struct FileOffset(u64);

static SSTABLE_IDX_SPARSENESS: usize = 3;

fn is_kv_sparsely_captured(entry_i: usize) -> bool {
    entry_i % SSTABLE_IDX_SPARSENESS == SSTABLE_IDX_SPARSENESS - 1
}

/// An SSTable has these components:
/// - A file which stores `(key, val_or_tombstone)` pairs, sorted by key, containing distinct keys.
/// - An in-memory sorted structure that maps `{key: file_offset}` on sparsely captured keys. The offsets point to locations within the above file.
pub struct SSTable<K, V> {
    path: PathBuf,
    sparse_file_offsets: SparseIndex<K>,
    _phant: PhantomData<V>,
}

impl<K, V> SSTable<K, V>
where
    K: Serializable + Ord + Clone,
    V: Serializable,
{
    pub fn new<'a>(
        entries: impl Iterator<Item = Entry<'a, K, OptDatum<V>>>,
        path: PathBuf,
    ) -> Result<Self>
    where
        K: 'a,
        V: 'a,
    {
        let mut sparse_file_offsets = SparseIndex::from(vec![]);
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)?;
        let mut file_offset = FileOffset(0);

        for (entry_i, entry) in entries.enumerate() {
            let (k_ref, v_ref) = entry.borrow_res()?;
            let delta_offset = serde::serialize_kv(k_ref, v_ref, &mut file)?;

            if is_kv_sparsely_captured(entry_i) {
                let k_own = entry.take_k()?;
                sparse_file_offsets.push((k_own, file_offset));
            }

            file_offset.0 += delta_offset as u64;
        }

        file.sync_all()?;

        Ok(Self {
            path,
            sparse_file_offsets,
            _phant: PhantomData,
        })
    }

    pub fn load(path: PathBuf) -> Result<Self> {
        let mut sparse_file_offsets = SparseIndex::from(vec![]);
        let mut file = File::open(&path)?;
        let mut file_offset = FileOffset(0);
        for entry_i in 0usize.. {
            // Key
            if is_kv_sparsely_captured(entry_i) {
                match serde::read_item::<K>(&mut file)? {
                    ReadItem::EOF => break,
                    ReadItem::Some { read_size, obj } => {
                        sparse_file_offsets.push((obj, file_offset));
                        file_offset.0 += read_size as u64;
                    }
                }
            } else {
                match serde::skip_item(&mut file)? {
                    SkipItem::EOF => break,
                    SkipItem::Some { read_size } => {
                        file_offset.0 += read_size as u64;
                    }
                }
            }

            // Value
            match serde::skip_item(&mut file)? {
                SkipItem::EOF => {
                    return Err(anyhow!("EOF while reading a Value from {:?}.", path));
                }
                SkipItem::Some { read_size } => {
                    file_offset.0 += read_size as u64;
                }
            }
        }

        Ok(Self {
            path,
            sparse_file_offsets,
            _phant: PhantomData,
        })
    }

    pub fn get_one<Q>(&self, k: &Q) -> Option<Result<(K, OptDatum<V>)>>
    where
        K: PartialOrd<Q>,
    {
        let mut iter = self.get_range(Some(k), None).take(1);
        iter.next().filter(|res| match res {
            Err(_) => true,
            Ok((sample_k, _)) => sample_k.partial_cmp(k).unwrap_or(Ordering::Equal).is_eq(),
        })
    }

    /// 1. Bisect in the in-memory sparse index, to find the lower-bound file offset.
    /// 1. Seek the offset in the file. Then read linearlly in file until either EOF or the last-read key is greater than the sought key.
    pub fn get_range<'a, Q>(
        &'a self,
        k_lo: Option<&'a Q>,
        k_hi: Option<&'a Q>,
    ) -> impl 'a + Iterator<Item = Result<(K, OptDatum<V>)>>
    where
        K: PartialOrd<Q>,
    {
        let file_offset = self.sparse_file_offsets.nearest_preceding_file_offset(k_lo);

        let mut res_file_iter = File::open(&self.path)
            .and_then(|mut file| -> Result<File, _> {
                file.seek(SeekFrom::Start(file_offset.0)).map(|_| file)
            })
            .map_err(|e| anyhow!(e))
            .map(|file| {
                KeyValueIterator::<K, OptDatum<V>>::from(file)
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
                                return sample_k
                                    .partial_cmp(k_hi)
                                    .unwrap_or(Ordering::Less)
                                    .is_le();
                            }
                        }
                        true
                    })
            });

        let ret_iter_fn = move || -> Option<Result<(K, OptDatum<V>)>> {
            match res_file_iter.as_mut() {
                Err(e) =>
                // This error occurred during the construction of the iter.
                // Return the err as an iterator item.
                {
                    Some(Err(anyhow!(e.to_string())))
                }
                Ok(file_iter) => file_iter.next(),
            }
        };
        iter::from_fn(ret_iter_fn)
    }

    pub fn remove_file(&self) -> Result<()> {
        fs::remove_file(&self.path)?;
        Ok(())
    }
}

#[derive(From, Deref, DerefMut)]
struct SparseIndex<K>(Vec<(K, FileOffset)>);

impl<K> SparseIndex<K> {
    fn nearest_preceding_file_offset<Q>(&self, k_lo: Option<&Q>) -> FileOffset
    where
        K: PartialOrd<Q>,
    {
        if k_lo.is_none() {
            return FileOffset(0);
        }
        let k_lo = k_lo.unwrap();

        // TODO Bisect. Currently this has O(n) cost.

        // Find the max key less than or equal to the desired key.
        let idx_pos = self.0.iter().rposition(|(sample_k, _off)| {
            sample_k
                .partial_cmp(k_lo)
                .unwrap_or(Ordering::Greater)
                .is_le()
        });
        match idx_pos {
            None => FileOffset(0),
            Some(idx_pos) => {
                let (_, file_offset) = self.0[idx_pos];
                file_offset
            }
        }
    }
}
