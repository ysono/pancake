use crate::{ds_n_a::bisect, entry::Entry, fs_utils};
use anyhow::{anyhow, Result};
use derive_more::{Deref, DerefMut, From};
use pancake_types::{
    iters::KeyValueReader,
    serde::ReadResult,
    types::{Deser, Ser},
};
use std::cmp::{Ord, Ordering, PartialOrd};
use std::fs::OpenOptions;
use std::io::{BufWriter, SeekFrom, Write};
use std::iter;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

/// The sparseness is exaggeratedly small, so as to be helpful with debugging.
/// In the future, we'll allow setting it from an env var.
const FILE_OFFSETS_SPARSENESS: usize = 3;

fn is_kv_sparsely_captured(entry_i: usize) -> bool {
    entry_i % FILE_OFFSETS_SPARSENESS == FILE_OFFSETS_SPARSENESS - 1
}

#[derive(Clone, Copy)]
struct FileOffset(u64);

/// An SSTable is an abstraction of a sorted dictionary.
/// An SSTable has these components:
/// - A file which stores `(key, val_or_tombstone)` pairs, sorted by key, containing distinct keys.
/// - An in-memory sorted structure that maps `{key: file_offset}` on sparsely captured keys. The offsets point to locations within the above file.
pub struct SSTable<K, V> {
    sparse_file_offsets: SparseFileOffsets<K>,
    kv_file_path: PathBuf,
    _phant: PhantomData<V>,
}

impl<K, V> SSTable<K, V>
where
    K: Ser + Ord,
    V: Ser,
{
    pub fn new<'a>(
        entries: impl Iterator<Item = Entry<'a, K, V>>,
        kv_file_path: PathBuf,
    ) -> Result<Self>
    where
        K: 'a + Clone,
        V: 'a,
    {
        let kv_file = fs_utils::open_file(
            &kv_file_path,
            OpenOptions::new().create_new(true).write(true),
        )?;
        let mut w = BufWriter::new(kv_file);

        let mut sparse_file_offsets = SparseFileOffsets::from(vec![]);
        let mut file_offset = FileOffset(0);

        for (entry_i, entry) in entries.enumerate() {
            let (k_ref, v_ref) = entry.try_borrow()?;

            let mut delta_offset = 0;
            delta_offset += *(k_ref.ser(&mut w)?);
            delta_offset += *(v_ref.ser(&mut w)?);

            if is_kv_sparsely_captured(entry_i) {
                let k_own = entry.into_owned_k()?;
                sparse_file_offsets.push((k_own, file_offset));
            }

            file_offset.0 += delta_offset as u64;
        }

        w.flush()?;

        Ok(Self {
            sparse_file_offsets,
            kv_file_path,
            _phant: PhantomData,
        })
    }
}

impl<K, V> SSTable<K, V>
where
    K: Deser + Ord,
    V: Deser,
{
    pub fn load<P: AsRef<Path>>(kv_file_path: P) -> Result<Self> {
        let kv_file_path = kv_file_path.as_ref();

        let kv_file = fs_utils::open_file(kv_file_path, OpenOptions::new().read(true))?;
        let mut reader = KeyValueReader::<_, K, V>::from(kv_file);

        let mut sparse_file_offsets = SparseFileOffsets::from(vec![]);
        let mut file_offset = FileOffset(0);

        for entry_i in 0usize.. {
            if is_kv_sparsely_captured(entry_i) {
                match reader.deser_k_skip_v()? {
                    ReadResult::EOF => break,
                    ReadResult::Some(delta_r_len, k) => {
                        sparse_file_offsets.push((k, file_offset));
                        file_offset.0 += delta_r_len as u64;
                    }
                }
            } else {
                match reader.skip_kv()? {
                    ReadResult::EOF => break,
                    ReadResult::Some(delta_r_len, ()) => file_offset.0 += delta_r_len as u64,
                }
            }
        }

        Ok(Self {
            sparse_file_offsets,
            kv_file_path: kv_file_path.into(),
            _phant: PhantomData,
        })
    }

    pub fn get_one<Q>(&self, k: &Q) -> Option<Result<(K, V)>>
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
    ) -> impl 'a + Iterator<Item = Result<(K, V)>>
    where
        K: PartialOrd<Q>,
    {
        let file_offset = self.sparse_file_offsets.nearest_preceding_file_offset(k_lo);

        let res_file = fs_utils::open_file(&self.kv_file_path, OpenOptions::new().read(true));
        let mut res_file_iter = res_file.and_then(|mut file| {
            fs_utils::seek(
                &mut file,
                SeekFrom::Start(file_offset.0),
                &self.kv_file_path,
            )?;

            let iter = KeyValueReader::from(file).into_iter_kv_range(k_lo, k_hi);

            Ok(iter)
        });

        let ret_iter_fn = move || -> Option<Result<(K, V)>> {
            match res_file_iter.as_mut() {
                Err(e) => {
                    // This error occurred during the construction of the iterator.
                    // Return the err as an iterator item.
                    Some(Err(anyhow!(e.to_string())))
                }
                Ok(file_iter) => file_iter.next(),
            }
        };
        iter::from_fn(ret_iter_fn)
    }

    pub fn get_all_keys(&self) -> impl Iterator<Item = Result<K>> {
        let res_file = fs_utils::open_file(&self.kv_file_path, OpenOptions::new().read(true));
        let mut res_file_iter = res_file.map(|file| {
            let iter = KeyValueReader::<_, K, V>::from(file).into_iter_k();
            iter
        });

        let ret_iter_fn = move || -> Option<Result<K>> {
            match res_file_iter.as_mut() {
                Err(e) => {
                    // This error occurred during the construction of the iterator.
                    // Return the err as an iterator item.
                    Some(Err(anyhow!(e.to_string())))
                }
                Ok(file_iter) => file_iter.next(),
            }
        };
        iter::from_fn(ret_iter_fn)
    }

    pub fn remove_file(&self) -> Result<()> {
        fs_utils::remove_file(&self.kv_file_path)?;
        Ok(())
    }
}

#[derive(From, Deref, DerefMut)]
struct SparseFileOffsets<K>(Vec<(K, FileOffset)>);

impl<K> SparseFileOffsets<K> {
    fn nearest_preceding_file_offset<Q>(&self, k_lo: Option<&Q>) -> FileOffset
    where
        K: PartialOrd<Q>,
    {
        if k_lo.is_none() {
            return FileOffset(0);
        }
        let k_lo = k_lo.unwrap();

        let mem_idx_right: usize =
            bisect::bisect_right(&self.0, 0, self.0.len(), |(sample_k, _offset)| {
                sample_k.partial_cmp(k_lo).unwrap_or(Ordering::Greater)
            });

        if mem_idx_right == 0 {
            FileOffset(0)
        } else {
            let (_k, offset) = &self[mem_idx_right - 1];
            *offset
        }
    }
}
