use crate::ds_n_a::bisect;
use crate::storage::lsm::Entry;
use crate::storage::serde::{
    DatumReader, DatumWriter, KeyValueRangeIterator, OptDatum, ReadResult, Ser, Serializable,
};
use anyhow::{anyhow, Result};
use derive_more::{Deref, DerefMut, From};
use std::any;
use std::cmp::{Ord, Ordering, PartialOrd};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};
use std::iter;
use std::marker::PhantomData;
use std::path::PathBuf;

#[derive(Clone, Copy)]
struct FileOffset(u64);

static SSTABLE_IDX_SPARSENESS: usize = 3;

fn is_kv_sparsely_captured(entry_i: usize) -> bool {
    entry_i % SSTABLE_IDX_SPARSENESS == SSTABLE_IDX_SPARSENESS - 1
}

/// An SSTable is an abstraction of a sorted dictionary.
/// An SSTable has these components:
/// - A file which stores `(key, val_or_tombstone)` pairs, sorted by key, containing distinct keys.
/// - An in-memory sorted structure that maps `{key: file_offset}` on sparsely captured keys. The offsets point to locations within the above file.
pub struct SSTable<K, V> {
    path: PathBuf,
    sparse_file_offsets: SparseFileOffsets<K>,
    _phant: PhantomData<V>,
}

impl<K, V> SSTable<K, V>
where
    K: Serializable + Ord + Clone,
    OptDatum<V>: Serializable,
{
    pub fn new<'a>(
        entries: impl Iterator<Item = Entry<'a, K, OptDatum<V>>>,
        path: PathBuf,
    ) -> Result<Self>
    where
        K: 'a,
        V: 'a,
    {
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)?;
        let mut datum_writer = DatumWriter::from(BufWriter::new(file));

        let mut sparse_file_offsets = SparseFileOffsets::from(vec![]);
        let mut file_offset = FileOffset(0);

        for (entry_i, entry) in entries.enumerate() {
            let (k_ref, v_ref) = entry.borrow_res()?;

            let mut delta_offset = 0;
            delta_offset += *(k_ref.ser(&mut datum_writer)?);
            delta_offset += *(v_ref.ser(&mut datum_writer)?);

            if is_kv_sparsely_captured(entry_i) {
                let k_own = entry.take_k()?;
                sparse_file_offsets.push((k_own, file_offset));
            }

            file_offset.0 += delta_offset as u64;
        }

        datum_writer.flush()?;

        Ok(Self {
            path,
            sparse_file_offsets,
            _phant: PhantomData,
        })
    }

    pub fn load(path: PathBuf) -> Result<Self> {
        let file = File::open(&path)?;
        let mut datum_reader = DatumReader::from(BufReader::new(file));

        let mut sparse_file_offsets = SparseFileOffsets::from(vec![]);
        let mut file_offset = FileOffset(0);

        for entry_i in 0usize.. {
            // Key
            if is_kv_sparsely_captured(entry_i) {
                match K::deser(&mut datum_reader)? {
                    ReadResult::EOF => break,
                    ReadResult::Some(delta_r_len, k) => {
                        sparse_file_offsets.push((k, file_offset));
                        file_offset.0 += delta_r_len as u64;
                    }
                }
            } else {
                match datum_reader.skip()? {
                    ReadResult::EOF => break,
                    ReadResult::Some(delta_r_len, ()) => {
                        file_offset.0 += delta_r_len as u64;
                    }
                }
            }

            // Value
            match datum_reader.skip()? {
                ReadResult::EOF => {
                    return Err(anyhow!(
                        "EOF while skipping a {} from {:?}.",
                        any::type_name::<V>(),
                        path
                    ));
                }
                ReadResult::Some(delta_r_len, ()) => {
                    file_offset.0 += delta_r_len as u64;
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
            .map(|file| KeyValueRangeIterator::new(file, k_lo, k_hi));

        let ret_iter_fn = move || -> Option<Result<(K, OptDatum<V>)>> {
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
        fs::remove_file(&self.path)?;
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
            let (_k, offset) = &self.0[mem_idx_right - 1];
            *offset
        }
    }
}
