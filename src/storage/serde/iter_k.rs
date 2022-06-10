use crate::storage::serde::{DatumReader, Deser, ReadResult};
use anyhow::{anyhow, Result};
use std::fs::File;
use std::io::BufReader;
use std::marker::PhantomData;

/// Given a file that encodes `K` and `V` alternately,
/// this iterator always skips `V`, and optionally deserializes `K`.
pub struct KeyIterator<K> {
    r: DatumReader<File>,
    _phant: PhantomData<K>,
}

impl<K> KeyIterator<K>
where
    K: Deser,
{
    pub fn new(file: File) -> Self {
        Self {
            r: DatumReader::from(BufReader::new(file)),
            _phant: PhantomData,
        }
    }

    pub fn read_k_skip_v(&mut self) -> Result<ReadResult<K>> {
        let mut read_len;
        let k;
        match K::deser(&mut self.r)? {
            ReadResult::EOF => return Ok(ReadResult::EOF),
            ReadResult::Some(read_len_, k_) => {
                read_len = read_len_;
                k = k_;
            }
        }
        match self.r.skip()? {
            ReadResult::EOF => return Err(anyhow!("EOF while skipping a V.",)),
            ReadResult::Some(read_len_, ()) => {
                read_len += read_len_;
            }
        }
        Ok(ReadResult::Some(read_len, k))
    }

    pub fn skip_kv(&mut self) -> Result<ReadResult<()>> {
        let mut read_len;
        match self.r.skip()? {
            ReadResult::EOF => return Ok(ReadResult::EOF),
            ReadResult::Some(read_len_, ()) => {
                read_len = read_len_;
            }
        }
        match self.r.skip()? {
            ReadResult::EOF => return Err(anyhow!("EOF while skipping a V.",)),
            ReadResult::Some(read_len_, ()) => {
                read_len += read_len_;
            }
        }
        Ok(ReadResult::Some(read_len, ()))
    }
}

impl<K> Iterator for KeyIterator<K>
where
    K: Deser,
{
    type Item = Result<(usize, K)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_k_skip_v() {
            Err(e) => Some(Err(e)),
            Ok(ReadResult::EOF) => None,
            Ok(ReadResult::Some(read_len, k)) => Some(Ok((read_len, k))),
        }
    }
}
