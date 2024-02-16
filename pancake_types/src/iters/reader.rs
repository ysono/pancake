use crate::{
    iters::{KeyIterator, KeyValueIterator, KeyValueRangeIterator},
    serde::ReadResult,
    types::Deser,
};
use anyhow::{anyhow, Result};
use std::any;
use std::io::BufReader;
use std::io::{Read, Seek};
use std::marker::PhantomData;

pub struct KeyValueReader<R, K, V> {
    r: BufReader<R>,
    _phant: PhantomData<(K, V)>,
}
impl<R, K, V> From<R> for KeyValueReader<R, K, V>
where
    R: Read,
{
    fn from(r: R) -> Self {
        Self {
            r: BufReader::new(r),
            _phant: PhantomData,
        }
    }
}
impl<RS, K, V> KeyValueReader<RS, K, V>
where
    RS: Read + Seek,
    K: Deser,
    V: Deser,
{
    pub fn deser_k(&mut self) -> Result<ReadResult<K>> {
        K::deser(&mut self.r)
    }
    pub fn skip_k(&mut self) -> Result<ReadResult<()>> {
        K::skip(&mut self.r)
    }
    pub fn deser_v(&mut self) -> Result<(usize, V)> {
        match V::deser(&mut self.r)? {
            ReadResult::EOF => Err(anyhow!(
                "EOF where a `V` typed {} is expected.",
                any::type_name::<V>()
            )),
            ReadResult::Some(r_len, v) => Ok((r_len, v)),
        }
    }
    pub fn skip_v(&mut self) -> Result<usize> {
        match V::skip(&mut self.r)? {
            ReadResult::EOF => Err(anyhow!(
                "EOF where a `V` typed {} is expected.",
                any::type_name::<V>()
            )),
            ReadResult::Some(r_len, ()) => Ok(r_len),
        }
    }

    pub fn deser_kv(&mut self) -> Result<ReadResult<(K, V)>> {
        let (mut r_len, k) = match self.deser_k()? {
            ReadResult::EOF => return Ok(ReadResult::EOF),
            ReadResult::Some(r_len, k) => (r_len, k),
        };

        let (delta_r_len, v) = self.deser_v()?;
        r_len += delta_r_len;

        Ok(ReadResult::Some(r_len, (k, v)))
    }
    pub fn deser_k_skip_v(&mut self) -> Result<ReadResult<K>> {
        let (mut r_len, k) = match self.deser_k()? {
            ReadResult::EOF => return Ok(ReadResult::EOF),
            ReadResult::Some(r_len, k) => (r_len, k),
        };

        let delta_r_len = self.skip_v()?;
        r_len += delta_r_len;

        Ok(ReadResult::Some(r_len, k))
    }
    pub fn skip_kv(&mut self) -> Result<ReadResult<()>> {
        let mut r_len = match self.skip_k()? {
            ReadResult::EOF => return Ok(ReadResult::EOF),
            ReadResult::Some(r_len, ()) => r_len,
        };

        let delta_r_len = self.skip_v()?;
        r_len += delta_r_len;

        Ok(ReadResult::Some(r_len, ()))
    }

    pub fn into_iter_kv(self) -> KeyValueIterator<RS, K, V> {
        KeyValueIterator::from(self)
    }
    pub fn into_iter_k(self) -> KeyIterator<RS, K, V> {
        KeyIterator::from(self)
    }
    pub fn into_iter_kv_range<'q, Q>(
        self,
        q_lo: Option<&'q Q>,
        q_hi: Option<&'q Q>,
    ) -> KeyValueRangeIterator<'q, RS, K, V, Q>
    where
        K: PartialOrd<Q>,
    {
        KeyValueRangeIterator::new(self, q_lo, q_hi)
    }
}
