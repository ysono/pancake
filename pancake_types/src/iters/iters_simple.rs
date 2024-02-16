use crate::{iters::KeyValueReader, serde::ReadResult, types::Deser};
use anyhow::Result;
use derive_more::From;
use std::io::{Read, Seek};

#[derive(From)]
pub struct KeyValueIterator<RS, K, V> {
    r: KeyValueReader<RS, K, V>,
}
impl<RS, K, V> Iterator for KeyValueIterator<RS, K, V>
where
    RS: Read + Seek,
    K: Deser,
    V: Deser,
{
    type Item = Result<(K, V)>;
    fn next(&mut self) -> Option<Self::Item> {
        let res_opt_kv = self.r.deser_kv().map(|read_result| match read_result {
            ReadResult::EOF => None,
            ReadResult::Some(_r_len, kv) => Some(kv),
        });
        res_opt_kv.transpose()
    }
}

#[derive(From)]
pub struct KeyIterator<RS, K, V> {
    r: KeyValueReader<RS, K, V>,
}
impl<RS, K, V> Iterator for KeyIterator<RS, K, V>
where
    RS: Read + Seek,
    K: Deser,
    V: Deser,
{
    type Item = Result<K>;
    fn next(&mut self) -> Option<Self::Item> {
        let res_opt_kv = self
            .r
            .deser_k_skip_v()
            .map(|read_result| match read_result {
                ReadResult::EOF => None,
                ReadResult::Some(_r_len, k) => Some(k),
            });
        res_opt_kv.transpose()
    }
}
