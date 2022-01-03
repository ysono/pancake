use crate::storage::serde::{DatumReader, ReadResult, Serializable};
use anyhow::{anyhow, Result};
use std::any;
use std::fs::File;
use std::io::BufReader;
use std::marker::PhantomData;

pub struct KeyValueIterator<K, V> {
    r: DatumReader<File>,
    _phant: PhantomData<(K, V)>,
}

impl<K, V> From<File> for KeyValueIterator<K, V> {
    fn from(file: File) -> Self {
        Self {
            r: DatumReader::from(BufReader::new(file)),
            _phant: PhantomData,
        }
    }
}

impl<K, V> Iterator for KeyValueIterator<K, V>
where
    K: Serializable,
    V: Serializable,
{
    type Item = Result<(K, V)>;

    fn next(&mut self) -> Option<Self::Item> {
        let key: K = match K::deser(&mut self.r) {
            Err(e) => return Some(Err(e)),
            Ok(ReadResult::EOF) => return None,
            Ok(ReadResult::Some(_r_len, k)) => k,
        };

        let val: V = match V::deser(&mut self.r) {
            Err(e) => return Some(Err(anyhow!(e))),
            Ok(ReadResult::EOF) => {
                return Some(Err(anyhow!("EOF while reading {}.", any::type_name::<V>())))
            }
            Ok(ReadResult::Some(_r_len, v)) => v,
        };

        Some(Ok((key, val)))
    }
}
