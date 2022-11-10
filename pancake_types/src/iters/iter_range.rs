use crate::{serde::ReadResult, types::Deser};
use anyhow::{anyhow, Result};
use std::any;
use std::cmp::Ordering;
use std::fs::File;
use std::io::BufReader;
use std::marker::PhantomData;

enum State {
    NotBegun,
    InRange,
    Terminated,
}

/// An iterator that reads a file that stores serialized `K` and `V` alternately, sorted by `K`.
/// I.e. it works on SSTable files only.
///
/// The iterator deserializes every `K`; and if this `K` is out of the desired range,
/// it skips deserialization of `V`.
pub struct KeyValueRangeIterator<'a, K, V, Q> {
    r: BufReader<File>,
    k_lo: Option<&'a Q>,
    k_hi: Option<&'a Q>,
    state: State,
    _phant: PhantomData<(K, V)>,
}

impl<'a, K, V, Q> KeyValueRangeIterator<'a, K, V, Q>
where
    K: Deser + PartialOrd<Q>,
    V: Deser,
{
    pub fn new(file: File, k_lo: Option<&'a Q>, k_hi: Option<&'a Q>) -> Self {
        Self {
            r: BufReader::new(file),
            k_lo,
            k_hi,
            state: State::NotBegun,
            _phant: PhantomData,
        }
    }

    fn deser_k(&mut self) -> Result<Option<K>> {
        match K::deser(&mut self.r)? {
            ReadResult::EOF => Ok(None),
            ReadResult::Some(_, k) => Ok(Some(k)),
        }
    }
    fn deser_v(&mut self) -> Result<V> {
        match V::deser(&mut self.r)? {
            ReadResult::EOF => Err(anyhow!(
                "EOF where a {} is expected.",
                any::type_name::<V>()
            )),
            ReadResult::Some(_, v) => Ok(v),
        }
    }
    fn skip_v(&mut self) -> Result<()> {
        match V::skip(&mut self.r)? {
            ReadResult::EOF => Err(anyhow!(
                "EOF where a {} is expected.",
                any::type_name::<V>()
            )),
            ReadResult::Some(_, ()) => Ok(()),
        }
    }

    fn get_first_k(&mut self) -> Result<Option<K>> {
        loop {
            match self.deser_k()? {
                None => return Ok(None),
                Some(k) => {
                    if self.cmp_k_vs_lo(&k).is_lt() {
                        self.skip_v()?;
                    } else {
                        return Ok(Some(k));
                    }
                }
            }
        }
    }

    fn cmp_k_vs_lo(&self, k: &K) -> Ordering {
        match self.k_lo {
            None => Ordering::Greater,
            Some(k_lo) => k.partial_cmp(k_lo).unwrap_or(Ordering::Greater),
        }
    }
    fn cmp_k_vs_hi(&self, k: &K) -> Ordering {
        match self.k_hi {
            None => Ordering::Less,
            Some(k_hi) => k.partial_cmp(k_hi).unwrap_or(Ordering::Less),
        }
    }

    fn next_impl(&mut self) -> Result<Option<(K, V)>> {
        match self.state {
            State::NotBegun => match self.get_first_k()? {
                Some(k) if self.cmp_k_vs_hi(&k).is_le() => {
                    self.state = State::InRange;
                    let v = self.deser_v()?;
                    return Ok(Some((k, v)));
                }
                _ => {
                    self.state = State::Terminated;
                    return Ok(None);
                }
            },
            State::InRange => match self.deser_k()? {
                Some(k) if self.cmp_k_vs_hi(&k).is_le() => {
                    let v = self.deser_v()?;
                    return Ok(Some((k, v)));
                }
                _ => {
                    self.state = State::Terminated;
                    return Ok(None);
                }
            },
            State::Terminated => return Ok(None),
        }
    }
}
impl<'a, K, V, Q> Iterator for KeyValueRangeIterator<'a, K, V, Q>
where
    K: Deser + PartialOrd<Q>,
    V: Deser,
{
    type Item = Result<(K, V)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.next_impl().transpose()
    }
}
