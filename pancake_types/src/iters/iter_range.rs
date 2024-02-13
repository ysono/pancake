use crate::{iters::KeyValueReader, serde::ReadResult, types::Deser};
use anyhow::Result;
use std::cmp::Ordering;
use std::io::{Read, Seek};

/// An iterator that reads a file that stores serialized `K` and `V` alternately, sorted by `K`.
/// I.e. it works on SSTable files only, not write-ahead log files.
///
/// The iterator deserializes every `K`; and if this `K` is out of the desired range,
/// it skips deserialization of `V`.
pub struct KeyValueRangeIterator<'q, RS, K, V, Q> {
    r: KeyValueReader<RS, K, V>,
    q_lo: Option<&'q Q>,
    q_hi: Option<&'q Q>,
    state: State,
}
impl<'q, RS, K, V, Q> KeyValueRangeIterator<'q, RS, K, V, Q>
where
    RS: Read + Seek,
    K: Deser + PartialOrd<Q>,
    V: Deser,
{
    pub fn new(r: KeyValueReader<RS, K, V>, q_lo: Option<&'q Q>, q_hi: Option<&'q Q>) -> Self {
        Self {
            r,
            q_lo,
            q_hi,
            state: State::NotBegun,
        }
    }

    fn cmp_k_vs_q_lo(&self, k: &K) -> Ordering {
        match self.q_lo {
            None => Ordering::Greater,
            Some(q_lo) => k.partial_cmp(q_lo).unwrap_or(Ordering::Greater),
        }
    }
    fn cmp_k_vs_q_hi(&self, k: &K) -> Ordering {
        match self.q_hi {
            None => Ordering::Less,
            Some(q_hi) => k.partial_cmp(q_hi).unwrap_or(Ordering::Less),
        }
    }

    fn get_first_k_gte_q_lo(&mut self) -> Result<Option<K>> {
        loop {
            match self.r.deser_k()? {
                ReadResult::EOF => return Ok(None),
                ReadResult::Some(_, k) => {
                    if self.cmp_k_vs_q_lo(&k).is_lt() {
                        self.r.skip_v()?;
                    } else {
                        return Ok(Some(k));
                    }
                }
            }
        }
    }

    fn get_next_kv(&mut self) -> Result<Option<(K, V)>> {
        match self.state {
            State::NotBegun => match self.get_first_k_gte_q_lo()? {
                Some(k) if self.cmp_k_vs_q_hi(&k).is_le() => {
                    self.state = State::InRange;
                    let (_, v) = self.r.deser_v()?;
                    return Ok(Some((k, v)));
                }
                _ => {
                    self.state = State::Terminated;
                    return Ok(None);
                }
            },
            State::InRange => match self.r.deser_k()? {
                ReadResult::Some(_, k) if self.cmp_k_vs_q_hi(&k).is_le() => {
                    let (_, v) = self.r.deser_v()?;
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
impl<'q, RS, K, V, Q> Iterator for KeyValueRangeIterator<'q, RS, K, V, Q>
where
    RS: Read + Seek,
    K: Deser + PartialOrd<Q>,
    V: Deser,
{
    type Item = Result<(K, V)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.get_next_kv().transpose()
    }
}

enum State {
    NotBegun,
    InRange,
    Terminated,
}
