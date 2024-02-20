use crate::serde::DatumType;
use std::cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd};

mod deser;
mod ser;
mod serde_test;
pub use deser::*;
pub use ser::*;

#[derive(PartialEq, Eq, Debug)]
pub enum Datum {
    I64(i64),
    Bytes(Vec<u8>),
    Str(String),
    Tuple(Vec<Datum>),
}
impl PartialOrd for Datum {
    fn partial_cmp(&self, other: &Datum) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Datum {
    fn cmp(&self, other: &Datum) -> Ordering {
        match (self, other) {
            (Self::Bytes(slf), Self::Bytes(oth)) => slf.cmp(oth),
            (Self::I64(slf), Self::I64(oth)) => slf.cmp(oth),
            (Self::Str(slf), Self::Str(oth)) => slf.cmp(oth),
            (Self::Tuple(slf), Self::Tuple(oth)) => slf.cmp(oth),
            _ => DatumType::from(self).cmp(&DatumType::from(other)),
        }
    }
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum OptDatum<T> {
    Tombstone,
    Some(T),
}
impl<T> From<Option<T>> for OptDatum<T> {
    fn from(opt: Option<T>) -> Self {
        match opt {
            None => OptDatum::Tombstone,
            Some(t) => OptDatum::Some(t),
        }
    }
}
impl<T> From<OptDatum<T>> for Option<T> {
    fn from(optdat: OptDatum<T>) -> Option<T> {
        match optdat {
            OptDatum::Tombstone => None,
            OptDatum::Some(t) => Some(t),
        }
    }
}
