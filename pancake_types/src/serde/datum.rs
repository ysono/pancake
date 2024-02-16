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
        let ord = match (self, other) {
            (Self::Bytes(slf), Self::Bytes(oth)) => slf.cmp(oth),
            (Self::I64(slf), Self::I64(oth)) => slf.cmp(oth),
            (Self::Str(slf), Self::Str(oth)) => slf.cmp(oth),
            (Self::Tuple(slf), Self::Tuple(oth)) => slf.cmp(oth),
            _ => DatumType::from(self).cmp(&DatumType::from(other)),
        };
        Some(ord)
    }
}
impl Ord for Datum {
    fn cmp(&self, other: &Datum) -> Ordering {
        self.partial_cmp(other).unwrap()
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
impl<T> Into<Option<T>> for OptDatum<T> {
    fn into(self) -> Option<T> {
        match self {
            Self::Tombstone => None,
            Self::Some(t) => Some(t),
        }
    }
}
