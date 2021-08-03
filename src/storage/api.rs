use derive_more::{Deref, From};
use std::cmp::{Eq, Ord, PartialEq, PartialOrd};

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Debug)]
pub enum Datum {
    Bytes(Vec<u8>),
    I64(i64),
    Str(String),
    Tuple(Vec<Datum>),
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Debug)]
pub enum OptDatum {
    Tombstone,
    Some(Datum),
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Deref, From, Debug)]
pub struct Key(pub Datum);

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Deref, Debug)]
pub struct Value(pub OptDatum);

impl From<Datum> for Value {
    fn from(dat: Datum) -> Self {
        Self(OptDatum::Some(dat))
    }
}
