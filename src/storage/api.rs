use derive_more::{Deref, From};
use std::cmp::{Eq, Ord, PartialEq, PartialOrd};

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Debug)]
pub enum Datum {
    Bytes(Vec<u8>),
    I64(i64),
    Str(String),
    Tuple(Vec<Datum>),
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Deref, From, Debug)]
pub struct Key(pub Datum);

/// Newtype for `Option<Datum>`. A `Value(None)` specifies a tombstone entry -- either writing a tombstone or reading a tombstone.
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Deref, Debug)]
pub struct Value(pub Option<Datum>);

impl From<Datum> for Value {
    fn from(dat: Datum) -> Self {
        Self(Some(dat))
    }
}
