use std::cmp::{Eq, Ord, PartialEq, PartialOrd};

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Datum {
    I64(i64),
    Bytes(Vec<u8>),
    Str(String),
    Tuple(Vec<Datum>),
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
