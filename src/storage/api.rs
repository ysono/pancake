use std::cmp::{Eq, Ord, PartialEq, PartialOrd};

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Debug)]
pub enum Datum {
    Bytes(Vec<u8>),
    I64(i64),
    Str(String),
    Tuple(Vec<Datum>),
}
