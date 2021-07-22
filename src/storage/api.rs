use derive_more::Deref;
use std::cmp::{Eq, Ord, PartialEq, PartialOrd};

/// API Key type.
/// Newtype of String.
#[derive(PartialEq, PartialOrd, Eq, Ord, Debug, Deref, Clone)]
pub struct Key(pub String);

/// API Value type.
#[derive(Debug, Clone)]
pub enum Value {
    Tombstone,
    // Integer(i64),
    // Text(String),
    Bytes(Vec<u8>),
}
