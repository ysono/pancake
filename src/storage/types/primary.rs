use crate::storage::serde::{Datum, DatumType, Serializable};
use anyhow::Result;
use derive_more::{Deref, From};
use std::cmp::{Ordering, PartialOrd};
use std::fs::File;
use std::io::Write;
use std::sync::Arc;

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Deref, From, Debug)]
pub struct PrimaryKey(pub Datum);

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Deref, From, Debug)]
pub struct Value(pub Datum);

pub type PKShared = Arc<PrimaryKey>;
pub type PVShared = Arc<Value>;

impl PartialEq<PrimaryKey> for PKShared {
    fn eq(&self, other: &PrimaryKey) -> bool {
        (self as &PrimaryKey).eq(other)
    }
}
impl PartialOrd<PrimaryKey> for PKShared {
    fn partial_cmp(&self, other: &PrimaryKey) -> Option<Ordering> {
        (self as &PrimaryKey).partial_cmp(other)
    }
}

impl<Inner, Outer> Serializable for Outer
where
    Inner: Serializable,
    Outer: std::ops::Deref<Target = Inner> + std::convert::From<Inner>,
{
    fn ser(&self, w: &mut impl Write) -> Result<usize> {
        self.deref().ser(w)
    }

    fn deser(datum_size: usize, datum_type: DatumType, r: &mut File) -> Result<Self> {
        let inner = Inner::deser(datum_size, datum_type, r)?;
        let outer = Outer::from(inner);
        Ok(outer)
    }
}
