use crate::storage::serde::{DatumType, Serializable};
use crate::storage::types::Datum;
use anyhow::Result;
use derive_more::{Deref, From};
use std::fs::File;
use std::io::Write;

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Deref, From, Clone, Debug)]
pub struct PrimaryKey(pub Datum);

#[derive(PartialEq, Eq, PartialOrd, Ord, Deref, From, Clone, Debug)]
pub struct Value(pub Datum);

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Deref, From, Clone, Debug)]
pub struct SubValue(pub Datum);

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
