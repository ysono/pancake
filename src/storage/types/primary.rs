use crate::storage::serde::{Datum, DatumWriter, OptDatum, Ser, Serializable, WriteLen};
use anyhow::Result;
use derive_more::{Deref, From};
use std::cmp::{Ordering, PartialOrd};
use std::io::Write;
use std::sync::Arc;

#[derive(From, Deref, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct PrimaryKey(pub Datum);

#[derive(From, Deref, PartialEq, Eq, Debug)]
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

impl Ser for PKShared {
    fn ser<W: Write>(&self, w: &mut DatumWriter<W>) -> Result<WriteLen> {
        w.ser_dat(self)
    }
}
impl From<Datum> for PKShared {
    fn from(dat: Datum) -> Self {
        Arc::new(PrimaryKey(dat))
    }
}
impl Serializable for PKShared {}

impl Ser for OptDatum<PVShared> {
    fn ser<W: Write>(&self, w: &mut DatumWriter<W>) -> Result<WriteLen> {
        match self {
            OptDatum::Tombstone => w.ser_optdat(&OptDatum::Tombstone),
            OptDatum::Some(pv) => w.ser_dat(pv),
        }
    }
}
impl From<Datum> for PVShared {
    fn from(dat: Datum) -> Self {
        Arc::new(Value(dat))
    }
}
impl Serializable for OptDatum<PVShared> {}
