use crate::{serde::Datum, types::Serializable};
use derive_more::{Deref, From};
use std::borrow::Borrow;
use std::cmp::{Ordering, PartialOrd};
use std::sync::Arc;

#[derive(From, Deref, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct PrimaryKey(pub Datum);

pub type PKShared = Arc<PrimaryKey>;

/* PKShared is comparable against PrimaryKey. */
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

/* PKShared is comparable against &PrimaryKey. */
impl PartialEq<&PrimaryKey> for PKShared {
    fn eq(&self, other: &&PrimaryKey) -> bool {
        (self as &PrimaryKey).eq(other)
    }
}
impl PartialOrd<&PrimaryKey> for PKShared {
    fn partial_cmp(&self, other: &&PrimaryKey) -> Option<Ordering> {
        (self as &PrimaryKey).partial_cmp(other)
    }
}

/* PKShared is Serializable. */
impl Borrow<Datum> for PKShared {
    fn borrow(&self) -> &Datum {
        self
    }
}
impl From<Datum> for PKShared {
    fn from(dat: Datum) -> Self {
        Arc::new(PrimaryKey(dat))
    }
}
impl Serializable for PKShared {}
