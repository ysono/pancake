use crate::{
    serde::{Datum, OptDatum},
    types::Serializable,
};
use derive_more::{Deref, From};
use std::borrow::Borrow;
use std::sync::Arc;

#[derive(From, Deref, PartialEq, Eq, Debug)]
pub struct Value(pub Datum);

pub type PVShared = Arc<Value>;

/* PVShared and OptDatum<PVShared> are Serializable. */
impl Borrow<Datum> for PVShared {
    fn borrow(&self) -> &Datum {
        self
    }
}
impl From<Datum> for PVShared {
    fn from(dat: Datum) -> Self {
        Arc::new(Value(dat))
    }
}
impl Serializable for OptDatum<PVShared> {}
