use crate::storage::serde::Datum;
use crate::storage::types::PVShared;
use derive_more::{Deref, From};
use owning_ref::OwningRef;
use std::cmp::{Ord, PartialOrd};
use std::ops::Deref;
use std::sync::Arc;

/// A sub-portion of a [Value](crate::storage::types::Value)
#[derive(From, Deref, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct SubValue(pub Datum);

#[derive(Clone, Debug)]
pub enum SVShared {
    Own(Arc<SubValue>),
    Ref(OwningRef<PVShared, Datum>),
}

impl Deref for SVShared {
    type Target = SubValue;
    fn deref(&self) -> &SubValue {
        match self {
            Self::Own(own) => own,
            Self::Ref(ownref) => {
                let ptr = ownref as &Datum as *const Datum as *const SubValue;
                unsafe { &*ptr }
            }
        }
    }
}

impl PartialEq for SVShared {
    fn eq(&self, other: &SVShared) -> bool {
        (self as &SubValue).eq(other as &SubValue)
    }
}
impl Eq for SVShared {}
