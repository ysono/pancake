use crate::storage::serde::{Datum, DatumWriter, Ser, Serializable, WriteLen};
use crate::storage::types::{PKShared, PrimaryKey, SVShared, SubValue};
use anyhow::{anyhow, Result};
use std::borrow::Borrow;
use std::cmp::{Ord, Ordering, PartialOrd};
use std::io::Write;
use std::sync::Arc;

/// A tuple containing [`SubValue`] and [`PrimaryKey`].
/// Orderable by sub-value first, then by primary-key.
#[derive(PartialEq, Eq, Clone)]
pub struct SVPKShared {
    pub sv: SVShared,
    pub pk: PKShared,
}

impl Ser for SVPKShared {
    fn ser<W: Write>(&self, w: &mut DatumWriter<W>) -> Result<WriteLen> {
        let data = [&self.sv as &Datum, &self.pk as &Datum];
        w.ser_root_tuple(&data[..])
    }
}
impl TryFrom<Datum> for SVPKShared {
    type Error = anyhow::Error;
    fn try_from(mut dat: Datum) -> Result<Self> {
        if let Datum::Tuple(tup) = dat {
            match tup.try_into() as Result<[Datum; 2], _> {
                Ok([sv, pk]) => {
                    let sv = SVShared::Own(Arc::new(SubValue(sv)));
                    let pk = Arc::new(PrimaryKey(pk));
                    return Ok(SVPKShared { sv, pk });
                }
                Err(mbrs) => dat = Datum::Tuple(mbrs),
            }
        }
        Err(anyhow!("SVPK could not be deserialized from {:?}", dat))
    }
}
impl Serializable for SVPKShared {}

impl Borrow<PKShared> for SVPKShared {
    fn borrow(&self) -> &PKShared {
        &self.pk
    }
}
impl Into<PKShared> for SVPKShared {
    fn into(self) -> PKShared {
        self.pk
    }
}

impl PartialOrd for SVPKShared {
    fn partial_cmp(&self, other: &SVPKShared) -> Option<Ordering> {
        let ord = self.sv.cmp(&other.sv).then_with(|| self.pk.cmp(&other.pk));
        Some(ord)
    }
}
impl Ord for SVPKShared {
    fn cmp(&self, other: &SVPKShared) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl PartialEq<SubValue> for SVPKShared {
    fn eq(&self, other: &SubValue) -> bool {
        (&self.sv as &SubValue).eq(other)
    }
}
impl PartialOrd<SubValue> for SVPKShared {
    /// In case `self SV == param SV`, the partial ordering is undefined and depends on the context:
    /// - When seeking the lower bound, self is greater than param.
    /// - When seeking the upper bound, self is greater than param.
    /// Caller must call `.unwrap_or(Ordering::Greater)` or `.unwrap_or(Ordering::Less)`, accordingly.
    fn partial_cmp(&self, other: &SubValue) -> Option<Ordering> {
        match (&self.sv as &SubValue).cmp(other) {
            Ordering::Equal => None,
            ord => Some(ord),
        }
    }
}
