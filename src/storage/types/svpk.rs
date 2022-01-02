use crate::storage::serde::{serialize_ref_datums, Datum, DatumType, Serializable};
use crate::storage::types::{PKShared, PrimaryKey, SVShared, SubValue};
use anyhow::{anyhow, Result};
use std::cmp::{Ord, Ordering, PartialOrd};
use std::fs::File;
use std::io::Write;
use std::sync::Arc;

/// SubValue and PrimaryKey.
#[derive(PartialEq, Eq, Hash, Clone)]
pub struct SVPKShared {
    pub sv: SVShared,
    pub pk: PKShared,
}

impl SVPKShared {
    fn from_datum(mut dat: Datum) -> Result<Self> {
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

impl Serializable for SVPKShared {
    fn ser(&self, w: &mut impl Write) -> Result<usize> {
        let tup = vec![&self.sv as &Datum, &self.pk as &Datum];
        serialize_ref_datums(tup, w)
    }

    fn deser(datum_size: usize, datum_type: DatumType, r: &mut File) -> Result<Self> {
        let dat = Datum::deser(datum_size, datum_type, r)?;
        Self::from_datum(dat)
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
