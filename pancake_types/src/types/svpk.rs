use crate::serde::{Datum, OptDatum, ReadResult, WriteLen};
use crate::types::{Deser, PKShared, PrimaryKey, SVShared, Ser, Serializable, SubValue};
use anyhow::{anyhow, Result};
use std::borrow::Borrow;
use std::cmp::{Ord, Ordering, PartialOrd};
use std::io::{Read, Seek, Write};
use std::sync::Arc;

/// A tuple containing a sub-value and a primary-key.
#[derive(PartialEq, Eq, Clone)]
pub struct SVPKShared {
    pub sv: SVShared,
    pub pk: PKShared,
}

/* SVPKShared is Serializable. */
impl Ser for SVPKShared {
    fn ser<W: Write>(&self, w: &mut W) -> Result<WriteLen> {
        let mut w_len = 0;
        w_len += *((&self.sv as &Datum).ser(w)?);
        w_len += *((&self.pk as &Datum).ser(w)?);
        Ok(WriteLen::new_manual(w_len))
    }
}
impl Deser for SVPKShared {
    fn skip<R: Read + Seek>(r: &mut R) -> Result<ReadResult<()>> {
        let sv_r_len = match OptDatum::<Datum>::skip(r)? {
            ReadResult::EOF => return Ok(ReadResult::EOF),
            ReadResult::Some(r_len, ()) => r_len,
        };
        let pk_r_len = match OptDatum::<Datum>::skip(r)? {
            ReadResult::EOF => return Err(anyhow!("SV found but PK not found.")),
            ReadResult::Some(r_len, ()) => r_len,
        };

        let r_len = sv_r_len + pk_r_len;
        return Ok(ReadResult::Some(r_len, ()));
    }
    fn deser<R: Read + Seek>(r: &mut R) -> Result<ReadResult<Self>> {
        let (sv_r_len, sv_dat) = match Datum::deser(r)? {
            ReadResult::EOF => return Ok(ReadResult::EOF),
            ReadResult::Some(r_len, dat) => (r_len, dat),
        };
        let (pk_r_len, pk_dat) = match Datum::deser(r)? {
            ReadResult::EOF => return Err(anyhow!("SV found but PK not found.")),
            ReadResult::Some(r_len, dat) => (r_len, dat),
        };

        let r_len = sv_r_len + pk_r_len;
        let svpk = SVPKShared {
            sv: SVShared::Own(Arc::new(SubValue(sv_dat))),
            pk: Arc::new(PrimaryKey(pk_dat)),
        };
        return Ok(ReadResult::Some(r_len, svpk));
    }
}
impl Serializable for SVPKShared {}

/* SVPKShared can be converted (= Borrow + Into) into PKShared. */
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

/* SVPKShared is comparable against the same type. */
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

/* SVPKShared is comparable against {SubValue, &SubValue, etc}. */
impl<O> PartialEq<O> for SVPKShared
where
    O: Borrow<SubValue>,
{
    fn eq(&self, other: &O) -> bool {
        (&self.sv as &SubValue).eq(other.borrow())
    }
}
impl<O> PartialOrd<O> for SVPKShared
where
    O: Borrow<SubValue>,
{
    /// Orders by sub-value, and *not* by primary-key.
    /// In case `self's sub-value == other's sub-value`, the absolute ordering is undefined and depends on the context. Eg:
    /// - "x vs lower bound y" := `x.partial_cmp(y).unwrap_or(Ordering::Greater)`
    /// - "x vs upper bound y" := `x.partial_cmp(y).unwrap_or(Ordering::Less)`
    fn partial_cmp(&self, other: &O) -> Option<Ordering> {
        match (&self.sv as &SubValue).cmp(other.borrow()) {
            Ordering::Equal => None,
            ord => Some(ord),
        }
    }
}
