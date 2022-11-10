use crate::serde::{Datum, OptDatum};
use anyhow::{anyhow, Result};
use derive_more::{Deref, From};
use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::{FromPrimitive, ToPrimitive};
use std::any;
use std::io::{self, Read};
use std::mem;

#[derive(From, Deref, Clone, Copy)]
pub struct DatumTypeInt(u8);
impl From<DatumType> for DatumTypeInt {
    fn from(dat_type: DatumType) -> Self {
        let int = dat_type.to_u8().unwrap();
        Self(int)
    }
}
impl DatumTypeInt {
    pub fn deser(r: &mut impl Read) -> Result<(usize, Self), io::Error> {
        let mut buf = [0u8; mem::size_of::<u8>()];
        r.read_exact(&mut buf)?;
        let int = u8::from_le_bytes(buf);
        Ok((buf.len(), Self(int)))
    }
}

/// We manually map enum members to data_type integers because:
/// - Rust does not support specifying discriminants on an enum containing non-simple members. [RFC](https://github.com/rust-lang/rust/issues/60553)
/// - One member, Tombstone, is outside the Datum enum.
/// - An automatic discriminant may change w/ enum definition change or compilation, according to [`std::mem::discriminant()`] doc.
#[repr(u8)]
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy, FromPrimitive, ToPrimitive, Debug)]
pub enum DatumType {
    Tombstone = 0,
    I64 = 1,
    Bytes = 2,
    Str = 3,
    Tuple = 4,
}
impl TryFrom<DatumTypeInt> for DatumType {
    type Error = anyhow::Error;
    fn try_from(int: DatumTypeInt) -> Result<Self> {
        DatumType::from_u8(int.0).ok_or(anyhow!(
            "Unknown {} {}",
            any::type_name::<DatumTypeInt>(),
            int.0
        ))
    }
}
impl From<&Datum> for DatumType {
    fn from(dat: &Datum) -> Self {
        match dat {
            Datum::I64(_) => DatumType::I64,
            Datum::Bytes(_) => DatumType::Bytes,
            Datum::Str(_) => DatumType::Str,
            Datum::Tuple(_) => DatumType::Tuple,
        }
    }
}
impl From<&OptDatum<Datum>> for DatumType {
    fn from(optdat: &OptDatum<Datum>) -> Self {
        match optdat {
            OptDatum::Tombstone => DatumType::Tombstone,
            OptDatum::Some(dat) => Self::from(dat),
        }
    }
}
