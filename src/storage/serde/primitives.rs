//! This mod contains types that are directly serializable and deserializable.
//! All other types are derivatives of these primitive types.

use crate::storage::serde::{self, ReadItem, Serializable};
use anyhow::{anyhow, Result};
use num_derive::{FromPrimitive, ToPrimitive};
use std::cmp::{Eq, Ord, PartialEq, PartialOrd};
use std::fs::File;
use std::io::{Read, Write};
use std::mem;

// TODO move somewhere else
pub fn serialize_ref_datums<'a>(vec: Vec<&'a Datum>, w: &mut impl Write) -> Result<usize> {
    let mut sz = 0;

    let dtype = DatumType::Tuple;
    let tup_len_bytes = vec.len().to_le_bytes();
    sz += serde::write_item(dtype, &tup_len_bytes, w)?;

    for dat in vec.iter() {
        sz += dat.ser(w)?;
    }

    Ok(sz)
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Datum {
    Bytes(Vec<u8>),
    I64(i64),
    Str(String),
    Tuple(Vec<Datum>),
}

impl Datum {
    pub fn to_type(&self) -> DatumType {
        match self {
            Datum::Bytes(_) => DatumType::Bytes,
            Datum::I64(_) => DatumType::I64,
            Datum::Str(_) => DatumType::Str,
            Datum::Tuple(_) => DatumType::Tuple,
        }
    }
    // TODO below, in Datum and OptDatum, use this to_type().
}

impl Serializable for Datum {
    fn ser(&self, w: &mut impl Write) -> Result<usize> {
        let write_size: usize = match self {
            Datum::Bytes(b) => serde::write_item(DatumType::Bytes, b, w)?,
            Datum::I64(i) => serde::write_item(DatumType::I64, &i.to_le_bytes(), w)?,
            Datum::Str(s) => serde::write_item(DatumType::Str, s.as_bytes(), w)?,
            Datum::Tuple(vec) => {
                let mut b: Vec<u8> = vec![];

                b.write(&vec.len().to_le_bytes())?;

                for dat in vec.iter() {
                    dat.ser(&mut b)?;
                }

                serde::write_item(DatumType::Tuple, &b, w)?
            }
        };
        Ok(write_size)
    }

    fn deser(datum_size: usize, datum_type: DatumType, r: &mut File) -> Result<Self> {
        let obj: Self = match datum_type {
            DatumType::Bytes => {
                let mut buf = vec![0u8; datum_size];
                r.read_exact(&mut buf)?;
                Datum::Bytes(buf)
            }
            DatumType::I64 => {
                let mut buf = [0u8; mem::size_of::<i64>()];
                r.read_exact(&mut buf)?;
                Datum::I64(i64::from_le_bytes(buf))
            }
            DatumType::Str => {
                let mut buf = vec![0u8; datum_size];
                r.read_exact(&mut buf)?;
                Datum::Str(String::from_utf8(buf)?)
            }
            DatumType::Tuple => {
                let mut tup_len_buf = [0u8; mem::size_of::<usize>()];
                r.read_exact(&mut tup_len_buf)?;
                let tup_len = usize::from_le_bytes(tup_len_buf);

                let mut members = Vec::<Datum>::with_capacity(tup_len);

                for _ in 0..tup_len {
                    match serde::read_item(r)? {
                        ReadItem::EOF => {
                            return Err(anyhow!("Unexpected EOF while reading a tuple."))
                        }
                        ReadItem::Some { read_size: _, obj } => {
                            members.push(obj);
                        }
                    }
                }

                Datum::Tuple(members)
            }
            _ => return Err(anyhow!("Unexpected datum_type {:?}", datum_type)),
        };
        Ok(obj)
    }
}

#[derive(Clone, Debug)]
pub enum OptDatum<T> {
    Tombstone,
    Some(T),
}
impl<T: Serializable> Serializable for OptDatum<T> {
    fn ser(&self, w: &mut impl Write) -> Result<usize> {
        match self {
            OptDatum::Tombstone => serde::write_item(DatumType::Tombstone, &[0u8; 0], w),
            OptDatum::Some(dat) => dat.ser(w),
        }
    }

    fn deser(datum_size: usize, datum_type: DatumType, r: &mut File) -> Result<Self> {
        let obj: Self = match datum_type {
            DatumType::Tombstone => OptDatum::Tombstone,
            _ => {
                let dat = T::deser(datum_size, datum_type, r)?;
                OptDatum::Some(dat)
            }
        };
        Ok(obj)
    }
}
impl<T> From<Option<T>> for OptDatum<T> {
    fn from(opt: Option<T>) -> Self {
        match opt {
            None => OptDatum::Tombstone,
            Some(t) => OptDatum::Some(t),
        }
    }
}
impl<T> Into<Option<T>> for OptDatum<T> {
    fn into(self) -> Option<T> {
        match self {
            Self::Tombstone => None,
            Self::Some(t) => Some(t),
        }
    }
}

/// We manually map enum members to data_type integers because:
/// - Rust does not support specifying discriminants on an enum containing non-simple members. [RFC](https://github.com/rust-lang/rust/issues/60553)
/// - One member, Tombstone, is outside the Datum enum.
/// - An automatic discriminant may change w/ enum definition change or compilation, according to [`std::mem::discriminant()`] doc.
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, FromPrimitive, ToPrimitive, Debug)]
pub enum DatumType {
    Tombstone = 0,
    Bytes = 1,
    I64 = 2,
    Str = 3,
    Tuple = 4,
}
