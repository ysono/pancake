//! # Serialization format
//!
//! Following pseudocode depicts the byte representation on disk. The actual struct definitions do not exit. (TODO consider defining them in tests.)
//!
//! This file format is applicable for both the commit log and ss tables.
//!
//! ```text
//! struct Item {
//!     // Encodes the on-disk byte size of `datum_type` and `datum`.
//!     // Span size is placed first, so that a reader that is uninterested
//!     // in deserializing datum can skip over it.
//!     span_size: usize,
//!
//!     // Identifies how `datum` should be deserialized.
//!     // The choice of u8 is arbitrary. In case we need to deprecate
//!     // supported datum_types over time, this allows us
//!     // (pow(2, 8) - count_of_active_datum_types) deprecations, before
//!     // rolling over to zero.
//!     datum_type: u8,
//!
//!     // Datum may be empty. Examples: tombstone, empty string, empty
//!     // map, custom serialization.
//!     datum: [u8; variable_length],
//! }
//!
//! struct File {
//!     k0: Item,
//!     v0: Item,
//!     k1: Item,
//!     v1: Item,
//!     ...
//!     // There are no separators in between Items and nothing to indicate
//!     // whether an Item is a key or a value.
//! }
//! ```
//!
//! A tuple is a type of datum that can nest other data, including other tuple-typed data.
//! For a tuple, the `datum` byte sequence encodes the following structure:
//!
//! ```text
//! struct TupleDatum {
//!     length_of_tuple: usize,
//!
//!     member_0: Item,
//!     member_1: Item,
//!     ...
//!     member_n-1: Item,
//! }
//! ```

use super::api::{Datum, OptDatum};
use anyhow::{anyhow, Result};
use derive_more::From;
use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::FromPrimitive;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::size_of;

/*
We manually map enum members to data_type integers because:
- Rust does not support specifying discriminants on an enum containing non-simple members. https://github.com/rust-lang/rust/issues/60553
- One member, Tombstone, is outside the Datum enum.
- An automatic discriminant may change w/ enum definition change or compilation, according to [`std::mem::discriminant()`] doc.
*/
#[derive(PartialEq, Eq, PartialOrd, Ord, FromPrimitive, ToPrimitive, Debug)]
pub enum DatumType {
    Tombstone = 0,
    Bytes = 1,
    I64 = 2,
    Str = 3,
    Tuple = 4,
}

type DatumTypeInt = u8;

pub trait Serializable: Sized {
    fn ser(&self, w: &mut impl Write) -> Result<usize>;
    fn deser(datum_size: usize, datum_type: DatumType, r: &mut File) -> Result<Self>;
}

impl Serializable for Datum {
    fn ser(&self, w: &mut impl Write) -> Result<usize> {
        let write_size: usize = match self {
            Datum::Bytes(b) => write_item(DatumType::Bytes, b, w)?,
            Datum::I64(i) => write_item(DatumType::I64, &i.to_le_bytes(), w)?,
            Datum::Str(s) => write_item(DatumType::Str, s.as_bytes(), w)?,
            Datum::Tuple(vec) => {
                let mut b: Vec<u8> = vec![];

                b.write(&vec.len().to_le_bytes())?;

                for dat in vec.iter() {
                    dat.ser(&mut b)?;
                }

                write_item(DatumType::Tuple, &b, w)?
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
                let mut buf = [0u8; size_of::<i64>()];
                r.read_exact(&mut buf)?;
                Datum::I64(i64::from_le_bytes(buf))
            }
            DatumType::Str => {
                let mut buf = vec![0u8; datum_size];
                r.read_exact(&mut buf)?;
                Datum::Str(String::from_utf8(buf)?)
            }
            DatumType::Tuple => {
                let mut tup_len_buf = [0u8; size_of::<usize>()];
                r.read_exact(&mut tup_len_buf)?;
                let tup_len = usize::from_le_bytes(tup_len_buf);

                let mut members = Vec::<Datum>::with_capacity(tup_len);

                for _ in 0..tup_len {
                    match read_item(r)? {
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

impl Serializable for OptDatum {
    fn ser(&self, w: &mut impl Write) -> Result<usize> {
        match self {
            OptDatum::Tombstone => write_item(DatumType::Tombstone, &[0u8; 0], w),
            OptDatum::Some(dat) => dat.ser(w),
        }
    }

    fn deser(datum_size: usize, datum_type: DatumType, r: &mut File) -> Result<Self> {
        let obj: Self = match datum_type {
            DatumType::Tombstone => OptDatum::Tombstone,
            _ => {
                let dat = Datum::deser(datum_size, datum_type, r)?;
                OptDatum::Some(dat)
            }
        };
        Ok(obj)
    }
}

/// @return Total count of bytes that are written to file.
fn write_item(datum_type: DatumType, datum_bytes: &[u8], w: &mut impl Write) -> Result<usize> {
    let mut span_size = 0usize;
    span_size += size_of::<DatumTypeInt>();
    span_size += datum_bytes.len();

    let mut write_size = 0usize;
    write_size += w.write(&span_size.to_le_bytes())?;
    write_size += w.write(&(datum_type as DatumTypeInt).to_le_bytes())?;
    write_size += w.write(datum_bytes)?;

    Ok(write_size)
}

pub fn serialize_kv<K: Serializable, V: Serializable>(
    k: &K,
    v: &V,
    w: &mut impl Write,
) -> Result<usize> {
    let mut write_size = 0usize;
    write_size += k.ser(w)?;
    write_size += v.ser(w)?;
    Ok(write_size)
}

enum ReadSpanSize {
    EOF,
    Some { read_size: usize, span_size: usize },
}

fn read_span_size(file: &mut File) -> Result<ReadSpanSize> {
    let mut span_size_buf = [0u8; size_of::<usize>()];
    let read_size = file.read(&mut span_size_buf)?;
    if read_size == 0 {
        return Ok(ReadSpanSize::EOF);
    } else if read_size != span_size_buf.len() {
        return Err(anyhow!("Unexpected EOF while reading a span_size."));
    }

    let span_size = usize::from_le_bytes(span_size_buf);

    Ok(ReadSpanSize::Some {
        read_size,
        span_size,
    })
}

pub enum SkipItem {
    EOF,
    Some { read_size: usize },
}

pub fn skip_item(file: &mut File) -> Result<SkipItem> {
    let ret = match read_span_size(file)? {
        ReadSpanSize::EOF => SkipItem::EOF,
        ReadSpanSize::Some {
            mut read_size,
            span_size,
        } => {
            file.seek(SeekFrom::Current(span_size as i64))?;
            read_size += span_size;
            SkipItem::Some { read_size }
        }
    };
    Ok(ret)
}

pub enum ReadItem<T> {
    EOF,
    Some { read_size: usize, obj: T },
}

pub fn read_item<T: Serializable>(file: &mut File) -> Result<ReadItem<T>> {
    let ret = match read_span_size(file)? {
        ReadSpanSize::EOF => ReadItem::EOF,
        ReadSpanSize::Some {
            mut read_size,
            span_size,
        } => {
            let mut dtype_buf = [0u8; size_of::<DatumTypeInt>()];
            file.read_exact(&mut dtype_buf)?;
            let dtype_int = DatumTypeInt::from_le_bytes(dtype_buf);

            let dtype =
                DatumType::from_u8(dtype_int).ok_or(anyhow!("Unknown datum type {}", dtype_int))?;

            let datum_size = span_size - dtype_buf.len();

            let obj = T::deser(datum_size, dtype, file)?;

            read_size += span_size;

            ReadItem::Some { read_size, obj }
        }
    };
    Ok(ret)
}

#[derive(From)]
pub struct KeyValueIterator {
    file: File,
}

impl Iterator for KeyValueIterator {
    type Item = Result<(Datum, OptDatum)>;

    fn next(&mut self) -> Option<Self::Item> {
        // TODO(btc): if read_kv returns an error, perhaps it should continue to return errors for all subsequent calls?
        let key: Datum = match read_item::<Datum>(&mut self.file) {
            Err(e) => return Some(Err(anyhow!(e))),
            Ok(ReadItem::EOF) => return None,
            Ok(ReadItem::Some { read_size: _, obj }) => obj,
        };

        let val: OptDatum = match read_item::<OptDatum>(&mut self.file) {
            Err(e) => return Some(Err(anyhow!(e))),
            Ok(ReadItem::EOF) => {
                return Some(Err(anyhow!(
                    "KeyValueIterator ,, Unexpected EOF while reading a value."
                )))
            }
            Ok(ReadItem::Some { read_size: _, obj }) => obj,
        };

        Some(Ok((key, val)))
    }
}
