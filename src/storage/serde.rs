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

use super::api::{Datum, Key, Value};
use anyhow::{anyhow, Result};
use derive_more::From;
use num_derive::FromPrimitive;
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
#[derive(FromPrimitive)]
enum DatumType {
    Tombstone = 0,
    Bytes = 1,
    I64 = 2,
    Str = 3,
    Tuple = 4,
}

type DatumTypeInt = u8;

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

fn serialize_dat(v: &Datum, w: &mut impl Write) -> Result<usize> {
    match v {
        Datum::Bytes(b) => write_item(DatumType::Bytes, b, w),
        Datum::I64(i) => write_item(DatumType::I64, &i.to_le_bytes(), w),
        Datum::Str(s) => write_item(DatumType::Str, s.as_bytes(), w),
        Datum::Tuple(vec) => {
            let mut b: Vec<u8> = vec![];

            b.write(&vec.len().to_le_bytes())?;

            for dat in vec.iter() {
                serialize_dat(dat, &mut b)?;
            }

            write_item(DatumType::Tuple, &b, w)
        }
    }
}

fn serialize_optdat(v: &Option<Datum>, w: &mut impl Write) -> Result<usize> {
    match v {
        None => write_item(DatumType::Tombstone, &[0u8; 0], w),
        Some(dat) => serialize_dat(dat, w),
    }
}

pub fn serialize_kv(k: &Key, v: &Value, w: &mut impl Write) -> Result<usize> {
    let mut write_size = 0usize;
    write_size += serialize_dat(k, w)?;
    write_size += serialize_optdat(v, w)?;
    Ok(write_size)
}

fn deserialize_optdat(
    datum_type: DatumTypeInt,
    datum_size: usize,
    r: &mut File,
) -> Result<Option<Datum>> {
    let datum_type =
        DatumType::from_u8(datum_type).ok_or(anyhow!("Unknown datum type {}", datum_type))?;
    match datum_type {
        DatumType::Tombstone => Ok(None),
        DatumType::Bytes => {
            let mut buf = vec![0u8; datum_size];
            r.read_exact(&mut buf)?;
            Ok(Some(Datum::Bytes(buf)))
        }
        DatumType::I64 => {
            let mut buf = [0u8; size_of::<i64>()];
            r.read_exact(&mut buf)?;
            Ok(Some(Datum::I64(i64::from_le_bytes(buf))))
        }
        DatumType::Str => {
            let mut buf = vec![0u8; datum_size];
            r.read_exact(&mut buf)?;
            Ok(Some(Datum::Str(String::from_utf8(buf)?)))
        }
        DatumType::Tuple => {
            let mut tup_len_buf = [0u8; size_of::<usize>()];
            r.read_exact(&mut tup_len_buf)?;
            let tup_len = usize::from_le_bytes(tup_len_buf);

            let mut datum = Vec::<Datum>::with_capacity(tup_len);

            for _ in 0..tup_len {
                match read_item(r, true)? {
                    FileItem::EOF => return Err(anyhow!("Unexpected EOF while reading a tuple.")),
                    FileItem::Skip(_) => return Err(anyhow!("Error in read_item() logic.")),
                    FileItem::Item(_, None) => {
                        return Err(anyhow!("Unexpected tombstone while reading a tuple."))
                    }
                    FileItem::Item(_, Some(dat)) => datum.push(dat),
                }
            }

            Ok(Some(Datum::Tuple(datum)))
        }
    }
}

/// `Skip`:  The result when caller requested not to deserialize.
/// `Item`:  The result when caller requested to deserialize. `None` means a tombstone.
///
/// The `usize` item: Total count of bytes that are read from file.
enum FileItem {
    EOF,
    Skip(usize),
    Item(usize, Option<Datum>),
}

fn read_item(file: &mut File, deser: bool) -> Result<FileItem> {
    let mut span_size_buf = [0u8; size_of::<usize>()];
    let mut read_size = file.read(&mut span_size_buf)?;
    if read_size == 0 {
        return Ok(FileItem::EOF);
    } else if read_size != span_size_buf.len() {
        return Err(anyhow!(
            "Unexpected EOF while reading the beginning of an item."
        ));
    }

    let span_size = usize::from_le_bytes(span_size_buf);
    read_size += span_size;
    if deser {
        let mut datum_type_buf = [0u8; size_of::<DatumTypeInt>()];
        file.read_exact(&mut datum_type_buf)?;
        let datum_type = DatumTypeInt::from_le_bytes(datum_type_buf);

        let datum_size = span_size - datum_type_buf.len();

        let optdat = deserialize_optdat(datum_type, datum_size, file)?;

        Ok(FileItem::Item(read_size, optdat))
    } else {
        file.seek(SeekFrom::Current(span_size as i64))?;
        Ok(FileItem::Skip(read_size))
    }
}

/// `KV`: The key and value are each an `Option`, reflecting whether the caller requested to deserize that part. This optionality is separate from tombstone; tombstone is captured within the `Value` type.
pub enum FileKeyValue {
    EOF,
    KV(usize, Option<Key>, Option<Value>),
}

/// @arg `deser_key`: Whether to deserialize the key.
/// @arg `deser_val`: A callable that reads the just-deserialized key and returns whether to deserialize the value. If `deser_key` was false, then regardless of the `deser_val` argument, the value will _not_ be deserialized.
pub fn read_kv<F>(file: &mut File, deser_key: bool, deser_val: F) -> Result<FileKeyValue>
where
    F: Fn(&Key) -> bool,
{
    let (key_sz, maybe_key) = match read_item(file, deser_key)? {
        FileItem::EOF => return Ok(FileKeyValue::EOF),
        FileItem::Skip(sz) => (sz, None),
        FileItem::Item(_, None) => return Err(anyhow!("Key is tombstone.")),
        FileItem::Item(sz, Some(dat)) => (sz, Some(Key(dat))),
    };

    let deser_val: bool = maybe_key.as_ref().map(deser_val).unwrap_or(false);

    let (val_sz, maybe_val) = match read_item(file, deser_val)? {
        FileItem::EOF => return Err(anyhow!("Key without value.")),
        FileItem::Skip(sz) => (sz, None),
        FileItem::Item(sz, optdat) => (sz, Some(Value(optdat))),
    };

    let sz = key_sz + val_sz;

    Ok(FileKeyValue::KV(sz, maybe_key, maybe_val))
}

#[derive(From)]
pub struct KeyValueIterator {
    file: File,
}

/// This iterator always deserializes both the key and the value.
impl Iterator for KeyValueIterator {
    type Item = Result<(Key, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        // TODO(btc): if read_kv returns an error, perhaps it should continue to return errors for all subsequent calls?
        match read_kv(&mut self.file, true, |_| true) {
            Err(e) => Some(Err(e)),
            Ok(FileKeyValue::EOF) => None,
            Ok(FileKeyValue::KV(_, Some(key), Some(val))) => Some(Ok((key, val))),
            _ => Some(Err(anyhow!("Error in read_kv logic"))),
        }
    }
}
