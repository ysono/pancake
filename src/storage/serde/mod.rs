//! # Serialization format
//!
//! Following pseudocode depicts the byte representation on disk. The actual struct definitions do not exit.
//!
//! This file format is applicable for both the commit log and ss tables.
//!
//! ```text
//! struct File {
//!     k0: Item,
//!     v0: Item,
//!     k1: Item,
//!     v1: Item,
//!     ...
//!     // There are no separators in between Items and nothing to indicate
//!     // whether an Item is a key or a value.
//! }
//!
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
//! ```
//!
//! A tuple is a type of datum that can nest other data, including other tuple-typed data.
//! For a tuple, the `datum` byte sequence encodes the following structure:
//!
//! ```text
//! struct TupleDatum {
//!     length_of_tuple: usize,
//!     member_0: Item,
//!     member_1: Item,
//!     ...
//!     member_n-1: Item,
//! }
//! ```

mod types;
pub use types::*;

use anyhow::{anyhow, Result};
use num_traits::FromPrimitive;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::mem::size_of;

type DatumTypeInt = u8;

pub trait Serializable: Sized {
    fn ser(&self, w: &mut impl Write) -> Result<usize>;
    fn deser(datum_size: usize, datum_type: DatumType, r: &mut File) -> Result<Self>;
}

/// @return Total count of bytes that are written to file.
pub fn write_item(datum_type: DatumType, datum_bytes: &[u8], w: &mut impl Write) -> Result<usize> {
    let mut span_size = 0usize;
    span_size += size_of::<DatumTypeInt>();
    span_size += datum_bytes.len();

    let mut write_size = 0usize;
    write_size += w.write(&span_size.to_le_bytes())?;
    write_size += w.write(&(datum_type as DatumTypeInt).to_le_bytes())?;
    write_size += w.write(datum_bytes)?;

    Ok(write_size)
}

pub fn serialize_kv(
    k: &impl Serializable,
    v: &impl Serializable,
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

pub struct KeyValueIterator<K, V> {
    file: File,
    phantom: PhantomData<(K, V)>,
}

impl<K, V> From<File> for KeyValueIterator<K, V> {
    fn from(file: File) -> Self {
        Self {
            file,
            phantom: PhantomData,
        }
    }
}

impl<K, V> Iterator for KeyValueIterator<K, V>
where
    K: Serializable,
    V: Serializable,
{
    type Item = Result<(K, V)>;

    fn next(&mut self) -> Option<Self::Item> {
        let key: K = match read_item::<K>(&mut self.file) {
            Err(e) => return Some(Err(anyhow!(e))),
            Ok(ReadItem::EOF) => return None,
            Ok(ReadItem::Some { read_size: _, obj }) => obj,
        };

        let val: V = match read_item::<V>(&mut self.file) {
            Err(e) => return Some(Err(anyhow!(e))),
            Ok(ReadItem::EOF) => {
                return Some(Err(anyhow!("Unexpected EOF while reading a value.")))
            }
            Ok(ReadItem::Some { read_size: _, obj }) => obj,
        };

        Some(Ok((key, val)))
    }
}
