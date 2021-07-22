use super::api::{Key, Value};
use anyhow::{anyhow, Result};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use derive_more::From;

fn serialize_key(k: &Key, w: &mut impl Write) -> Result<usize> {
    let pre_sz = w.write(&k.len().to_le_bytes())?;
    let datum_sz = w.write(k.as_bytes())?;
    Ok(pre_sz + datum_sz)
}

fn serialize_val(v: &Value, w: &mut impl Write) -> Result<usize> {
    let sz = match v {
        Value::Tombstone => w.write(&(0usize).to_le_bytes())?,
        Value::Bytes(bytes) => {
            let pre_sz = w.write(&bytes.len().to_le_bytes())?;
            let datum_sz = w.write(bytes)?;
            pre_sz + datum_sz
        },
    };
    Ok(sz)
}

/// This file format is applicable for both the commit log and ss tables.
pub fn serialize_kv(k: &Key, v: &Value, w: &mut impl Write) -> Result<usize> {
    let pre_sz = serialize_key(k, w)?;
    let datum_sz = serialize_val(v, w)?;
    Ok(pre_sz + datum_sz)
}

fn deserialize_key(bytes: Vec<u8>) -> Result<Key> {
    Ok(Key(String::from_utf8(bytes)?))
}

fn deserialize_val(bytes: Vec<u8>) -> Value {
    Value::Bytes(bytes)
}

enum FileItem {
    EOF,
    Skip(usize),
    Empty(usize),
    Item(usize, Vec<u8>),
}

/// From file reads one item, which consists of:
/// 1) prefix bytes indicating the length of data to follow
/// 2) bytes taken up by the data
///
/// The returned bytes = the prefix bytes + the data bytes
///
/// An error is returned iff the exact amount of expected bytes cannot be read from file.
fn read_item(file: &mut File, deser: bool) -> Result<FileItem> {
    const PRE_SIZE: usize = size_of::<usize>();
    let mut buf = [0u8; PRE_SIZE];
    let read_size = file.read(&mut buf)?;
    if read_size == 0 {
        return Ok(FileItem::EOF);
    } else if read_size != buf.len() {
        return Err(anyhow!("File is corrupted."));
    }

    let datum_size = usize::from_le_bytes(buf);
    let total_size = PRE_SIZE + datum_size;
    if deser {
        if datum_size == 0 {
            Ok(FileItem::Empty(total_size))
        } else {
            let mut buf = vec![0u8; datum_size];
            file.read_exact(&mut buf)?;
            Ok(FileItem::Item(total_size, buf))
        }
    } else {
        file.seek(SeekFrom::Current(datum_size as i64))?;
        Ok(FileItem::Skip(total_size))
    }
}

pub enum FileKeyValue {
    EOF,
    KV(usize, Option<Key>, Option<Value>),
}

pub fn read_kv<F>(file: &mut File, deser_key: bool, deser_val: F) -> Result<FileKeyValue>
where
    F: Fn(&Key) -> bool,
{
    let (key_sz, maybe_key) = match read_item(file, deser_key)? {
        FileItem::EOF => { return Ok(FileKeyValue::EOF); }
        FileItem::Skip(sz) => (sz, None),
        FileItem::Empty(_) => { return Err(anyhow!("Read key as a zero-byte item.")); }
        FileItem::Item(sz, bytes) => (sz, Some(deserialize_key(bytes)?)),
    };

    let deser_val = match &maybe_key {
        None => false,
        Some(key) => deser_val(key)
    };

    let (val_sz, maybe_val) = match read_item(file, deser_val)? {
        FileItem::EOF => { return Err(anyhow!("Key without value.")) }
        FileItem::Skip(sz) => (sz, None),
        FileItem::Empty(sz) => (sz, Some(Value::Tombstone)),
        FileItem::Item(sz, bytes) => (sz, Some(deserialize_val(bytes))),
    };

    let sz = key_sz + val_sz;

    Ok(FileKeyValue::KV(sz, maybe_key, maybe_val))
}

#[derive(From)]
pub struct KeyValueIterator {
    file: File,
}

impl Iterator for KeyValueIterator {
    type Item = (Key, Value);

    fn next(&mut self) -> Option<Self::Item> {
        // TODO(btc): if read_kv returns an error, perhaps it should continue to return errors for all subsequent calls?
        match read_kv(&mut self.file, true, |_| true).unwrap() {
            FileKeyValue::EOF => None,
            FileKeyValue::KV(_, maybe_key, maybe_val) => {
                Some((maybe_key.unwrap(), maybe_val.unwrap()))
            }
        }
    }
}
