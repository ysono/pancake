use super::api::{Key, Value};
use anyhow::{anyhow, Result};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::size_of;

fn write_key(k: &Key, w: &mut impl Write) -> Result<usize> {
    let mut sz = w.write(&k.0.len().to_le_bytes())?;
    sz += w.write(k.0.as_bytes())?;
    Ok(sz)
}

fn write_val(v: Option<&Value>, w: &mut impl Write) -> Result<usize> {
    let sz = match v {
        None => w.write(&(0usize).to_le_bytes())?,
        Some(v) => match v {
            Value::Bytes(bytes) => {
                let mut sz = w.write(&bytes.len().to_le_bytes())?;
                sz += w.write(bytes)?;
                sz
            }
        },
    };
    Ok(sz)
}

/// This file format is applicable for both the commit log and ss tables.
pub fn write_kv(k: &Key, v: Option<&Value>, file: &mut File) -> Result<usize> {
    let mut sz = write_key(k, file)?;
    sz += write_val(v, file)?;
    Ok(sz)
}

enum FileItem {
    EOF,
    Item(usize, Option<Vec<u8>>),
}

/// From file reads one item, which consists of:
/// 1) prefix bytes indicating the length of data to follow
/// 2) bytes taken up by the data
///
/// In case of success, the returned tuple is:
/// tuple.0 =
///     the raw bytes = the prefix bytes + the data bytes
/// tuple.1 =
///     If the data bytes == 0, then None. If the item is a value, this indicates a tombstone.
///     Else, a vector containing raw bytes.
///
/// An error is returned if the exact amount of expected bytes cannot be read from file.
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
    if deser {
        if datum_size == 0 {
            // The item must be a tombstone value.
            Ok(FileItem::Item(PRE_SIZE, None))
        } else {
            let mut buf = vec![0u8; datum_size];
            file.read_exact(&mut buf)?;
            Ok(FileItem::Item(PRE_SIZE + datum_size, Some(buf)))
        }
    } else {
        file.seek(SeekFrom::Current(datum_size as i64))?;
        Ok(FileItem::Item(PRE_SIZE + datum_size, None))
    }
}

pub enum FileKeyValue {
    EOF,
    KeyValue(usize, Option<Key>, Option<Value>),
}

pub fn read_kv<F>(file: &mut File, deser_key: bool, deser_val: F) -> Result<FileKeyValue>
where
    F: Fn(&Key) -> bool,
{
    match read_item(file, deser_key)? {
        FileItem::EOF => Ok(FileKeyValue::EOF),
        FileItem::Item(0, _) => Err(anyhow!("Read key as a zero-byte item.")),
        FileItem::Item(key_raw_size, maybe_key_bytes) => {
            let maybe_key = maybe_key_bytes.map(|key_bytes| deserialize_key(key_bytes).unwrap());
            let deser_val = match &maybe_key {
                None => false,
                Some(key) => deser_val(&key),
            };
            match read_item(file, deser_val).unwrap() {
                FileItem::EOF => Err(anyhow!("Key without value.")),
                FileItem::Item(val_raw_size, maybe_val_bytes) => {
                    let size = key_raw_size + val_raw_size;
                    let maybe_val = maybe_val_bytes.map(deserialize_val);
                    Ok(FileKeyValue::KeyValue(size, maybe_key, maybe_val))
                }
            }
        }
    }
}

fn deserialize_key(bytes: Vec<u8>) -> Result<Key> {
    Ok(Key(String::from_utf8(bytes)?))
}

fn deserialize_val(bytes: Vec<u8>) -> Value {
    Value::Bytes(bytes)
}

pub struct KeyValueIterator {
    file: File,
}

impl From<File> for KeyValueIterator {
    fn from(file: File) -> Self {
        Self { file }
    }
}

impl Iterator for KeyValueIterator {
    type Item = (Key, Option<Value>);

    fn next(&mut self) -> Option<Self::Item> {
        // TODO(btc): if read_kv returns an error, perhaps it should continue to return errors for all subsequent calls?
        match read_kv(&mut self.file, true, |_| true).unwrap() {
            FileKeyValue::EOF => None,
            FileKeyValue::KeyValue(_, maybe_key, maybe_val) => {
                Some((maybe_key.unwrap(), maybe_val))
            }
        }
    }
}
