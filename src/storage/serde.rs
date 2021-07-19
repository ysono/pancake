use super::api::{Key, Value};
use anyhow::{anyhow, Result};
use std::fs::File;
use std::io::{Read, Write};
use std::mem::size_of;

fn write_key(k: &Key, w: &mut impl Write) -> Result<()> {
    w.write(&k.0.len().to_le_bytes())?;
    w.write(k.0.as_bytes())?;
    Ok(())
}

fn write_val(v: Option<&Value>, w: &mut impl Write) -> Result<()> {
    match v {
        None => {
            w.write(&(0 as usize).to_le_bytes())?;
        }
        Some(v) => match v {
            Value::Bytes(bytes) => {
                w.write(&bytes.len().to_le_bytes())?;
                w.write(bytes)?;
            }
        },
    }
    Ok(())
}

/// This file format is applicable for both the commit log and ss tables.
pub fn write_kv(k: &Key, v: Option<&Value>, file: &mut File) -> Result<()> {
    write_key(k, file)?;
    write_val(v, file)?;
    Ok(())
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
///     If the data bytes == 0, then None. This indicates a tombstone.
///     Else, a vector containing raw bytes.
///
/// An error is returned if the exact amount of expected bytes cannot be read from file.
fn read_item(file: &mut File) -> Result<FileItem> {
    const PRE_SIZE: usize = size_of::<usize>();
    let mut buf = [0u8; PRE_SIZE];
    let read_size = file.read(&mut buf)?;
    if read_size == 0 {
        return Ok(FileItem::EOF);
    } else if read_size != buf.len() {
        return Err(anyhow!("File is corrupted."));
    }

    let item_size = usize::from_le_bytes(buf);
    match item_size {
        0 => {
            // Value is tombstone.
            Ok(FileItem::Item(PRE_SIZE, None))
        }
        item_size => {
            let mut buf = vec![0u8; item_size];
            file.read_exact(&mut buf)?;
            Ok(FileItem::Item(PRE_SIZE + item_size, Some(buf)))
        }
    }
}

fn deserialize_key(bytes: Vec<u8>) -> Result<Key> {
    Ok(Key(String::from_utf8(bytes)?))
}

fn deserialize_val(bytes: Vec<u8>) -> Value {
    Value::Bytes(bytes)
}

pub struct KeyValueIterator<'a> {
    file: &'a mut File,
}

impl<'a> From<&'a mut File> for KeyValueIterator<'a> {
    fn from(file: &'a mut File) -> Self {
        Self { file }
    }
}

impl Iterator for KeyValueIterator<'_> {
    type Item = Result<(usize, Key, Option<Value>)>;

    fn next(&mut self) -> Option<Self::Item> {
        // TODO(btc): if the iterator returns an error, perhaps it should continue to return errors for all subsequent calls?
        match read_item(self.file).unwrap() {
            FileItem::EOF => None,
            FileItem::Item(_, None) => Some(Err(anyhow!("Read key as a zero-byte item."))),
            FileItem::Item(key_raw_size, Some(key_bytes)) => match read_item(self.file).unwrap() {
                FileItem::EOF => Some(Err(anyhow!("Key without value."))),
                FileItem::Item(val_raw_size, maybe_val_bytes) => {
                    let size = key_raw_size + val_raw_size;
                    let key = deserialize_key(key_bytes).unwrap();
                    let maybe_val = maybe_val_bytes.map(deserialize_val);
                    Some(Ok((size, key, maybe_val)))
                }
            },
        }
    }
}