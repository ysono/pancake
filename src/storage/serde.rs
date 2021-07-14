use super::api::{Key, Value};
use anyhow::{anyhow, Result};
use std::fs::File;
use std::io::{Read, Write};
use std::mem::size_of;

pub fn write_key(k: &Key, w: &mut impl Write) -> Result<()> {
    w.write(&k.0.len().to_le_bytes())?;
    w.write(k.0.as_bytes())?;
    Ok(())
}

pub fn write_val(v: Option<&Value>, w: &mut impl Write) -> Result<()> {
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
    let mut buffer = Vec::<u8>::new();
    write_key(k, &mut buffer)?;
    write_val(v, &mut buffer)?;

    file.write(&buffer)?;

    Ok(())
}

/// In case of success, the returned tuple is:
/// tuple.0 =
///     Amount of bytes read from file. This includes:
///     1) the prefix bytes indicating the length of data
///     2) the bytes taken up by the data
///     If EOF, this amount is zero.
/// tuple.1 =
///     A vector containing raw bytes.
///
/// An error is returned if the exact amount of expected bytes cannot be read from file.
pub fn read_item(file: &mut File) -> Result<(usize, Option<Vec<u8>>)> {
    const PRE_SIZE: usize = size_of::<usize>();
    let mut buf = [0u8; PRE_SIZE];
    let read_size = file.read(&mut buf)?;
    if read_size == 0 {
        return Ok((0, None));
    } else if read_size != buf.len() {
        return Err(anyhow!("File is corrupted."));
    }

    let item_size = usize::from_le_bytes(buf);
    match item_size {
        0 => {
            // Value is tombstone.
            Ok((PRE_SIZE, None))
        }
        item_size => {
            let mut buf = vec![0u8; item_size];
            file.read_exact(&mut buf)?;
            Ok((PRE_SIZE + item_size, Some(buf)))
        }
    }
}

pub fn deserialize_key(bytes: Vec<u8>) -> Result<Key> {
    Ok(Key(String::from_utf8(bytes)?))
}

pub fn deserialize_val(bytes: Vec<u8>) -> Result<Value> {
    Ok(Value::Bytes(bytes))
}

pub struct KeyValueIterator<'a> {
    pub file: &'a mut File,
}

impl Iterator for KeyValueIterator<'_> {
    type Item = (usize, Key, Option<Value>);

    fn next(&mut self) -> Option<Self::Item> {
        match read_item(self.file).unwrap() {
            (0, _) => {
                // EOF.
                None
            }
            (_, None) => {
                panic!("Read key as a zero-byte item.")
            }
            (key_raw_size, Some(key_bytes)) => {
                match read_item(self.file).unwrap() {
                    (0, _) => {
                        // EOF.
                        panic!("Key without value.")
                    }
                    (val_raw_size, maybe_val_bytes) => {
                        let key = deserialize_key(key_bytes).unwrap();
                        let maybe_val =
                            maybe_val_bytes.map(|bytes| deserialize_val(bytes).unwrap());
                        Some((key_raw_size + val_raw_size, key, maybe_val))
                    }
                }
            }
        }
    }
}
