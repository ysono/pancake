use crate::storage::serde::{Datum, DatumBodyLen, DatumType, DatumTypeInt, MembersCount, OptDatum};
use anyhow::{anyhow, Result};
use derive_more::From;
use std::io::{BufReader, ErrorKind, Read, Seek, SeekFrom};
use std::mem;

#[derive(PartialEq, Eq, Debug)]
pub enum ReadResult<T> {
    EOF,
    Some(usize, T),
}

#[derive(From)]
pub struct DatumReader<R: Read + Seek> {
    r: BufReader<R>,
}

impl<R: Read + Seek> DatumReader<R> {
    pub fn skip(&mut self) -> Result<ReadResult<()>> {
        match DatumTypeInt::read(&mut self.r) {
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(ReadResult::EOF),
            Err(e) => return Err(anyhow!(e)),
            Ok((mut r_len, dat_type_int)) => {
                let dat_type = DatumType::try_from(dat_type_int)?;
                let dat_body_len = match dat_type {
                    DatumType::Tombstone => 0,
                    DatumType::I64 => mem::size_of::<i64>(),
                    DatumType::Bytes | DatumType::Str | DatumType::Tuple => {
                        let (delta_r_len, body_len) =
                            DatumBodyLen::read(&mut self.r).map_err(|e| anyhow!(e))?;
                        r_len += delta_r_len;
                        *body_len
                    }
                };
                self.r.seek(SeekFrom::Current(dat_body_len as i64))?;
                r_len += dat_body_len;
                Ok(ReadResult::Some(r_len, ()))
            }
        }
    }

    pub fn deser(&mut self) -> Result<ReadResult<OptDatum<Datum>>> {
        self.deser_single(true)
    }

    fn deser_single(&mut self, is_root: bool) -> Result<ReadResult<OptDatum<Datum>>> {
        match DatumTypeInt::read(&mut self.r) {
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(ReadResult::EOF),
            Err(e) => return Err(anyhow!(e)),
            Ok((mut r_len, dat_type_int)) => {
                let dat_type = DatumType::try_from(dat_type_int)?;
                let optdat = match dat_type {
                    DatumType::Tombstone => OptDatum::Tombstone,
                    DatumType::I64 => {
                        let mut buf = [0u8; mem::size_of::<i64>()];
                        self.r.read_exact(&mut buf).map_err(|e| anyhow!(e))?;
                        r_len += buf.len();
                        let i = i64::from_ne_bytes(buf);
                        OptDatum::Some(Datum::I64(i))
                    }
                    DatumType::Bytes => {
                        let buf = self.read_variable_body_bytes(&mut r_len)?;
                        OptDatum::Some(Datum::Bytes(buf))
                    }
                    DatumType::Str => {
                        let buf = self.read_variable_body_bytes(&mut r_len)?;
                        let s = String::from_utf8(buf)?;
                        OptDatum::Some(Datum::Str(s))
                    }
                    DatumType::Tuple => {
                        let dat = self.deser_tuple(is_root, &mut r_len)?;
                        OptDatum::Some(dat)
                    }
                };
                Ok(ReadResult::Some(r_len, optdat))
            }
        }
    }

    fn read_variable_body_bytes(&mut self, r_len: &mut usize) -> Result<Vec<u8>> {
        let (delta_r_len, body_len) = DatumBodyLen::read(&mut self.r).map_err(|e| anyhow!(e))?;
        *r_len += delta_r_len;

        let mut buf = vec![0u8; *body_len];
        self.r.read_exact(&mut buf).map_err(|e| anyhow!(e))?;
        *r_len += buf.len();

        Ok(buf)
    }

    fn deser_tuple(&mut self, is_root: bool, r_len: &mut usize) -> Result<Datum> {
        if is_root {
            let dat_body_len_len = mem::size_of::<DatumBodyLen>();
            self.r.seek(SeekFrom::Current(dat_body_len_len as i64))?;
            *r_len += dat_body_len_len;
        }

        let (delta_r_len, members_count) =
            MembersCount::read(&mut self.r).map_err(|e| anyhow!(e))?;
        *r_len += delta_r_len;

        let mut members = Vec::with_capacity(*members_count);
        for _ in 0..*members_count {
            match self.deser_single(false)? {
                ReadResult::EOF => return Err(anyhow!("EOF while reading Tuple member.")),
                ReadResult::Some(_, OptDatum::Tombstone) => {
                    return Err(anyhow!("Tombstone nested under Tuple."))
                }
                ReadResult::Some(delta_r_len, OptDatum::Some(dat)) => {
                    *r_len += delta_r_len;
                    members.push(dat);
                }
            }
        }
        Ok(Datum::Tuple(members))
    }
}
