use crate::serde::{Datum, DatumBodyLen, DatumType, DatumTypeInt, OptDatum, TupleMembersCount};
use anyhow::{anyhow, Result};
use std::io::{self, ErrorKind, Read, Seek, SeekFrom};
use std::mem;

#[derive(PartialEq, Eq, Debug)]
pub enum ReadResult<T> {
    EOF,
    Some(usize, T),
}

impl OptDatum<Datum> {
    pub fn skip<R: Read + Seek>(r: &mut R) -> Result<ReadResult<()>> {
        /* datum_type */
        let (mut r_len, dtype_int) = match DatumTypeInt::deser(r) {
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(ReadResult::EOF),
            Err(e) => return Err(anyhow!(e)),
            Ok((r_len, dtype_int)) => (r_len, dtype_int),
        };
        let dtype = DatumType::try_from(dtype_int)?;

        /* datum_body_len */
        let dbody_len = match dtype {
            DatumType::Tombstone => 0,
            DatumType::I64 => mem::size_of::<i64>(),
            DatumType::Bytes | DatumType::Str | DatumType::Tuple => {
                let (delta_r_len, dbody_len) = DatumBodyLen::deser(r).map_err(|e| anyhow!(e))?;
                r_len += delta_r_len;
                *dbody_len as usize
            }
        };

        /* datum_body */
        r.seek(SeekFrom::Current(dbody_len as i64))?;
        r_len += dbody_len;

        Ok(ReadResult::Some(r_len, ()))
    }

    pub fn deser<R: Read + Seek>(r: &mut R) -> Result<ReadResult<Self>> {
        Self::deser_::<true, _>(r)
    }

    pub fn deser_<const IS_ROOT: bool, R: Read + Seek>(r: &mut R) -> Result<ReadResult<Self>> {
        /* datum_type */
        let (mut r_len, dtype_int) = match DatumTypeInt::deser(r) {
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(ReadResult::EOF),
            Err(e) => return Err(anyhow!(e)),
            Ok((r_len, dtype_int)) => (r_len, dtype_int),
        };
        let dtype = DatumType::try_from(dtype_int)?;

        /* datum_body_len and datum_body */
        let optdat = match dtype {
            DatumType::Tombstone => OptDatum::Tombstone,
            DatumType::I64 => {
                let mut buf = [0u8; mem::size_of::<i64>()];
                r.read_exact(&mut buf).map_err(|e| anyhow!(e))?;
                r_len += buf.len();
                let i = i64::from_le_bytes(buf);
                OptDatum::Some(Datum::I64(i))
            }
            DatumType::Bytes => {
                let body = Self::deser_dynalen_body(r, &mut r_len)?;
                OptDatum::Some(Datum::Bytes(body))
            }
            DatumType::Str => {
                let body = Self::deser_dynalen_body(r, &mut r_len)?;
                let s = String::from_utf8(body)?;
                OptDatum::Some(Datum::Str(s))
            }
            DatumType::Tuple => {
                if IS_ROOT {
                    let dbody_len_len = mem::size_of::<DatumBodyLen>();
                    r.seek(SeekFrom::Current(dbody_len_len as i64))?;
                    r_len += dbody_len_len;
                }
                let dat = Self::deser_tuple_body(r, &mut r_len)?;
                OptDatum::Some(dat)
            }
        };

        return Ok(ReadResult::Some(r_len, optdat));
    }

    fn deser_dynalen_body<R: Read>(r: &mut R, r_len: &mut usize) -> Result<Vec<u8>, io::Error> {
        let (delta_r_len, dbody_len) = DatumBodyLen::deser(r)?;
        *r_len += delta_r_len;

        let mut buf = vec![0u8; *dbody_len as usize];
        r.read_exact(&mut buf)?;
        *r_len += buf.len();

        Ok(buf)
    }

    fn deser_tuple_body<R: Read + Seek>(r: &mut R, r_len: &mut usize) -> Result<Datum> {
        /* members_count */
        let (delta_r_len, membs_ct) = TupleMembersCount::deser(r).map_err(|e| anyhow!(e))?;
        *r_len += delta_r_len;

        /* members */
        let mut members = Vec::with_capacity(*membs_ct as usize);
        for _ in 0..*membs_ct {
            match Self::deser_::<false, _>(r)? {
                ReadResult::EOF => return Err(anyhow!("EOF while reading Tuple member.")),
                ReadResult::Some(delta_r_len, optdat) => {
                    *r_len += delta_r_len;
                    match optdat {
                        OptDatum::Tombstone => {
                            return Err(anyhow!("Tombstone nested under Tuple."));
                        }
                        OptDatum::Some(dat) => {
                            members.push(dat);
                        }
                    }
                }
            }
        }
        Ok(Datum::Tuple(members))
    }
}
