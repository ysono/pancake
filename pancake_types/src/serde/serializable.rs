use crate::serde::{Datum, DatumReader, DatumWriter, OptDatum, ReadResult, WriteLen};
use anyhow::{anyhow, Result};
use std::any;
use std::io::{Read, Seek, Write};

pub trait Ser {
    fn ser<W: Write>(&self, w: &mut DatumWriter<W>) -> Result<WriteLen>;
}
pub trait Deser: Sized {
    fn deser<R: Read + Seek>(r: &mut DatumReader<R>) -> Result<ReadResult<Self>>;
}

impl<T, E> Deser for T
where
    T: TryFrom<Datum, Error = E>,
    E: Into<anyhow::Error>,
{
    fn deser<R: Read + Seek>(r: &mut DatumReader<R>) -> Result<ReadResult<Self>> {
        match r.deser()? {
            ReadResult::EOF => Ok(ReadResult::EOF),
            ReadResult::Some(_, OptDatum::Tombstone) => Err(anyhow!(
                "Tombstone while reading {}",
                any::type_name::<Self>()
            )),
            ReadResult::Some(r_len, OptDatum::Some(dat)) => {
                let moi = Self::try_from(dat).map_err(|e| anyhow!(e))?;
                Ok(ReadResult::Some(r_len, moi))
            }
        }
    }
}
impl<T, E> Deser for OptDatum<T>
where
    T: TryFrom<Datum, Error = E>,
    E: Into<anyhow::Error>,
{
    fn deser<R: Read + Seek>(r: &mut DatumReader<R>) -> Result<ReadResult<Self>> {
        match r.deser()? {
            ReadResult::EOF => Ok(ReadResult::EOF),
            ReadResult::Some(r_len, OptDatum::Tombstone) => {
                Ok(ReadResult::Some(r_len, OptDatum::Tombstone))
            }
            ReadResult::Some(r_len, OptDatum::Some(dat)) => {
                let t = T::try_from(dat).map_err(|e| anyhow!(e))?;
                Ok(ReadResult::Some(r_len, OptDatum::Some(t)))
            }
        }
    }
}

pub trait Serializable: Ser + Deser {}
