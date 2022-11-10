use crate::serde::{Datum, OptDatum, ReadResult, WriteLen};
use anyhow::{anyhow, Result};
use std::any;
use std::borrow::Borrow;
use std::io::{Cursor, Read, Seek, Write};

pub trait Ser {
    fn ser<W: Write>(&self, w: &mut W) -> Result<WriteLen>;

    fn ser_solo(&self) -> Result<Vec<u8>> {
        let mut buf = vec![];
        self.ser(&mut buf)?;
        Ok(buf)
    }
}
pub trait Deser: Sized {
    fn skip<R: Read + Seek>(r: &mut R) -> Result<ReadResult<()>>;
    fn deser<R: Read + Seek>(r: &mut R) -> Result<ReadResult<Self>>;

    fn deser_solo(buf: &[u8]) -> Result<Self> {
        let mut r = Cursor::new(&buf);
        match Self::deser(&mut r)? {
            ReadResult::EOF => Err(anyhow!("No data")),
            ReadResult::Some(_, moi) => Ok(moi),
        }
    }
}

/* Blanket impls for Ser */
impl<T> Ser for T
where
    T: Borrow<Datum>,
{
    fn ser<W: Write>(&self, w: &mut W) -> Result<WriteLen> {
        let dat: &Datum = self.borrow();
        dat.ser(w)
    }
}
impl<T> Ser for OptDatum<T>
where
    T: Borrow<Datum>,
{
    fn ser<W: Write>(&self, w: &mut W) -> Result<WriteLen> {
        match self {
            OptDatum::Tombstone => OptDatum::Tombstone.ser(w),
            OptDatum::Some(t) => {
                let dat: &Datum = t.borrow();
                dat.ser(w)
            }
        }
    }
}

/* Blanket impls for Deser */
impl<T, E> Deser for T
where
    T: TryFrom<Datum, Error = E>,
    E: Into<anyhow::Error>,
{
    fn skip<R: Read + Seek>(r: &mut R) -> Result<ReadResult<()>> {
        OptDatum::<Datum>::skip(r)
    }
    fn deser<R: Read + Seek>(r: &mut R) -> Result<ReadResult<Self>> {
        match OptDatum::<Datum>::deser(r)? {
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
    fn skip<R: Read + Seek>(r: &mut R) -> Result<ReadResult<()>> {
        OptDatum::<Datum>::skip(r)
    }
    fn deser<R: Read + Seek>(r: &mut R) -> Result<ReadResult<Self>> {
        match OptDatum::<Datum>::deser(r)? {
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

/* trait Serializable */
pub trait Serializable: Ser + Deser {}
