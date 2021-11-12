use crate::storage::serde::{Datum, DatumType, Serializable};
use anyhow::{anyhow, Result};
use std::fs::File;
use std::io::Write;

#[derive(Clone)]
pub struct Empty;

impl Empty {
    fn to_datum(&self) -> Result<Datum> {
        let dat = Datum::Bytes(Vec::with_capacity(0));
        Ok(dat)
    }

    fn from_datum(dat: &Datum) -> Result<Self> {
        if let Datum::Bytes(vec) = dat {
            if vec.is_empty() {
                return Ok(Empty {});
            }
        }
        Err(anyhow!("Bool could not be deserialized from {:?}", dat))
    }
}

impl Serializable for Empty {
    fn ser(&self, w: &mut impl Write) -> Result<usize> {
        self.to_datum()?.ser(w)
    }

    fn deser(datum_size: usize, datum_type: DatumType, r: &mut File) -> Result<Self> {
        let dat = Datum::deser(datum_size, datum_type, r)?;
        Self::from_datum(&dat)
    }
}
