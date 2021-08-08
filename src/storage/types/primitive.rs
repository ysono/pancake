use crate::storage::serde::{DatumType, Serializable};
use crate::storage::types::Datum;
use anyhow::{anyhow, Result};
use std::fs::File;
use std::io::Write;

#[derive(Clone, Debug)]
pub struct Bool(pub bool);

impl Bool {
    fn to_datum(&self) -> Result<Datum> {
        let dat = Datum::Bytes(vec![1u8]);
        Ok(dat)
    }

    fn from_datum(dat: &Datum) -> Result<Self> {
        if let Datum::Bytes(vec) = dat {
            if let [b] = vec.as_slice() {
                let obj = Self(b == &1u8);
                return Ok(obj);
            }
        }
        Err(anyhow!("Bool could not be deserialized from {:?}", dat))
    }
}

impl Serializable for Bool {
    fn ser(&self, w: &mut impl Write) -> Result<usize> {
        self.to_datum()?.ser(w)
    }

    fn deser(datum_size: usize, datum_type: DatumType, r: &mut File) -> Result<Self> {
        let dat = Datum::deser(datum_size, datum_type, r)?;
        Self::from_datum(&dat)
    }
}
