use crate::storage::serde::{Datum, DatumType, Serializable};
use crate::storage::types::{PrimaryKey, SubValue};
use anyhow::{anyhow, Result};
use std::fs::File;
use std::io::Write;

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Debug)]
pub struct SubValueAndKey {
    pub sv: SubValue,
    pub pk: PrimaryKey,
}

impl SubValueAndKey {
    fn to_datum(&self) -> Result<Datum> {
        let dat = Datum::Tuple(vec![self.sv.0.clone(), self.pk.0.clone()]);
        Ok(dat)
    }

    fn from_datum(dat: &Datum) -> Result<Self> {
        if let Datum::Tuple(vec) = dat {
            if let [sv, key] = vec.as_slice() {
                let obj = Self {
                    sv: SubValue(sv.clone()),
                    pk: PrimaryKey(key.clone()),
                };
                return Ok(obj);
            }
        }
        Err(anyhow!(
            "SubValueAndKey could not be deserialized from {:?}",
            dat
        ))
    }
}

impl Serializable for SubValueAndKey {
    fn ser(&self, w: &mut impl Write) -> Result<usize> {
        self.to_datum()?.ser(w)
    }

    fn deser(datum_size: usize, datum_type: DatumType, r: &mut File) -> Result<Self> {
        let dat = Datum::deser(datum_size, datum_type, r)?;
        Self::from_datum(&dat)
    }
}
