use crate::storage::serde::{DatumType, Serializable};
use crate::storage::types::{Datum, SubValue, Value};
use anyhow::{anyhow, Result};
use num_traits::{FromPrimitive, ToPrimitive};
use std::fs::File;
use std::io::Write;

/// [`SubValueSpec`] specifies a sub-portion of a [`Value`].
///
/// The spec is a DSL for locating this sub-portion,
/// as well as an extractor of this sub-portion that returns [`SubValue`].
///
/// #### Whole
///
/// [`SubValueSpec::Whole`] specifies non-subdivided [`Datum`].
/// For example, it can be used to specify the whole [`Value`].
///
/// #### Partial
///
/// [`SubValueSpec::PartialTuple`] specifies a member of a [`Datum::Tuple`].
/// The member is identified by the member index as well as a [`SubValueSpec`]
/// that's applicable at this member index.
///
/// This nested [`SubValueSpec`], in turn, can be [`SubValueSpec::Whole`] or [`SubValueSpec::PartialTuple`].
///
/// For example in pseudocode, given a tuple-typed [`Value`]
///
/// ```text
/// Value(
///     Datum::Tuple(vec![
///         Datum::I64(0),
///         Datum::Tuple(vec![
///             Datum::I64(1),
///             Datum::Str(String::from("2")),
///             Datum::Tuple(vec![
///                 Datum::I64(3),
///                 Datum::Str(String::from("4")),
///             ])
///         ])
///     ])
/// )
/// ```
///
/// If you want to specify the `Datum::I64(1)`:
///
/// ```text
/// SubValueSpec::PartialTuple {
///     member_idx: 1,
///     member_spec: Box::new(SubValueSpec::PartialTuple {
///         member_idx: 0,
///         member_spec: SubValueSpec::Whole(DatumType::I64)
///     })
/// }
/// ```
///
/// If you want to specify the `Datum::Tuple` containing data 3 and 4:
///
/// ```text
/// SubValueSpec::PartialTuple {
///     member_idx: 1,
///     member_spec: Box::new(SubValueSpec::PartialTuple {
///         member_idx: 0,
///         member_spec: SubValueSpec::Whole(DatumType::Tuple)
///     })
/// }
/// ```
///
/// Notice, in this case, `SubValueSpec::Whole(DatumType::Tuple)` specifies the whole tuple,
/// not further sub-divided.
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Debug)]
pub enum SubValueSpec {
    Whole(DatumType),
    PartialTuple {
        member_idx: usize,
        member_spec: Box<SubValueSpec>,
    },
}

impl SubValueSpec {
    pub fn extract(&self, v: &Value) -> Option<SubValue> {
        self.extract_impl(v).map(SubValue)
    }

    // pub fn extract_impl<'a>(&self, dat: &Datum) -> Option<&'a Datum> {
    fn extract_impl(&self, dat: &Datum) -> Option<Datum> {
        match self {
            SubValueSpec::Whole(exp_dtype) => {
                let actual_dtype = dat.to_type();
                if exp_dtype == &actual_dtype {
                    Some(dat.clone())
                } else {
                    None
                }
            }
            SubValueSpec::PartialTuple {
                member_idx,
                member_spec,
            } => {
                if let Datum::Tuple(members) = dat {
                    if let Some(member_dat) = members.get(*member_idx) {
                        return member_spec.extract_impl(member_dat);
                    }
                }
                None
            }
        }
    }

    // TODO Instead of converting to Datum, serialize directly.
    fn to_datum(&self) -> Result<Datum> {
        let dat = match self {
            SubValueSpec::Whole(datum_type) => {
                let datum_type = datum_type.to_i64().unwrap();
                Datum::I64(datum_type)
            }
            SubValueSpec::PartialTuple {
                member_idx,
                member_spec,
            } => Datum::Tuple(vec![
                Datum::I64(*member_idx as i64),
                member_spec.to_datum()?.clone(),
            ]),
        };
        Ok(dat)
    }

    fn from_datum(dat: &Datum) -> Result<Self> {
        if let Datum::I64(dtype) = dat {
            let dtype =
                DatumType::from_i64(*dtype).ok_or(anyhow!("Unknown datum type {}", *dtype))?;
            let ret = SubValueSpec::Whole(dtype);
            return Ok(ret);
        } else if let Datum::Tuple(vec) = dat {
            if let [Datum::I64(member_idx), member_spec] = vec.as_slice() {
                let member_idx = *member_idx as usize;
                let member_spec = SubValueSpec::from_datum(member_spec)?;
                let member_spec = Box::new(member_spec);
                let ret = SubValueSpec::PartialTuple {
                    member_idx,
                    member_spec,
                };
                return Ok(ret);
            }
        }
        Err(anyhow!(
            "SubValueSpec could not be deserialized from {:?}",
            dat
        ))
    }
}

impl Serializable for SubValueSpec {
    fn ser(&self, w: &mut impl Write) -> Result<usize> {
        self.to_datum()?.ser(w)
    }

    fn deser(datum_size: usize, datum_type: DatumType, r: &mut File) -> Result<Self> {
        let dat = Datum::deser(datum_size, datum_type, r)?;
        Self::from_datum(&dat)
    }
}
