use crate::storage::serde::{Datum, DatumType, DatumTypeInt};
use crate::storage::types::{SVShared, Value};
use anyhow::{anyhow, Result};
use num_traits::{FromPrimitive, ToPrimitive};
use owning_ref::OwningRef;
use std::any;
use std::io::{Read, Write};
use std::mem;
use std::sync::Arc;

/// [`SubValueSpec`] specifies a sub-portion of a [`Value`].
///
/// The spec is a DSL for locating this sub-portion,
/// as well as an extractor of this sub-portion that returns [`SVShared`].
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
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum SubValueSpec {
    Whole(DatumType),
    PartialTuple {
        member_idx: usize,
        member_spec: Box<SubValueSpec>,
    },
}

impl SubValueSpec {
    pub fn extract(&self, pv: &Arc<Value>) -> Option<SVShared> {
        self.extract_impl(pv).map(|dat| {
            let dat = dat as *const _;
            let dat = unsafe { &*dat };
            let ownref = OwningRef::new(Arc::clone(pv)).map(|_| dat);
            SVShared::Ref(ownref)
        })
    }

    fn extract_impl<'a>(&self, dat: &'a Datum) -> Option<&'a Datum> {
        match self {
            SubValueSpec::Whole(exp_dtype) => {
                let actual_dtype = dat.to_type();
                if exp_dtype == &actual_dtype {
                    Some(dat)
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
}

impl SubValueSpec {
    pub fn ser(&self, w: &mut impl Write) -> Result<()> {
        match self {
            Self::Whole(datum_type) => {
                let datum_type_int: DatumTypeInt = datum_type.to_u8().unwrap();
                w.write(&datum_type_int.to_le_bytes())?;
                Ok(())
            }
            Self::PartialTuple {
                member_idx,
                member_spec,
            } => {
                /* Write depth-first. Reading will be breadth-first. */
                member_spec.ser(w)?;
                w.write(&member_idx.to_le_bytes())?;
                Ok(())
            }
        }
    }

    pub fn deser(r: &mut impl Read) -> Result<Self> {
        let mut buf = [0u8; mem::size_of::<DatumTypeInt>()];
        r.read_exact(&mut buf)?;
        let datum_type_int = DatumTypeInt::from_le_bytes(buf);
        let datum_type = DatumType::from_u8(datum_type_int)
            .ok_or(anyhow!("Invalid DatumTypeInt {}", datum_type_int))?;
        let mut spec = Self::Whole(datum_type);

        loop {
            let mut buf = [0u8; mem::size_of::<usize>()];
            let r_len = r.read(&mut buf)?;
            if r_len == 0 {
                return Ok(spec);
            } else if r_len == buf.len() {
                let member_idx = usize::from_le_bytes(buf);
                spec = Self::PartialTuple {
                    member_idx,
                    member_spec: Box::new(spec),
                };
            } else {
                return Err(anyhow!(
                    "Byte misalignment in file for {}.",
                    any::type_name::<Self>()
                ));
            }
        }
    }
}
