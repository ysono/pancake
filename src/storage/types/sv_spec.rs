use crate::storage::serde::{Datum, DatumType, DatumTypeInt};
use crate::storage::types::{SVShared, Value};
use anyhow::{anyhow, Result};
use num_traits::{FromPrimitive, ToPrimitive};
use owning_ref::OwningRef;
use std::any;
use std::io::{Read, Write};
use std::mem;
use std::sync::Arc;

/// [`SubValueSpec`] specifies a contiguous sub-portion of a [`Value`].
///
/// The spec is a DSL for locating this sub-portion,
/// as well as an extractor of this sub-portion that returns [`SVShared`].
///
/// #### Specification
///
/// The [`DatumType`] of the target sub-portion must be specified.
/// The target could be a [`DatumType::Tuple`].
///
/// If the sub-portion is actually the entire [`Value`], `member_idxs` is empty.
/// If the sub-portion is nested with a [`Datum::Tuple`], then `member_idxs` specifies the member idx at each depth.
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
/// SubValueSpec {
///     member_idxs: vec![1, 0],
///     datum_type: DatumType::I64,
/// }
/// ```
///
/// If you want to specify the `Datum::Tuple` containing data 3 and 4:
///
/// ```text
/// SubValueSpec::PartialTuple {
///     member_idxs: vec![1, 2],
///     datum_type: DatumType::Tuple,
/// }
/// ```
///
/// Notice, in this case, `SubValueSpec::Whole(DatumType::Tuple)` specifies the whole tuple,
/// not further sub-divided.
#[derive(PartialEq, Eq, Hash, Debug)]
pub struct SubValueSpec {
    pub member_idxs: Vec<usize>,
    pub datum_type: DatumType,
}

impl From<DatumType> for SubValueSpec {
    fn from(datum_type: DatumType) -> Self {
        Self {
            member_idxs: vec![],
            datum_type,
        }
    }
}

impl SubValueSpec {
    pub fn extract(&self, pv: &Arc<Value>) -> Option<SVShared> {
        let mut dat: &Datum = pv;
        for member_idx in self.member_idxs.iter() {
            match dat {
                Datum::Tuple(members) if *member_idx < members.len() => {
                    dat = &members[*member_idx];
                }
                _ => return None,
            }
        }

        if dat.to_type() == self.datum_type {
            let dat = dat as *const _;
            let dat = unsafe { &*dat };
            let ownref = OwningRef::new(Arc::clone(pv)).map(|_| dat);
            return Some(SVShared::Ref(ownref));
        }

        None
    }
}

impl SubValueSpec {
    pub fn ser(&self, w: &mut impl Write) -> Result<()> {
        // Write datum_type first, for easy alignemnt during reading.
        let datum_type_int: DatumTypeInt = self.datum_type.to_u8().unwrap();
        w.write(&datum_type_int.to_le_bytes())?;

        for member_idx in self.member_idxs.iter() {
            w.write(&member_idx.to_le_bytes())?;
        }

        Ok(())
    }

    pub fn deser(r: &mut impl Read) -> Result<Self> {
        let mut buf = [0u8; mem::size_of::<DatumTypeInt>()];
        r.read_exact(&mut buf)?;
        let datum_type_int = DatumTypeInt::from_le_bytes(buf);
        let datum_type = DatumType::from_u8(datum_type_int)
            .ok_or(anyhow!("Invalid DatumTypeInt {}", datum_type_int))?;

        let mut member_idxs = vec![];
        loop {
            let mut buf = [0u8; mem::size_of::<usize>()];
            let r_len = r.read(&mut buf)?;
            if r_len == 0 {
                break;
            } else if r_len == buf.len() {
                let member_idx = usize::from_le_bytes(buf);
                member_idxs.push(member_idx);
            } else {
                return Err(anyhow!(
                    "Byte misalignment in file for {}.",
                    any::type_name::<Self>()
                ));
            }
        }

        Ok(Self {
            member_idxs,
            datum_type,
        })
    }
}
