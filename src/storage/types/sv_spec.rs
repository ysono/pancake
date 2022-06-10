use crate::storage::serde::{Datum, DatumType, DatumTypeInt};
use crate::storage::types::{SVShared, Value};
use anyhow::{anyhow, Result};
use owning_ref::OwningRef;
use std::any;
use std::io::{BufReader, BufWriter, Read, Write};
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
/// If the target is a [`DatumType::Tuple`], we specify the undivided tuple.
///
/// If the sub-portion is actually the entire [`Value`], then `member_idxs` is empty.
/// If the sub-portion is nested within a [`Datum::Tuple`], then `member_idxs` specifies the member idx at each depth.
///
/// For example, given a tuple-typed [`Value`]
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
#[derive(PartialEq, Eq, Hash, Debug)]
pub struct SubValueSpec {
    pub member_idxs: Vec<usize>,
    pub datum_type: DatumType,
}

/* Shorthand helper for a non-nested spec. */
impl SubValueSpec {
    pub fn whole(datum_type: DatumType) -> Self {
        Self {
            member_idxs: vec![],
            datum_type,
        }
    }
}

/* Extraction. */
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

        if DatumType::from(dat) == self.datum_type {
            let dat = dat as *const _;
            let dat = unsafe { &*dat };
            let ownref = OwningRef::new(Arc::clone(pv)).map(|_| dat);
            return Some(SVShared::Ref(ownref));
        }

        None
    }
}

/* De/Serialization. */
impl SubValueSpec {
    pub fn ser<W: Write>(&self, w: &mut BufWriter<W>) -> Result<()> {
        // Write datum_type first, for easy alignment during reading.
        let datum_type_int = DatumTypeInt::from(self.datum_type);
        w.write(&datum_type_int.to_ne_bytes())?;

        for member_idx in self.member_idxs.iter() {
            w.write(&member_idx.to_ne_bytes())?;
        }

        Ok(())
    }

    pub fn deser<R: Read>(r: &mut BufReader<R>) -> Result<Self> {
        let (_r_len, datum_type_int) = DatumTypeInt::read(r).map_err(|e| anyhow!(e))?;
        let datum_type = DatumType::try_from(datum_type_int)?;

        let mut member_idxs = vec![];
        loop {
            let mut buf = [0u8; mem::size_of::<usize>()];
            let r_len = r.read(&mut buf)?;
            if r_len == 0 {
                break;
            } else if r_len == buf.len() {
                let member_idx = usize::from_ne_bytes(buf);
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

#[cfg(test)]
mod test;
