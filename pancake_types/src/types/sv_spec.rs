use crate::io_utils;
use crate::serde::{Datum, DatumType, DatumTypeInt};
use crate::types::{SVShared, Value};
use anyhow::Result;
use owning_ref::OwningRef;
use std::io::{BufRead, Cursor, Write};
use std::str;
use std::sync::Arc;

mod test;

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
    pub member_idxs: Vec<u32>,
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
            let member_idx = *member_idx as usize;
            match dat {
                Datum::Tuple(members) if member_idx < members.len() => {
                    dat = &members[member_idx];
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
    pub fn ser<W: Write>(&self, w: &mut W) -> Result<()> {
        /* datum_type */
        let datum_type_int = DatumTypeInt::from(self.datum_type);
        write!(w, "{};", *datum_type_int)?;

        /* member_idxs */
        for member_idx in self.member_idxs.iter() {
            write!(w, "{},", member_idx)?;
        }

        Ok(())
    }

    pub fn deser<R: BufRead>(r: &mut R) -> Result<Self> {
        let mut buf = vec![];

        /* datum_type */
        io_utils::read_until_then_trim(r, ';' as u8, &mut buf)?;
        let datum_type_int = str::from_utf8(&buf)?.parse::<u8>()?;
        let datum_type_int = DatumTypeInt::from(datum_type_int);
        let datum_type = DatumType::try_from(datum_type_int)?;

        /* member_idxs */
        let mut member_idxs = vec![];
        loop {
            buf.clear();
            io_utils::read_until_then_trim(r, ',' as u8, &mut buf)?;
            if buf.is_empty() {
                break;
            }
            let member_idx = str::from_utf8(&buf)?.parse::<u32>()?;
            member_idxs.push(member_idx);
        }

        Ok(Self {
            member_idxs,
            datum_type,
        })
    }

    pub fn ser_solo(&self) -> Result<Vec<u8>> {
        let mut buf = vec![];
        self.ser(&mut buf)?;
        Ok(buf)
    }

    pub fn deser_solo(buf: &[u8]) -> Result<Self> {
        let mut r = Cursor::new(&buf);
        Self::deser(&mut r)
    }
}
