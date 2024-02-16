use crate::serde::{Datum, DatumBodyLen, DatumType, DatumTypeInt, OptDatum, TupleMembersCount};
use anyhow::Result;
use derive_more::Deref;
use std::io::Write;
use std::mem;

#[derive(Deref)]
pub struct WriteLen(usize);
impl WriteLen {
    pub fn new_manual(i: usize) -> Self {
        Self(i)
    }
}

impl OptDatum<Datum> {
    pub fn ser(&self, w: &mut impl Write) -> Result<WriteLen> {
        match self {
            OptDatum::Tombstone => {
                let dtype = DatumTypeInt::from(DatumType::Tombstone);
                let w_len = w.write(&dtype.to_le_bytes())?;
                Ok(WriteLen(w_len))
            }
            OptDatum::Some(datum) => datum.ser(w),
        }
    }
}

impl Datum {
    pub fn ser(&self, w: &mut impl Write) -> Result<WriteLen> {
        self.ser_::<true>(w)
    }

    fn ser_<const IS_ROOT: bool>(&self, w: &mut impl Write) -> Result<WriteLen> {
        let mut w_len = WriteLen(0);

        /* datum_type */
        let dtype = DatumType::from(self);
        let dtype = DatumTypeInt::from(dtype);
        w_len.0 += w.write(&dtype.to_le_bytes())?;

        /* datum_body_len */
        let dbody_len = match self {
            Datum::I64(_) => None,
            Datum::Bytes(b) => Some(DatumBodyLen::from_dynalen_body(b)?),
            Datum::Str(s) => Some(DatumBodyLen::from_dynalen_body(s.as_bytes())?),
            Datum::Tuple(_) => {
                if IS_ROOT {
                    Some(self.intra_tuple_datum_len::<true>()?)
                } else {
                    None
                }
            }
        };
        if let Some(datum_body_len) = dbody_len {
            w_len.0 += w.write(&datum_body_len.to_le_bytes())?;
        }

        /* datum_body */
        match self {
            Datum::I64(i) => w_len.0 += w.write(&i.to_le_bytes())?,
            Datum::Bytes(b) => w_len.0 += w.write(b)?,
            Datum::Str(s) => w_len.0 += w.write(s.as_bytes())?,
            Datum::Tuple(members) => {
                /* members_count */
                let membs_ct = TupleMembersCount::from_members(members)?;
                w_len.0 += w.write(&membs_ct.to_le_bytes())?;

                /* members */
                for member in members {
                    w_len.0 += member.ser_::<false>(w)?.0;
                }
            }
        }

        Ok(w_len)
    }

    fn intra_tuple_datum_len<const IS_ROOT: bool>(&self) -> Result<DatumBodyLen> {
        /* datum_type's len */
        let dtype_len = match self {
            Datum::I64(_) | Datum::Bytes(_) | Datum::Str(_) => mem::size_of::<DatumTypeInt>(),
            Datum::Tuple(_) => {
                if IS_ROOT {
                    0
                } else {
                    mem::size_of::<DatumTypeInt>()
                }
            }
        };

        /* datum_body_len's len */
        let dbody_len_len = match self {
            Datum::I64(_) => 0,
            Datum::Bytes(_) | Datum::Str(_) => mem::size_of::<DatumBodyLen>(),
            Datum::Tuple(_) => 0,
        };

        /* datum_body's len */
        let dbody_len = match self {
            Datum::I64(i) => mem::size_of_val(i),
            Datum::Bytes(b) => b.len(),
            Datum::Str(s) => s.as_bytes().len(),
            Datum::Tuple(members) => {
                /* members_count's len */
                let mut root_body_len = mem::size_of::<TupleMembersCount>();

                /* members' lens */
                for memb in members {
                    let memb_body_len = memb.intra_tuple_datum_len::<false>()?;
                    root_body_len += *memb_body_len as usize;
                }

                root_body_len
            }
        };

        /* total */
        let tot = dtype_len + dbody_len_len + dbody_len;
        let tot = u32::try_from(tot)?;
        Ok(DatumBodyLen::new_manual(tot))
    }
}
