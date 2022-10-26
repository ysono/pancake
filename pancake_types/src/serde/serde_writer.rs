use crate::serde::{
    Datum, DatumBodyLen, DatumType, DatumTypeInt, MembersCount, NestedDatumLen, OptDatum,
    TupleDatumBodyLen,
};
use anyhow::Result;
use derive_more::{Deref, DerefMut, From, Into};
use std::borrow::Borrow;
use std::io::{BufWriter, Write};

#[derive(Deref)]
pub struct WriteLen(usize);

#[derive(From, Into, Deref, DerefMut)]
pub struct DatumWriter<W: Write> {
    w: BufWriter<W>,
}

impl<W: Write> DatumWriter<W> {
    pub fn ser_optdat(&mut self, optdat: &OptDatum<Datum>) -> Result<WriteLen> {
        let dat_type = DatumType::from(optdat);
        match optdat {
            OptDatum::Tombstone => self.ser_fixed_len_datum_body(dat_type, [0u8; 0]),
            OptDatum::Some(dat) => self.ser_dat(dat),
        }
    }

    pub fn ser_dat(&mut self, dat: &Datum) -> Result<WriteLen> {
        let dat_type = DatumType::from(dat);
        match dat {
            Datum::I64(i) => self.ser_fixed_len_datum_body(dat_type, i.to_ne_bytes()),
            Datum::Bytes(b) => self.ser_variable_len_datum_body(dat_type, b),
            Datum::Str(s) => self.ser_variable_len_datum_body(dat_type, s.as_bytes()),
            Datum::Tuple(members) => self.ser_root_tuple(&members[..]),
        }
    }

    fn ser_fixed_len_datum_body<const LEN: usize>(
        &mut self,
        dat_type: DatumType,
        buf: [u8; LEN],
    ) -> Result<WriteLen> {
        let mut w_len = 0;
        w_len += self.w.write(&DatumTypeInt::from(dat_type).to_ne_bytes())?;
        w_len += self.w.write(&buf)?;
        Ok(WriteLen(w_len))
    }
    fn ser_variable_len_datum_body(&mut self, dat_type: DatumType, buf: &[u8]) -> Result<WriteLen> {
        let mut w_len = 0;
        w_len += self.w.write(&DatumTypeInt::from(dat_type).to_ne_bytes())?;
        w_len += self
            .w
            .write(&DatumBodyLen::from_body_buf(buf).to_ne_bytes())?;
        w_len += self.w.write(buf)?;
        Ok(WriteLen(w_len))
    }

    pub fn ser_root_tuple<D: Borrow<Datum>>(&mut self, members: &[D]) -> Result<WriteLen> {
        let root_tup_body_len = Self::derive_nested_tuple_len(members);

        let mut w_len = 0;
        w_len += self
            .w
            .write(&DatumTypeInt::from(DatumType::Tuple).to_ne_bytes())?;
        w_len += self.w.write(&root_tup_body_len.to_ne_bytes())?;
        self.ser_nested_tuple_body(members, &mut w_len)?;
        Ok(WriteLen(w_len))
    }
    fn derive_nested_tuple_len<D: Borrow<Datum>>(members: &[D]) -> TupleDatumBodyLen {
        let mut tup_len = TupleDatumBodyLen::new();
        for member in members {
            tup_len.add_member(Self::derive_nested_single_len(member.borrow()));
        }
        tup_len
    }
    fn derive_nested_single_len(dat: &Datum) -> NestedDatumLen {
        match dat {
            Datum::I64(i) => NestedDatumLen::from_fixed_body_len(&i.to_ne_bytes()),
            Datum::Bytes(b) => {
                NestedDatumLen::from_variable_body_len(DatumBodyLen::from_body_buf(b))
            }
            Datum::Str(s) => {
                NestedDatumLen::from_variable_body_len(DatumBodyLen::from_body_buf(s.as_bytes()))
            }
            Datum::Tuple(members) => {
                let tup_len = Self::derive_nested_tuple_len(&members[..]);
                NestedDatumLen::from_tuple_body_len(tup_len)
            }
        }
    }
    fn ser_nested_tuple_body<D: Borrow<Datum>>(
        &mut self,
        members: &[D],
        w_len: &mut usize,
    ) -> Result<()> {
        *w_len += self
            .w
            .write(&MembersCount::from_members(members).to_ne_bytes())?;
        for member in members {
            self.ser_nested_single(member.borrow(), w_len)?;
        }
        Ok(())
    }
    fn ser_nested_single(&mut self, dat: &Datum, w_len: &mut usize) -> Result<()> {
        let dat_type = DatumType::from(dat);
        match dat {
            Datum::I64(i) => {
                *w_len += self.ser_fixed_len_datum_body(dat_type, i.to_ne_bytes())?.0;
            }
            Datum::Bytes(b) => {
                *w_len += self.ser_variable_len_datum_body(dat_type, b)?.0;
            }
            Datum::Str(s) => {
                *w_len += self.ser_variable_len_datum_body(dat_type, s.as_bytes())?.0;
            }
            Datum::Tuple(members) => {
                *w_len += self
                    .w
                    .write(&DatumTypeInt::from(DatumType::Tuple).to_ne_bytes())?;
                self.ser_nested_tuple_body(members, w_len)?;
            }
        }
        Ok(())
    }
}
