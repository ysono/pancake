use crate::serde::{Datum, DatumTypeInt};
use anyhow::Result;
use derive_more::Deref;
use std::borrow::Borrow;
use std::io::{self, Read};
use std::mem;

#[derive(Deref, Clone, Copy)]
pub struct DatumBodyLen(usize);
impl DatumBodyLen {
    pub fn from_body_buf(buf: &[u8]) -> Self {
        Self(buf.len())
    }
    pub fn read(r: &mut impl Read) -> Result<(usize, Self), io::Error> {
        let mut buf = [0u8; mem::size_of::<usize>()];
        r.read_exact(&mut buf)?;
        let int = usize::from_ne_bytes(buf);
        Ok((buf.len(), Self(int)))
    }
}

#[derive(Deref, Clone, Copy)]
pub struct TupleDatumBodyLen(usize);
impl TupleDatumBodyLen {
    pub fn new() -> Self {
        Self(mem::size_of::<MembersCount>())
    }
    pub fn add_member(&mut self, member_len: NestedDatumLen) {
        self.0 += *member_len;
    }
}

#[derive(Deref, Clone, Copy)]
pub struct MembersCount(usize);
impl MembersCount {
    pub fn from_members<D: Borrow<Datum>>(members: &[D]) -> Self {
        Self(members.len())
    }
    pub fn read(r: &mut impl Read) -> Result<(usize, Self), io::Error> {
        let mut buf = [0u8; mem::size_of::<usize>()];
        r.read_exact(&mut buf)?;
        let int = usize::from_ne_bytes(buf);
        Ok((buf.len(), Self(int)))
    }
}

#[derive(Deref, Clone, Copy)]
pub struct NestedDatumLen(usize);
impl NestedDatumLen {
    pub fn from_fixed_body_len<const LEN: usize>(buf: &[u8; LEN]) -> Self {
        Self(mem::size_of::<DatumTypeInt>() + buf.len())
    }
    pub fn from_variable_body_len(body_len: DatumBodyLen) -> Self {
        Self(mem::size_of::<DatumTypeInt>() + mem::size_of::<DatumBodyLen>() + *body_len)
    }
    pub fn from_tuple_body_len(tup_body_len: TupleDatumBodyLen) -> Self {
        Self(mem::size_of::<DatumTypeInt>() + *tup_body_len)
    }
}
