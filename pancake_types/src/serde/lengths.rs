use crate::serde::Datum;
use anyhow::Result;
use derive_more::Deref;
use std::borrow::Borrow;
use std::io::{self, Read};
use std::mem;

#[derive(Deref, Clone, Copy)]
pub struct DatumBodyLen(u32);
impl DatumBodyLen {
    pub fn new_manual(len: u32) -> Self {
        Self(len)
    }
    pub fn from_dynalen_body(buf: &[u8]) -> Result<Self> {
        let int = u32::try_from(buf.len())?;
        Ok(Self(int))
    }
    pub fn deser(r: &mut impl Read) -> Result<(usize, Self), io::Error> {
        let mut buf = [0u8; mem::size_of::<u32>()];
        r.read_exact(&mut buf)?;
        let int = u32::from_le_bytes(buf);
        Ok((buf.len(), Self(int)))
    }
}

#[derive(Deref, Clone, Copy)]
pub struct TupleMembersCount(u32);
impl TupleMembersCount {
    pub fn from_members<D: Borrow<Datum>>(members: &[D]) -> Result<Self> {
        let membs_ct = u32::try_from(members.len())?;
        Ok(Self(membs_ct))
    }
    pub fn deser(r: &mut impl Read) -> Result<(usize, Self), io::Error> {
        let mut buf = [0u8; mem::size_of::<u32>()];
        r.read_exact(&mut buf)?;
        let int = u32::from_le_bytes(buf);
        Ok((buf.len(), Self(int)))
    }
}
