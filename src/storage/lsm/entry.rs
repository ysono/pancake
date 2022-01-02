use crate::storage::serde::{OptDatum, Serializable};
use anyhow::{anyhow, Result};
use std::borrow::Borrow;

pub enum Entry<'a, K, V> {
    Ref((&'a K, &'a V)),
    Own(Result<(K, V)>),
}

impl<'a, K, V> Entry<'a, K, V> {
    pub fn borrow_res(&'a self) -> Result<(&'a K, &'a V)> {
        match self {
            Self::Ref((k, v)) => Ok((k, v)),
            Self::Own(res) => res
                .as_ref()
                .map(|(k, v)| (k, v))
                .or_else(|e| Err(anyhow!(e.to_string()))),
        }
    }
}

impl<'a, K, V> Entry<'a, K, V>
where
    K: Clone,
{
    pub fn take_k(self) -> Result<K> {
        match self {
            Self::Ref((k, _v)) => Ok(k.clone()),
            Self::Own(res) => res.map(|(k, _v)| k),
        }
    }
}
impl<'a, K, V> Entry<'a, K, V>
where
    V: Clone,
{
    pub fn take_v(self) -> Result<V> {
        match self {
            Self::Ref((_k, v)) => Ok(v.clone()),
            Self::Own(res) => res.map(|(_k, v)| v),
        }
    }
}
impl<'a, K, V> Entry<'a, K, V>
where
    K: Clone,
    V: Clone,
{
    pub fn take_kv(self) -> Result<(K, V)> {
        match self {
            Self::Ref((k, v)) => Ok((k.clone(), v.clone())),
            Self::Own(res) => res,
        }
    }
}
impl<'a, K, V> Entry<'a, K, V> {
    pub fn convert<K2, V2>(self) -> Entry<'a, K2, V2>
    where
        K: Borrow<K2> + Into<K2>,
        V: Borrow<V2> + Into<V2>,
    {
        match self {
            Self::Ref((k, v)) => Entry::Ref((k.borrow(), v.borrow())),
            Self::Own(res) => Entry::Own(res.map(|(k, v)| (k.into(), v.into()))),
        }
    }
}

impl<'a, K, V> Entry<'a, K, OptDatum<V>>
where
    K: Clone,
    V: Serializable + Clone,
{
    pub fn to_option_entry(self) -> Option<Entry<'a, K, V>> {
        match self {
            Self::Ref((k, optdat_v)) => match optdat_v {
                OptDatum::Tombstone => None,
                OptDatum::Some(v) => Some(Entry::Ref((k, v))),
            },
            Self::Own(res) => match res {
                Err(e) => Some(Entry::Own(Err(e))),
                Ok((_k, OptDatum::Tombstone)) => None,
                Ok((k, OptDatum::Some(v))) => Some(Entry::Own(Ok((k, v)))),
            },
        }
    }
}
