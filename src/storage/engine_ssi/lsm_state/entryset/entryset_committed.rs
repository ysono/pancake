use crate::storage::engines_common::{Entry, ReadonlyMemLog, SSTable};
use crate::storage::serde::{Deser, OptDatum};
use std::borrow::Borrow;
use std::iter;

pub enum CommittedEntrySet<K, V> {
    RMemLog(ReadonlyMemLog<K, V>),
    SSTable(SSTable<K, V>),
}

impl<K, V> CommittedEntrySet<K, V>
where
    K: Deser + Ord,
    OptDatum<V>: Deser,
{
    pub fn get_one<'a, Q>(&'a self, k: &'a Q) -> Option<Entry<'a, K, OptDatum<V>>>
    where
        K: Borrow<Q> + PartialOrd<Q>,
        Q: Ord,
    {
        match self {
            Self::RMemLog(r_memlog) => r_memlog.get_one(k).map(Entry::Ref),
            Self::SSTable(sstable) => sstable.get_one(k).map(Entry::Own),
        }
    }

    pub fn get_range<'a, Q>(
        &'a self,
        k_lo: Option<&'a Q>,
        k_hi: Option<&'a Q>,
    ) -> impl Iterator<Item = Entry<'a, K, OptDatum<V>>>
    where
        K: PartialOrd<Q>,
    {
        let mut rml_iter = None;
        let mut sst_iter = None;
        match self {
            Self::RMemLog(r_memlog) => {
                let iter = r_memlog.get_range(k_lo, k_hi).map(Entry::Ref);
                rml_iter = Some(iter);
            }
            Self::SSTable(sstable) => {
                let iter = sstable.get_range(k_lo, k_hi).map(Entry::Own);
                sst_iter = Some(iter);
            }
        }

        let ret_iter_fn = move || -> Option<Entry<K, OptDatum<V>>> {
            if let Some(rml_iter) = rml_iter.as_mut() {
                rml_iter.next()
            } else if let Some(sst_iter) = sst_iter.as_mut() {
                sst_iter.next()
            } else {
                None
            }
        };
        iter::from_fn(ret_iter_fn)
    }
}

impl<K, V> CommittedEntrySet<K, V>
where
    K: Deser + Ord,
{
    pub fn get_all_keys(&self) -> impl Iterator<Item = Entry<K, ()>> {
        let mut rml_iter = None;
        let mut sst_iter = None;
        match self {
            Self::RMemLog(r_memlog) => {
                let iter = r_memlog.memtable.iter().map(|(k, _v)| Entry::Ref((k, &())));
                rml_iter = Some(iter);
            }
            Self::SSTable(sstable) => {
                let iter = sstable
                    .get_all_keys()
                    .map(|res_k| Entry::Own(res_k.map(|k| (k, ()))));
                sst_iter = Some(iter);
            }
        }

        let ret_iter_fn = move || -> Option<Entry<K, ()>> {
            if let Some(rml_iter) = rml_iter.as_mut() {
                rml_iter.next()
            } else if let Some(sst_iter) = sst_iter.as_mut() {
                sst_iter.next()
            } else {
                None
            }
        };
        iter::from_fn(ret_iter_fn)
    }
}
