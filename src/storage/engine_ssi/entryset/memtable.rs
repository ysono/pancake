use crate::storage::serde::OptDatum;
use derive_more::{Deref, DerefMut, From};
use std::cmp::Ordering;
use std::collections::BTreeMap;

#[derive(From, Deref, DerefMut)]
pub struct Memtable<K, V>(BTreeMap<K, OptDatum<V>>);

impl<K, V> Memtable<K, V> {
    pub fn get_range<'a, Q>(
        &'a self,
        k_lo: Option<&'a Q>,
        k_hi: Option<&'a Q>,
    ) -> impl 'a + Iterator<Item = (&'a K, &'a OptDatum<V>)>
    where
        K: PartialOrd<Q>,
    {
        /*
        The intent here is to first search for tree node in O(log(n)), then iterate from there.
        If this is not possible, then iterating twice in this fashion is obviously wasteful.
        */
        let mut iter = self.iter();
        if let Some(k_lo) = k_lo {
            /*
            Find the max key less than the desired key. Not equal to it, b/c
                `.nth()` takes the item at the provided position.
            */
            if let Some(iter_pos) = self.iter().rposition(|(sample_k, _v)| {
                sample_k
                    .partial_cmp(k_lo)
                    .unwrap_or(Ordering::Greater)
                    .is_lt()
            }) {
                iter.nth(iter_pos);
            }
        }

        iter.take_while(move |(sample_k, _v)| {
            // This closure moves k_hi.
            if let Some(k_hi) = k_hi {
                return sample_k
                    .partial_cmp(&k_hi)
                    .unwrap_or(Ordering::Less)
                    .is_le();
            }
            true
        })
    }
}
