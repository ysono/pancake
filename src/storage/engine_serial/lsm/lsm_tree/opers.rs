use super::LSMTree;
use crate::storage::engine_serial::lsm::merging;
use crate::storage::engines_common::Entry;
use crate::storage::serde::{OptDatum, Serializable};
use anyhow::Result;
use std::borrow::Borrow;

impl<K, V> LSMTree<K, V>
where
    K: Serializable + Ord + Clone,
    OptDatum<V>: Serializable,
{
    pub fn put(&mut self, k: K, v: Option<V>) -> Result<()> {
        let v = OptDatum::from(v);
        self.memlog.put(k, v)?;

        self.maybe_run_gc()?;

        Ok(())
    }

    pub fn get_one<'a, Q>(&'a self, k: &'a Q) -> Option<Entry<'a, K, V>>
    where
        K: Borrow<Q> + PartialOrd<Q>,
        Q: Ord,
    {
        if let Some(kv) = self.memlog.get_one(k) {
            return Entry::Ref(kv).to_option_entry();
        }
        // TODO bloom filter here
        if let Some(res) = self.sstables.iter().rev().find_map(|sst| sst.get_one(k)) {
            return Entry::Own(res).to_option_entry();
        }
        None
    }

    pub fn get_range<'a, Q>(
        &'a self,
        k_lo: Option<&'a Q>,
        k_hi: Option<&'a Q>,
    ) -> impl 'a + Iterator<Item = Entry<'a, K, V>>
    where
        K: PartialOrd<Q>,
    {
        merging::merge_memlog_and_sstables(&self.memlog, &self.sstables[..], k_lo, k_hi)
            .filter_map(|entry| entry.to_option_entry())
    }

    pub fn get_whole_range<'a>(&'a self) -> impl 'a + Iterator<Item = Entry<'a, K, V>> {
        self.get_range(None, None)
    }
}