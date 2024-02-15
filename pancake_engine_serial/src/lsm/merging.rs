use anyhow::Result;
use pancake_engine_common::{merging, Entry, SSTable, WritableMemLog};
use pancake_types::types::Deser;
use std::cmp::{Ord, PartialOrd};

/// @arg sstables: From older to newer. (The *opposite* of the convention in [`pancake_engine_common::merging`].)
pub fn merge_sstables<'a, K, V, Q>(
    sstables: &'a [SSTable<K, V>],
    k_lo: Option<&'a Q>,
    k_hi: Option<&'a Q>,
) -> impl 'a + Iterator<Item = Result<(K, V)>>
where
    K: Deser + Ord + PartialOrd<Q>,
    V: Deser,
{
    let entry_iters = sstables
        .iter()
        .rev()
        .map(move |sst| sst.get_range(k_lo, k_hi));

    merging::merge_entry_iters(entry_iters)
}

/// @arg sstables: From older to newer. (The *opposite* of the convention in [`pancake_engine_common::merging`].)
pub fn merge_memlog_and_sstables<'a, K, V, Q>(
    memlog: &'a WritableMemLog<K, V>,
    sstables: &'a [SSTable<K, V>],
    k_lo: Option<&'a Q>,
    k_hi: Option<&'a Q>,
) -> impl 'a + Iterator<Item = Entry<'a, K, V>>
where
    K: Deser + Ord + PartialOrd<Q>,
    V: Deser,
{
    let memlog_entry_iter = memlog.r_memlog().get_range(k_lo, k_hi);
    let memlog_entry_iter = Some(memlog_entry_iter);

    let sstables_entry_iter = merge_sstables(sstables, k_lo, k_hi).map(Entry::Own);

    merging::merge_differently_typed_entry_iters(memlog_entry_iter, sstables_entry_iter)
}
