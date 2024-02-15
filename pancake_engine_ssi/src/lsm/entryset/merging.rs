use crate::lsm::entryset::CommittedEntrySet;
use pancake_engine_common::{merging, Entry, WritableMemLog};
use pancake_types::types::Deser;
use std::cmp::{Ord, PartialOrd};

/// @arg entrysets: From newer to older. (Same as the convention in [`pancake_engine_common::merging`].)
pub fn merge_committed_entrysets<'a, K, V, Q>(
    entrysets: impl Iterator<Item = &'a CommittedEntrySet<K, V>>,
    k_lo: Option<&'a Q>,
    k_hi: Option<&'a Q>,
) -> impl 'a + Iterator<Item = Entry<'a, K, V>>
where
    K: 'a + Deser + Ord + PartialOrd<Q>,
    V: 'a + Deser,
{
    let entry_iters = entrysets.map(move |entryset| entryset.get_range(k_lo, k_hi));

    merging::merge_entry_iters(entry_iters)
}

/// @arg entrysets: From newer to older. (Same as the convention in [`pancake_engine_common::merging`].)
pub fn merge_txnlocal_and_committed_entrysets<'a, K, V, Q>(
    staging: Option<&'a WritableMemLog<K, V>>,
    committed_entrysets: impl 'a + Iterator<Item = &'a CommittedEntrySet<K, V>>,
    k_lo: Option<&'a Q>,
    k_hi: Option<&'a Q>,
) -> impl 'a + Iterator<Item = Entry<'a, K, V>>
where
    K: 'a + Deser + Ord + PartialOrd<Q>,
    V: 'a + Deser,
{
    let staging_entry_iter = staging.map(|w_memlog| w_memlog.r_memlog().get_range(k_lo, k_hi));

    let committed_entry_iter = merge_committed_entrysets(committed_entrysets, k_lo, k_hi);

    merging::merge_differently_typed_entry_iters(staging_entry_iter, committed_entry_iter)
}
