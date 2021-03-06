use crate::storage::engines_common::{Entry, SSTable, WritableMemLog};
use crate::storage::serde::{Deser, OptDatum};
use anyhow::Result;
use itertools::Itertools;
use std::cmp::{Ord, Ordering, PartialOrd};
use std::iter;

/// @arg sstables: From older to newer.
pub fn merge_sstables<'a, K, V, Q>(
    sstables: &'a [SSTable<K, V>],
    k_lo: Option<&'a Q>,
    k_hi: Option<&'a Q>,
) -> impl 'a + Iterator<Item = Result<(K, OptDatum<V>)>>
where
    K: Deser + Ord + PartialOrd<Q>,
    OptDatum<V>: Deser,
{
    let iter_of_iters = sstables.iter().enumerate().map(|(sst_i, sst)| {
        // NB: the index/position of the sstable is included for the purpose of breaking ties
        // on duplicate keys.
        sst.get_range(k_lo, k_hi).zip(iter::repeat(sst_i))
    });

    let merged_entries = iter_of_iters.kmerge_by(|(a_res_kv, a_i), (b_res_kv, b_i)| {
        /*
        the comparator contract dictates we return true iff |a| is ordered before |b|
            or said differently: |a| < |b|.

        for equal keys, we define |a| < |b| iff |a| is more recent.
            note: |a| is more recent when index_a > index_b.

        by defining the ordering in this way,
            we only keep the first instance of key |k| in the compacted iterator.
            duplicate items with key |k| must be discarded.

        In case of any error, mark it as the lesser item, for early termination.
         */
        match (a_res_kv, b_res_kv) {
            (Err(_), _) => return true,
            (_, Err(_)) => return false,
            (Ok((a_k, _)), Ok((b_k, _))) => {
                let key_cmp = a_k.cmp(b_k);
                if key_cmp.is_eq() {
                    // Larger `i` means newer.
                    return a_i > b_i;
                } else {
                    return key_cmp.is_lt();
                }
            }
        }
    });

    let deduped_entries =
        merged_entries.dedup_by(|(a_res_kv, _), (b_res_kv, _)| match (a_res_kv, b_res_kv) {
            (Err(_), _) => return false,
            (_, Err(_)) => return false,
            (Ok((a_k, _)), Ok((b_k, _))) => return a_k == b_k,
        });

    let entries = deduped_entries.map(|(res_kv, _i)| res_kv);

    entries
}

pub fn merge_memlog_and_sstables<'a, K, V, Q>(
    memlog: &'a WritableMemLog<K, V>,
    sstables: &'a [SSTable<K, V>],
    k_lo: Option<&'a Q>,
    k_hi: Option<&'a Q>,
) -> impl 'a + Iterator<Item = Entry<'a, K, OptDatum<V>>>
where
    K: Deser + Ord + PartialOrd<Q>,
    OptDatum<V>: Deser,
{
    let mut mt_iter = memlog.r_memlog().get_range(k_lo, k_hi).peekable();
    let mut ssts_iter = merge_sstables(sstables, k_lo, k_hi).peekable();

    /*
    K-merge manually due to type difference.
    Memtable iterator item = (&K, &V)
    SSTable iterator item = Result<(K, V)>
    */
    let ret_iter_fn = move || -> Option<Entry<K, OptDatum<V>>> {
        let mt_cmp_sst = match (mt_iter.peek(), ssts_iter.peek()) {
            (None, None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (Some(_), None) => Ordering::Less,
            (Some(_), Some(Err(_))) => Ordering::Greater,
            (Some((mt_k, _)), Some(Ok((sst_k, _)))) => mt_k.cmp(&sst_k),
        };
        match mt_cmp_sst {
            Ordering::Less => mt_iter.next().map(Entry::Ref),
            Ordering::Greater => ssts_iter.next().map(Entry::Own),
            Ordering::Equal => {
                ssts_iter.next();
                mt_iter.next().map(Entry::Ref)
            }
        }
    };
    iter::from_fn(ret_iter_fn)
}
