use crate::lsm::entryset::CommittedEntrySet;
use itertools::Itertools;
use pancake_engine_common::{Entry, WritableMemLog};
use pancake_types::types::Deser;
use std::cmp::{Ord, Ordering, PartialOrd};
use std::iter;

/// @arg entrysets: newer to older.
pub fn merge_committed_entrysets<'a, K, V, Q>(
    entrysets: impl Iterator<Item = &'a CommittedEntrySet<K, V>>,
    k_lo: Option<&'a Q>,
    k_hi: Option<&'a Q>,
) -> impl 'a + Iterator<Item = Entry<'a, K, V>>
where
    K: 'a + Deser + Ord + PartialOrd<Q>,
    V: 'a + Deser,
{
    let entries_iters = entrysets
        .enumerate()
        .map(|(entryset_i, entryset)| entryset.get_range(k_lo, k_hi).zip(iter::repeat(entryset_i)));

    let merged_entries = entries_iters.kmerge_by(|(a_entry, a_es_i), (b_entry, b_es_i)| {
        let a_res_kv = a_entry.try_borrow();
        let b_res_kv = b_entry.try_borrow();

        match (a_res_kv, b_res_kv) {
            (Err(_), _) => return true,
            (_, Err(_)) => return false,
            (Ok((a_k, _)), Ok((b_k, _))) => {
                let key_cmp = a_k.cmp(b_k);
                if key_cmp.is_eq() {
                    // Smaller `i` means newer.
                    return a_es_i < b_es_i;
                } else {
                    return key_cmp.is_lt();
                }
            }
        }
    });

    let deduped_entries = merged_entries.dedup_by(|(a_entry, _), (b_entry, _)| {
        let a_res_kv = a_entry.try_borrow();
        let b_res_kv = b_entry.try_borrow();

        match (a_res_kv, b_res_kv) {
            (Err(_), _) => return false,
            (_, Err(_)) => return false,
            (Ok((a_k, _)), Ok((b_k, _))) => {
                return a_k == b_k;
            }
        }
    });

    let entries = deduped_entries.map(|(entry, _)| entry);

    entries
}

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
    let mut s_entries = staging.map(|w_memlog| w_memlog.r_memlog().get_range(k_lo, k_hi));
    let s_entries = iter::from_fn(move || match s_entries.as_mut() {
        None => None,
        Some(iter) => iter.next(),
    });
    let mut s_entries = s_entries.peekable();

    let mut c_entries = merge_committed_entrysets(committed_entrysets, k_lo, k_hi).peekable();

    /* K-merge manually due to type difference. */
    let ret_iter_fn = move || -> Option<Entry<K, V>> {
        let s_cmp_c = match (s_entries.peek(), c_entries.peek()) {
            (None, None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (Some(_), None) => Ordering::Less,
            (Some((s_k, _)), Some(c_entry)) => match c_entry.try_borrow() {
                Err(_) => Ordering::Greater,
                Ok((c_k, _)) => s_k.cmp(&c_k),
            },
        };
        match s_cmp_c {
            Ordering::Less => return s_entries.next().map(Entry::Ref),
            Ordering::Greater => return c_entries.next(),
            Ordering::Equal => {
                c_entries.next();
                return s_entries.next().map(Entry::Ref);
            }
        }
    };
    iter::from_fn(ret_iter_fn)
}
