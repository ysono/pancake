use crate::storage::engine_ssi::entryset::{CommittedEntrySet, WritableMemLog};
use crate::storage::engines_common::Entry;
use crate::storage::serde::{Deser, OptDatum};
use itertools::Itertools;
use std::cmp::Ordering;
use std::iter;

pub fn merge_committed_entrysets<'a, K, V, Q>(
    entrysets: impl Iterator<Item = &'a CommittedEntrySet<K, V>>,
    k_lo: Option<&'a Q>,
    k_hi: Option<&'a Q>,
) -> impl 'a + Iterator<Item = Entry<'a, K, OptDatum<V>>>
where
    K: 'a + Deser + Ord + PartialOrd<Q> + Clone,
    OptDatum<V>: 'a + Deser,
{
    let entries_iters = entrysets.map(|entryset| {
        entryset
            .get_range(k_lo, k_hi)
            .zip(iter::repeat(entryset.info().commit_info.commit_ver_hi_incl))
    });

    let merged_entries = entries_iters.kmerge_by(|(a_entry, a_cmt_ver), (b_entry, b_cmt_ver)| {
        let a_res_kv = a_entry.try_borrow();
        let b_res_kv = b_entry.try_borrow();

        match (a_res_kv, b_res_kv) {
            (Err(_), _) => return true,
            (_, Err(_)) => return false,
            (Ok((a_k, _a_v)), Ok((b_k, _b_v))) => {
                let key_cmp = a_k.cmp(b_k);
                if key_cmp.is_eq() {
                    a_cmt_ver > b_cmt_ver
                } else {
                    return key_cmp.is_lt();
                }
            }
        }
    });

    // Manually implement unique_by(k).
    let mut prev_key: Option<K> = None;
    let deduped_entries =
        merged_entries.filter(move |(entry, _cmt_ver)| match entry.try_borrow() {
            Err(_) => return true,
            Ok((k, _)) => match prev_key.as_ref() {
                Some(prv_k) if prv_k == k => return false,
                _ => {
                    prev_key = Some(k.clone());
                    return true;
                }
            },
        });

    let entries = deduped_entries.map(|(entry, _cmt_ver)| entry);

    entries
}

pub fn merge_txnlocal_and_committed_entrysets<'a, K, V, Q>(
    written: Option<&'a WritableMemLog<K, V>>,
    committed_entrysets: impl 'a + Iterator<Item = &'a CommittedEntrySet<K, V>>,
    k_lo: Option<&'a Q>,
    k_hi: Option<&'a Q>,
) -> impl 'a + Iterator<Item = Entry<'a, K, OptDatum<V>>>
where
    K: 'a + Deser + Ord + PartialOrd<Q> + Clone,
    OptDatum<V>: 'a + Deser,
{
    let mut w_entries = written.map(|w_memlog| w_memlog.memtable.get_range(k_lo, k_hi));
    let w_entries = iter::from_fn(move || match w_entries.as_mut() {
        None => None,
        Some(iter) => iter.next(),
    });
    let mut w_entries = w_entries.peekable();

    let mut c_entries = merge_committed_entrysets(committed_entrysets, k_lo, k_hi).peekable();

    /* K-merge manually due to type difference. */
    let ret_iter_fn = move || -> Option<Entry<K, OptDatum<V>>> {
        let w_cmp_c = match (w_entries.peek(), c_entries.peek()) {
            (None, None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (Some(_), None) => Ordering::Less,
            (Some((w_k, _)), Some(c_entry)) => match c_entry.try_borrow() {
                Err(_) => Ordering::Greater,
                Ok((c_k, _)) => w_k.cmp(&c_k),
            },
        };
        match w_cmp_c {
            Ordering::Less => return w_entries.next().map(Entry::Ref),
            Ordering::Greater => return c_entries.next(),
            Ordering::Equal => {
                c_entries.next();
                return w_entries.next().map(Entry::Ref);
            }
        }
    };
    iter::from_fn(ret_iter_fn)
}
