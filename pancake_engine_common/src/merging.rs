use crate::entry::Entry;
use anyhow::anyhow;
use itertools::Itertools;
use std::cmp::Ordering;
use std::iter;

pub trait Mergeable<K> {
    fn try_borrow<'a>(&'a self) -> Result<&'a K, anyhow::Error>;
}
impl<K, V> Mergeable<K> for Result<(K, V), anyhow::Error> {
    fn try_borrow<'a>(&'a self) -> Result<&'a K, anyhow::Error> {
        self.as_ref()
            .map_err(|e| anyhow!(e.to_string()))
            .map(|(k, _v)| k)
    }
}
impl<K, V> Mergeable<K> for Entry<'_, K, V> {
    fn try_borrow<'a>(&'a self) -> Result<&'a K, anyhow::Error> {
        self.try_borrow().map(|(k, _v)| k)
    }
}

/// K-merges and then dedupes the arg iters.
///
/// @arg entry_iters: An iterator of iterators of entrysets,
///     where each entryset contains borrowable `K`s and is internally sorted by `K`,
///     from newer entryset to older entryset.
pub fn merge_entry_iters<'a, EntIter, Ent, K>(
    entry_iters: impl Iterator<Item = EntIter>,
) -> impl 'a + Iterator<Item = Ent>
where
    EntIter: 'a + Iterator<Item = Ent>,
    Ent: 'a + Mergeable<K>,
    K: 'a + Ord,
{
    let entry_age_iters = entry_iters.enumerate().map(|(entryset_age, entry_iter)| {
        // NB: The index/position of the entryset is included for the purpose of breaking ties
        // on duplicate keys.
        entry_iter.zip(iter::repeat(entryset_age))
    });

    let merged_entry_age_iter = entry_age_iters.kmerge_by(|(a_entry, a_age), (b_entry, b_age)| {
        /*
        The comparator contract dictates we return true iff |a| is ordered before |b|
            or said differently: |a| < |b|.

        For equal keys, we define |a| < |b| iff |a| is more recent,
            i.e. iff a_age < b_age.

        In case either |a| or |b| is error, we mark it as the lesser item, for early detection.
        */

        let a_res_kv = a_entry.try_borrow();
        let b_res_kv = b_entry.try_borrow();

        match (a_res_kv, b_res_kv) {
            (Err(_), _) => return true,
            (_, Err(_)) => return false,
            (Ok(a_k), Ok(b_k)) => {
                let key_cmp = a_k.cmp(b_k);
                if key_cmp.is_eq() {
                    return a_age < b_age; // Smaller age is newer.
                } else {
                    return key_cmp.is_lt();
                }
            }
        }
    });

    let merged_entry_iter = merged_entry_age_iter.map(|(entry, _)| entry);

    let deduped_entry_iter = merged_entry_iter.dedup_by(|a_entry, b_entry| {
        let a_res_kv = a_entry.try_borrow();
        let b_res_kv = b_entry.try_borrow();

        match (a_res_kv, b_res_kv) {
            (Err(_), _) | (_, Err(_)) => return false,
            (Ok(a_k), Ok(b_k)) => return a_k.eq(b_k),
        }
    });

    deduped_entry_iter
}

pub fn merge_differently_typed_entry_iters<'a, K, V>(
    mut entry_iter_newer: Option<impl 'a + Iterator<Item = (&'a K, &'a V)>>,
    entry_iter_older: impl 'a + Iterator<Item = Entry<'a, K, V>>,
) -> impl 'a + Iterator<Item = Entry<'a, K, V>>
where
    K: 'a + Ord,
    V: 'a,
{
    let entry_iter_newer = iter::from_fn(move || -> Option<(&'a K, &'a V)> {
        match entry_iter_newer.as_mut() {
            None => None,
            Some(iter) => iter.next(),
        }
    });

    let mut entry_iter_newer = entry_iter_newer.peekable();
    let mut entry_iter_older = entry_iter_older.peekable();

    let iter_fn = move || -> Option<Entry<'a, K, V>> {
        let newer_cmp_older = match (entry_iter_newer.peek(), entry_iter_older.peek()) {
            (_, None) => Ordering::Less,
            (None, _) => Ordering::Greater,
            (Some((newer_k, _newer_v)), Some(entry_older)) => match entry_older.try_borrow() {
                Err(_) => Ordering::Greater,
                Ok((older_k, _older_v)) => newer_k.cmp(&older_k),
            },
        };
        match newer_cmp_older {
            Ordering::Less => return entry_iter_newer.next().map(Entry::Ref),
            Ordering::Greater => return entry_iter_older.next(),
            Ordering::Equal => {
                entry_iter_older.next();
                return entry_iter_newer.next().map(Entry::Ref);
            }
        }
    };
    iter::from_fn(iter_fn)
}
