use std::collections::HashMap;
use std::hash::Hash;

struct DictValue<K, V> {
    newer_k: Option<K>,
    v: V,
    older_k: Option<K>,
}

/// A structure that contains
/// - a hash-map and
/// - a virtual linked-list connecting neighboring entries in their insertion order.
pub struct OrderedDict<K, V> {
    dict: HashMap<K, DictValue<K, V>>,
    newest_k: Option<K>,
}

impl<K, V> OrderedDict<K, V>
where
    K: Hash + Eq + Clone,
{
    pub fn new() -> Self {
        Self {
            dict: Default::default(),
            newest_k: None,
        }
    }

    pub fn get<'s, 'a>(&'s self, k: &'a K) -> Option<&'s V> {
        self.dict.get(k).map(|dv| &dv.v)
    }

    pub fn get_mut<'s, 'a>(&'s mut self, k: &'a K) -> Option<&'s mut V> {
        self.dict.get_mut(k).map(|dv| &mut dv.v)
    }

    pub fn get_newest_key(&self) -> Option<&K> {
        self.newest_k.as_ref()
    }

    pub fn get_neighbors<'s, 'a>(&'s self, k: &'a K) -> Option<Neighbors<'s, K, V>> {
        self.dict.get(k).map(|q_dv| {
            let newer_k_v = q_dv.newer_k.as_ref().map(|k| {
                let newer_dv = self.dict.get(k).unwrap();
                (k, &newer_dv.v)
            });
            let older_k_v = q_dv.older_k.as_ref().map(|k| {
                let older_dv = self.dict.get(k).unwrap();
                (k, &older_dv.v)
            });
            Neighbors {
                newer: newer_k_v,
                older: older_k_v,
            }
        })
    }

    pub fn insert(&mut self, k: K, v: V) {
        self.remove(&k);

        if let Some(prev_k) = self.newest_k.as_ref() {
            let prev_dv = self.dict.get_mut(prev_k).unwrap();
            prev_dv.newer_k = Some(k.clone());
        }

        let dv = DictValue {
            newer_k: None,
            v,
            older_k: self.newest_k.take(),
        };
        self.dict.insert(k.clone(), dv);

        self.newest_k = Some(k);
    }

    /// ```text
    ///            / _k
    /// newer_k -- | newer_v
    ///            \ rm_k  -->  older_k
    ///
    ///            / newer_k  -->  None
    /// rm_k ----- | rm_v
    ///            \ older_k  -->  None
    ///
    ///            / rm_k  -->  newer_k
    /// older_k -- | older_v
    ///            \ _k
    /// ```
    pub fn remove<'s, 'a>(&'s mut self, k: &'a K) -> Option<RemovalResult<'s, K, V>>
    where
        K: Sized,
    {
        self.dict.remove(k).map(|rm_dv| {
            if self.newest_k.as_ref() == Some(k) {
                self.newest_k = rm_dv.older_k.clone();
            }

            let newer_k_v = rm_dv.newer_k.as_ref().map(|newer_k| {
                let (newer_k, newer_dv) = self.dict.get_key_value(newer_k).unwrap();
                #[allow(invalid_reference_casting)]
                let newer_dv_mut =
                    unsafe { &mut *((newer_dv as *const DictValue<K, V>).cast_mut()) };
                newer_dv_mut.older_k = rm_dv.older_k.clone();
                (newer_k, &newer_dv.v)
            });
            let older_k_v = rm_dv.older_k.as_ref().map(|older_k| {
                let (older_k, older_dv) = self.dict.get_key_value(older_k).unwrap();
                #[allow(invalid_reference_casting)]
                let older_dv_mut =
                    unsafe { &mut *((older_dv as *const DictValue<K, V>).cast_mut()) };
                older_dv_mut.newer_k = rm_dv.newer_k.clone();
                (older_k, &older_dv.v)
            });

            RemovalResult {
                removed_v: rm_dv.v,
                neighbors: Neighbors {
                    newer: newer_k_v,
                    older: older_k_v,
                },
            }
        })
    }
}

pub struct Neighbors<'s, K, V> {
    pub newer: Option<(&'s K, &'s V)>,
    pub older: Option<(&'s K, &'s V)>,
}

// #[derive(Debug)]
pub struct RemovalResult<'s, K, V> {
    pub removed_v: V,
    pub neighbors: Neighbors<'s, K, V>,
}

#[cfg(test)]
mod test {
    use super::*;
    use itertools::Itertools;
    use std::collections::VecDeque;

    const KEY_OFFSET: i16 = 100;
    const VALUE_OFFSET: i16 = 200;

    #[test]
    fn test() {
        let len = 5;
        for removal_idxs in (0..len).permutations(len) {
            push_then_remove(removal_idxs);
        }
    }

    fn push_then_remove(removal_idxs: Vec<usize>) {
        let len = removal_idxs.len();

        let nonexistent_k = KEY_OFFSET + len as i16;

        /* Initiate the expected and actual collections as empty. */
        let mut vd = VecDeque::new();
        let mut od = OrderedDict::new();

        /* Verify the empty state. */
        assert_eq!(None, od.get_newest_key());
        assert_elems(&vd, &od);
        assert!(od.get(&nonexistent_k).is_none());
        assert!(od.remove(&nonexistent_k).is_none());

        /* Populate the expected and actual collections. */
        for i in 0..(len as i16) {
            let k = KEY_OFFSET + i;
            let v = VALUE_OFFSET + i;
            vd.push_back(Some((k, v)));
            od.insert(k, v);

            /* Verify the post-insertion state. */
            assert_eq!(Some(&k), od.get_newest_key());
            assert_eq!(Some(&v), od.get(&k));
            assert_elems(&vd, &od);
            assert!(od.get(&nonexistent_k).is_none());
            assert!(od.remove(&nonexistent_k).is_none());
        }

        /* Re-insert already-existing keys, from oldest-inserted to newest-inserted. */
        for _ in 0..len {
            let entry = vd.pop_front().unwrap();
            let (k, mut v) = entry.unwrap();
            v += VALUE_OFFSET;
            vd.push_back(Some((k, v)));
            od.insert(k, v);

            /* Verify the post-insertion state. */
            assert_eq!(Some(&k), od.get_newest_key());
            assert_eq!(Some(&v), od.get(&k));
            assert_elems(&vd, &od);
            assert!(od.get(&nonexistent_k).is_none());
            assert!(od.remove(&nonexistent_k).is_none());
        }

        /* Re-insert already-existing keys, from newest-inserted to oldest-inserted. */
        for idx in 0..len {
            let entry = vd.remove(idx).unwrap();
            let (k, mut v) = entry.unwrap();
            v += VALUE_OFFSET;
            vd.push_back(Some((k, v)));
            od.insert(k, v);

            /* Verify the post-insertion state. */
            assert_eq!(Some(&k), od.get_newest_key());
            assert_eq!(Some(&v), od.get(&k));
            assert_elems(&vd, &od);
            assert!(od.get(&nonexistent_k).is_none());
            assert!(od.remove(&nonexistent_k).is_none());
        }

        /* Remove all entries, in the arg-specified order. */
        for rm_idx in removal_idxs.into_iter() {
            /* Remove one entry. */
            let (rm_k, rm_v) = vd[rm_idx].take().unwrap();
            let rm_res = od.remove(&rm_k).unwrap();
            let RemovalResult {
                removed_v,
                neighbors: Neighbors { newer, older },
            } = rm_res;
            let act_newer = newer.map(|(k, v)| (*k, *v));
            let act_older = older.map(|(k, v)| (*k, *v));

            let exp_newer = vd
                .iter()
                .skip(rm_idx + 1)
                .filter_map(|opt| opt.clone())
                .next();
            let exp_older = vd
                .iter()
                .take(rm_idx)
                .rev()
                .filter_map(|opt| opt.clone())
                .next();

            /* Verify the post-removal state. */
            assert_eq!(exp_newer, act_newer);
            assert_eq!(exp_older, act_older);
            assert_eq!(rm_v, removed_v);
            assert_elems(&vd, &od);
        }
    }

    fn assert_elems(exp: &VecDeque<Option<(i16, i16)>>, od: &OrderedDict<i16, i16>) {
        /* Traverse old-ward. */
        let mut act_kv_oldward = vec![];
        let mut curr_k = od.newest_k.as_ref();
        while let Some(act_k) = curr_k {
            match od.dict.get(act_k) {
                Some(dv) => {
                    act_kv_oldward.push((*act_k, dv.v));
                    curr_k = dv.older_k.as_ref();
                }
                None => break,
            }
        }
        let mut exp_remaining = exp
            .iter()
            .rev()
            .filter_map(|opt| opt.clone())
            .collect::<Vec<_>>();
        assert_eq!(&exp_remaining, &act_kv_oldward);

        /* Traverse new-ward. */
        let mut act_kv_newward = vec![];
        let mut curr_k = exp
            .iter()
            .filter_map(|opt| opt.as_ref())
            .map(|(k, _)| k)
            .next();
        while let Some(act_k) = curr_k {
            match od.dict.get(act_k) {
                Some(dv) => {
                    act_kv_newward.push((*act_k, dv.v));
                    curr_k = dv.newer_k.as_ref();
                }
                None => break,
            }
        }
        exp_remaining.reverse();
        assert_eq!(exp_remaining, act_kv_newward);
    }
}
