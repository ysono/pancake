use crate::ds_n_a::atomic_linked_list::ListElem;
use crate::storage::engine_ssi::container::LSMTree;
use crate::storage::engine_ssi::entryset::{merging, CommitVer, CommittedEntrySet, WritableMemLog};
use crate::storage::engines_common::Entry;
use crate::storage::serde::{OptDatum, Serializable};
use anyhow::Result;
use std::borrow::Borrow;

impl<'a, K, V> LSMTree<K, V>
where
    K: Serializable + Ord + Clone,
    OptDatum<V>: Serializable,
    V: Clone,
{
    /// Unlike other "get" operations, we choose to return not `Entry` but owned `(K, V)`.
    /// This is purely for developer convenience: giving client access to Entry would require client
    /// to provide a callback that reads an Entry, which would complicates the syntax.
    pub async fn get_one<Q, CbAdv>(
        &'a self,
        written: Option<&'a WritableMemLog<K, V>>,
        commit_ver_hi_excl: Option<CommitVer>,
        commit_ver_lo_incl: Option<CommitVer>,
        k: &'a Q,
        on_trailing_list_ver_advanced: CbAdv,
    ) -> Result<Option<(K, V)>>
    where
        K: Borrow<Q> + PartialOrd<Q>,
        Q: Ord,
        CbAdv: FnOnce(),
    {
        let opt_res_kv: Option<Result<(K, V)>> = async {
            if let Some(written) = written {
                if let Some(kv) = written.memtable.get_key_value(k) {
                    return Entry::Ref(kv)
                        .to_option_entry()
                        .map(|entry| entry.take_kv());
                }
            }

            let list_ver = self.list_ver_state.hold_leading().await;

            let mut entrysets = self.do_iter_entrysets(commit_ver_hi_excl, commit_ver_lo_incl);
            let maybe_found = entrysets
                .find_map(|entryset| entryset.get_one(k))
                .and_then(|entry| entry.to_option_entry())
                .map(|entry| entry.take_kv());

            self.list_ver_state
                .unhold(list_ver, on_trailing_list_ver_advanced)
                .await;

            maybe_found
        }
        .await;
        opt_res_kv.transpose()
    }
}

impl<'a, K, V> LSMTree<K, V>
where
    K: Serializable + Ord + Clone,
    OptDatum<V>: Serializable,
{
    pub async fn get_range<Q, CbEnt, CbEntRet, CbAdv>(
        &'a self,
        written: Option<&'a WritableMemLog<K, V>>,
        commit_ver_hi_excl: Option<CommitVer>,
        commit_ver_lo_incl: Option<CommitVer>,
        k_lo: Option<&'a Q>,
        k_hi: Option<&'a Q>,
        handle_entries: CbEnt,
        on_trailing_list_ver_advanced: CbAdv,
    ) -> CbEntRet
    where
        K: PartialOrd<Q>,
        CbEnt: for<'cb> FnOnce(&'cb mut (dyn 'cb + Iterator<Item = Entry<'a, K, V>>)) -> CbEntRet,
        CbAdv: FnOnce(),
    {
        let list_ver = self.list_ver_state.hold_leading().await;

        let committed_entrysets = self.do_iter_entrysets(commit_ver_hi_excl, commit_ver_lo_incl);

        let mut entries = merging::merge_txnlocal_and_committed_entrysets(
            written,
            committed_entrysets,
            k_lo,
            k_hi,
        )
        .filter_map(|entry| entry.to_option_entry());

        let cb_ent_ret = handle_entries(&mut entries);

        self.list_ver_state
            .unhold(list_ver, on_trailing_list_ver_advanced)
            .await;

        cb_ent_ret
    }

    pub async fn iter_entrysets<CbEnt, CbEntRet, CbAdv>(
        &'a self,
        commit_ver_hi_excl: Option<CommitVer>,
        commit_ver_lo_incl: Option<CommitVer>,
        handle_entrysets: CbEnt,
        on_trailing_list_ver_advanced: CbAdv,
    ) -> CbEntRet
    where
        CbEnt: for<'cb> FnOnce(
            &'cb mut (dyn 'cb + Iterator<Item = &'a CommittedEntrySet<K, V>>),
        ) -> CbEntRet,
        CbAdv: Fn(),
    {
        let list_ver = self.list_ver_state.hold_leading().await;

        let mut entrysets = self.do_iter_entrysets(commit_ver_hi_excl, commit_ver_lo_incl);
        let cb_ent_ret = handle_entrysets(&mut entrysets);

        self.list_ver_state
            .unhold(list_ver, on_trailing_list_ver_advanced)
            .await;

        cb_ent_ret
    }

    fn do_iter_entrysets(
        &'a self,
        commit_ver_hi_excl: Option<CommitVer>,
        commit_ver_lo_incl: Option<CommitVer>,
    ) -> impl 'a + Iterator<Item = &'a CommittedEntrySet<K, V>> {
        self.list
            .iter()
            .filter_map(|elem| match elem {
                ListElem::Dummy { .. } => None,
                ListElem::Elem(entryset) => Some(entryset),
            })
            .skip_while(move |entryset| match commit_ver_hi_excl {
                None => false,
                Some(lim_hi) => entryset.info().commit_info.commit_ver_lo_incl >= lim_hi,
            })
            .take_while(move |entryset| match commit_ver_lo_incl {
                None => true,
                Some(lim_lo) => entryset.info().commit_info.commit_ver_hi_incl >= lim_lo,
            })
    }
}
