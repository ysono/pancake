use crate::ds_n_a::atomic_linked_list::ListElem;
use crate::storage::engine_ssi::container::LSMTree;
use crate::storage::engine_ssi::entryset::{
    CommitVer, CommittedEntrySet, ReadonlyMemLog, WritableMemLog,
};
use crate::storage::serde::{OptDatum, Serializable};
use anyhow::Result;
use std::cmp::Ord;

impl<'a, K, V> LSMTree<K, V>
where
    K: Serializable + Ord + Clone,
    OptDatum<V>: Serializable,
{
    pub async fn create_writable_entryset(&self) -> Result<WritableMemLog<K, V>> {
        let es_path = self.format_new_entryset_dir_path();
        WritableMemLog::new(es_path)
    }

    pub async fn commit(
        &self,
        w_memlog: WritableMemLog<K, V>,
        commit_ver: CommitVer,
    ) -> Result<()> {
        let r_memlog = ReadonlyMemLog::from(w_memlog, commit_ver)?;
        let elem = ListElem::Elem(CommittedEntrySet::RMemLog(r_memlog));
        self.list.push_newest(elem);
        Ok(())
    }
}
