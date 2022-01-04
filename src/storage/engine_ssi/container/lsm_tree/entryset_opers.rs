use crate::ds_n_a::atomic_linked_list::ListElem;
use crate::storage::engine_ssi::container::LSMTree;
use crate::storage::engine_ssi::entryset::{
    CommitVer, CommittedEntrySet, EntrySetDir, ReadonlyMemLog, WritableMemLog,
};
use crate::storage::serde::{OptDatum, Serializable};
use anyhow::Result;
use std::cmp::Ord;
use std::path::Path;

const ENTRYSET_DIR_NAME_PFX: &str = "entryset-";

impl<'a, K, V> LSMTree<K, V>
where
    K: Serializable + Ord + Clone,
    OptDatum<V>: Serializable,
{
    pub async fn create_writable_entryset(&self) -> Result<WritableMemLog<K, V>> {
        let es_path = self.format_new_entryset_dir_path().await?;
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

    /// Formats dir path but does *not* create the dir.
    pub async fn format_new_entryset_dir_path(&self) -> Result<EntrySetDir> {
        let uniq_id = {
            let mut uniq_id_gen = self.unique_id.lock().await;
            uniq_id_gen.get_and_inc()?
        };

        let dirname = format!("{}{}", ENTRYSET_DIR_NAME_PFX, *uniq_id);
        let path = self.lsm_dir_path.join(dirname);

        Ok(EntrySetDir::from(path))
    }

    pub(super) fn is_entryset_dir<P: AsRef<Path>>(path: P) -> bool {
        path.as_ref().starts_with(ENTRYSET_DIR_NAME_PFX)
    }
}
