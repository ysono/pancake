use crate::ds_n_a::atomic_linked_list::{ListElem, ListNode};
use crate::ds_n_a::send_ptr::SendPtr;
use crate::storage::engine_ssi::container::LSMTree;
use crate::storage::engine_ssi::entryset::{
    merging, CommitInfo, CommitVer, CommittedEntrySet, CommittedEntrySetInfo, SSTable, Timestamp,
};
use crate::storage::serde::{OptDatum, Serializable};
use anyhow::Result;
use std::sync::atomic::Ordering;

impl<'a, K, V> LSMTree<K, V>
where
    K: Serializable + Ord + Clone,
    OptDatum<V>: Serializable,
{
    /// The main entrypoint for GC's modification of the internal linked list.
    pub async fn modify_linked_list(&self, min_separate_commit_ver: CommitVer) -> Result<()> {
        let k_elem = ListElem::new_dummy(false);
        let k_ref = self.list.push_newest(k_elem);
        let mut k_ptr = SendPtr::from(k_ref);

        let mut j_ptr = SendPtr::from(k_ref.older.load(Ordering::SeqCst));

        self.flush_each_separate_memlog(min_separate_commit_ver, &mut k_ptr, &mut j_ptr)
            .await?;
        self.compact_memlogs(&mut k_ptr, &mut j_ptr).await?;

        /*
        Compacting SSTables may be less urgent than flushing Memtables.
        In the future, the criteria for proceeding onto SSTables may be adjusted.
        */
        let should_work_on_sstables = true;
        if should_work_on_sstables {
            self.skip_separate_sstables(min_separate_commit_ver, &mut k_ptr, &mut j_ptr)
                .await;
            self.compact_entrysets(&mut k_ptr, &mut j_ptr).await?;
        }

        Ok(())
    }

    /// Flush each MemLog that must remain separate.
    async fn flush_each_separate_memlog(
        &self,
        min_separate_commit_ver: CommitVer,
        k_ptr: &mut SendPtr<ListNode<CommittedEntrySet<K, V>>>,
        j_ptr: &mut SendPtr<ListNode<CommittedEntrySet<K, V>>>,
    ) -> Result<()> {
        loop {
            self.cut_nonterminus_dummies(*k_ptr, j_ptr).await;

            let j_ref = unsafe { j_ptr.as_ref() };
            match &j_ref.elem {
                ListElem::Elem(ml @ CommittedEntrySet::RMemLog(memlog)) => {
                    if memlog.entryset_info.commit_info.commit_ver_hi_incl
                        >= min_separate_commit_ver
                    {
                        let g_ptr = SendPtr::from(j_ref.older.load(Ordering::SeqCst));

                        let slice = vec![ml];
                        let skip_tombstones = self.is_g_eq_dummy_oldest(g_ptr);
                        let flushed_sstable = self.compact(slice, skip_tombstones).await?;

                        self.replace_slice(k_ptr, j_ptr, *j_ptr, g_ptr, flushed_sstable)
                            .await;
                    } else {
                        break;
                    }
                }
                ListElem::Elem(CommittedEntrySet::SSTable(_)) => break,
                ListElem::Dummy { .. } => break,
            }
        }

        Ok(())
    }

    /// Compact 1+ MemLogs that do not need to remain separate.
    async fn compact_memlogs(
        &self,
        k_ptr: &mut SendPtr<ListNode<CommittedEntrySet<K, V>>>,
        j_ptr: &mut SendPtr<ListNode<CommittedEntrySet<K, V>>>,
    ) -> Result<()> {
        let mut h_ptr = *k_ptr;
        let mut g_ptr = *j_ptr;

        let mut slice = vec![];
        loop {
            self.cut_nonterminus_dummies(h_ptr, &mut g_ptr).await;

            let g_ref = unsafe { g_ptr.as_ref() };
            match &g_ref.elem {
                ListElem::Elem(ml @ CommittedEntrySet::RMemLog(_)) => {
                    slice.push(ml);

                    h_ptr = g_ptr;
                    g_ptr = SendPtr::from(g_ref.older.load(Ordering::SeqCst));
                }
                ListElem::Elem(CommittedEntrySet::SSTable(_)) => break,
                ListElem::Dummy { .. } => break,
            }
        }

        if slice.len() > 0 {
            let skip_tombstones = self.is_g_eq_dummy_oldest(g_ptr);
            let compacted_sstable = self.compact(slice, skip_tombstones).await?;

            self.replace_slice(k_ptr, j_ptr, h_ptr, g_ptr, compacted_sstable)
                .await;
        }

        Ok(())
    }

    /// Skip each SSTable that must remain separate.
    async fn skip_separate_sstables(
        &self,
        min_separate_commit_ver: CommitVer,
        k_ptr: &mut SendPtr<ListNode<CommittedEntrySet<K, V>>>,
        j_ptr: &mut SendPtr<ListNode<CommittedEntrySet<K, V>>>,
    ) {
        loop {
            self.cut_nonterminus_dummies(*k_ptr, j_ptr).await;

            let j_ref = unsafe { j_ptr.as_ref() };
            match &j_ref.elem {
                ListElem::Elem(CommittedEntrySet::RMemLog(_)) => break,
                ListElem::Elem(CommittedEntrySet::SSTable(sstable)) => {
                    if sstable.entryset_info.commit_info.commit_ver_hi_incl
                        >= min_separate_commit_ver
                    {
                        *k_ptr = *j_ptr;
                        *j_ptr = SendPtr::from(j_ref.older.load(Ordering::SeqCst));
                    } else {
                        break;
                    }
                }
                ListElem::Dummy { .. } => break,
            }
        }
    }

    /// Compact 2+ entrysets that do not need to remain separate.
    /// These entrysets are expected to be all SSTables; hence 2+ and not 1+.
    /// Currently, we go ahead and compact all such entrysets into one SSTable.
    async fn compact_entrysets(
        &self,
        k_ptr: &mut SendPtr<ListNode<CommittedEntrySet<K, V>>>,
        j_ptr: &mut SendPtr<ListNode<CommittedEntrySet<K, V>>>,
    ) -> Result<()> {
        let mut h_ptr = *k_ptr;
        let mut g_ptr = *j_ptr;

        let mut slice = vec![];
        loop {
            self.cut_nonterminus_dummies(h_ptr, &mut g_ptr).await;

            let g_ref = unsafe { g_ptr.as_ref() };
            match &g_ref.elem {
                ListElem::Elem(entryset) => {
                    slice.push(entryset);

                    h_ptr = g_ptr;
                    g_ptr = SendPtr::from(g_ref.older.load(Ordering::SeqCst));
                }
                ListElem::Dummy { .. } => break,
            }
        }

        if slice.len() > 1 {
            let skip_tombstones = self.is_g_eq_dummy_oldest(g_ptr);
            let compacted_sstable = self.compact(slice, skip_tombstones).await?;

            self.replace_slice(k_ptr, j_ptr, h_ptr, g_ptr, compacted_sstable)
                .await;
        }

        Ok(())
    }

    fn is_g_eq_dummy_oldest(&self, g_ptr: SendPtr<ListNode<CommittedEntrySet<K, V>>>) -> bool {
        g_ptr.raw() == self.list.dummy_oldest()
    }

    async fn compact(
        &self,
        slice: Vec<&CommittedEntrySet<K, V>>,
        skip_tombstones: bool,
    ) -> Result<SSTable<K, V>> {
        let commit_ver_hi_incl = slice[0].info().commit_info.commit_ver_hi_incl;
        let commit_ver_lo_incl = slice.last().unwrap().info().commit_info.commit_ver_lo_incl;
        let timestamp = Timestamp::inc_from(slice.iter().map(|es| es.info().commit_info.timestamp));
        let entryset_dir = self.format_new_entryset_dir_path();
        let entryset_info = CommittedEntrySetInfo {
            commit_info: CommitInfo {
                commit_ver_hi_incl,
                commit_ver_lo_incl,
                timestamp,
            },
            entryset_dir,
        };

        let entries =
            merging::merge_committed_entrysets(slice.into_iter(), None, None).filter(|entry| {
                if skip_tombstones {
                    match entry.try_borrow() {
                        Err(_) => true,
                        Ok((_k, v)) => match v {
                            OptDatum::Tombstone => false,
                            OptDatum::Some(_) => true,
                        },
                    }
                } else {
                    true
                }
            });

        SSTable::new(entries, entryset_info)
    }
}
