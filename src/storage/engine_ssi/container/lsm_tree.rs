use crate::ds_n_a::atomic_linked_list::{AtomicLinkedList, ListElem};
use crate::ds_n_a::atomic_queue::AtomicQueue;
use crate::storage::engine_ssi::container::{DanglingSlice, VersionState};
use crate::storage::engine_ssi::entryset::{
    CommitVer, CommittedEntrySet, CommittedEntrySetInfo, EntrySetDir, LoadCommitInfoResult,
};
use crate::storage::engines_common::fs_utils::{self, PathNameNum};
use crate::storage::serde::{OptDatum, Serializable};
use anyhow::{anyhow, Result};
use derive_more::{Deref, DerefMut, From};
use shorthand::ShortHand;
use std::cmp;
use std::cmp::Ord;
use std::collections::BinaryHeap;
use std::fs::{self};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(From, Deref, DerefMut, Copy, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct ListVer(pub u64);

const ON_BOOTUP_LIST_VER: ListVer = ListVer(0);

#[derive(ShortHand)]
#[shorthand(disable(get))]
pub struct LSMTree<K, V> {
    lsm_dir_path: PathBuf,
    #[shorthand(enable(get))]
    list: Pin<Box<AtomicLinkedList<CommittedEntrySet<K, V>>>>,
    list_ver_state: VersionState<ListVer>,
    dangling_slices: Pin<Box<AtomicQueue<DanglingSlice<K, V>>>>,
    next_entryset_num: AtomicU64,
}

impl<K, V> LSMTree<K, V>
where
    K: Serializable + Ord + Clone,
    OptDatum<V>: Serializable,
{
    pub fn load_or_new<P: AsRef<Path>>(lsm_dir_path: P) -> Result<Self> {
        fs::create_dir_all(&lsm_dir_path)?;

        let (es_infos, next_entryset_num) =
            Self::clean_and_collect_committed_entrysets(&lsm_dir_path)?;
        let entrysets = es_infos
            .into_iter()
            .map(CommittedEntrySet::load)
            .collect::<Result<Vec<_>>>()?;
        let list = AtomicLinkedList::new(entrysets.into_iter());

        Ok(Self {
            lsm_dir_path: lsm_dir_path.as_ref().into(),
            list,
            list_ver_state: VersionState::new(ON_BOOTUP_LIST_VER),
            dangling_slices: AtomicQueue::new(),
            next_entryset_num: AtomicU64::from(next_entryset_num),
        })
    }

    /// 1. Scan all entryset dirs. Deser [`CommittedEntrySetInfo`].
    ///     If commit info is not found, remove the entryset dir.
    /// 1. Add all [`CommittedEntrySetInfo`]s into a priority queue.
    ///     The entryset to be peeked/popped the earliest is the entryset with
    ///     the newest upper-bound commit-ver and the newest timestamp-id.
    /// 1. Selectively collect [`CommittedEntrySetInfo`]s.
    ///     Among any entrysets that cover an overlapping commit version range,
    ///     collect the one entryset with the newest timestamp id, and remove the rest's dirs.
    fn clean_and_collect_committed_entrysets<P: AsRef<Path>>(
        lsm_dir_path: P,
    ) -> Result<(Vec<CommittedEntrySetInfo>, u64)> {
        let mut es_info_pq = BinaryHeap::new();
        let mut max_entryset_num = 0;
        for sub_path in fs_utils::read_dir(&lsm_dir_path)? {
            let sub_path = sub_path?;

            let num = Self::parse_entryset_dir_num(&sub_path)?;
            max_entryset_num = cmp::max(max_entryset_num, *num);

            let es_dir = EntrySetDir::from(sub_path);
            match es_dir.load_commit_info()? {
                LoadCommitInfoResult::NotFound(es_dir) => {
                    fs::remove_dir_all(&*es_dir)?;
                }
                LoadCommitInfoResult::Committed(es_info) => {
                    es_info_pq.push(es_info);
                }
            }
        }
        let next_entryset_num = max_entryset_num + 1;

        let mut es_infos: Vec<CommittedEntrySetInfo> = vec![];
        while !es_info_pq.is_empty() {
            let curr_es_info = es_info_pq.pop().unwrap();
            let is_included = es_infos.is_empty()
                || es_infos.last().unwrap().commit_info.commit_ver_lo_incl
                    > curr_es_info.commit_info.commit_ver_hi_incl;
            if is_included {
                es_infos.push(curr_es_info);
            } else {
                fs::remove_dir_all(&*curr_es_info.entryset_dir)?;
            }
        }
        Ok((es_infos, next_entryset_num))
    }

    pub fn format_new_entryset_dir_path(&self) -> EntrySetDir {
        let num = self.next_entryset_num.fetch_add(1, Ordering::SeqCst);
        let dir_name = PathNameNum::from(num).format_hex();
        let dir_path = self.lsm_dir_path.join(dir_name);
        EntrySetDir::from(dir_path)
    }
    fn parse_entryset_dir_num<P: AsRef<Path>>(dir_path: P) -> Result<PathNameNum> {
        let dir_path = dir_path.as_ref();
        let maybe_file_name = dir_path.file_name().and_then(|os_str| os_str.to_str());
        let res_file_name =
            maybe_file_name.ok_or(anyhow!("Unexpected entryset dir path {:?}", dir_path));
        res_file_name.and_then(|file_name| PathNameNum::parse_hex(file_name))
    }

    /// *NOT* concurrency-safe! This method must be used only while there are
    ///     no concurrently modifying jobs (GC job, scnd idx creation job).
    pub fn newest_commit_ver(&self) -> Option<CommitVer> {
        self.list
            .iter()
            .filter_map(|elem| match elem {
                ListElem::Dummy { .. } => None,
                ListElem::Elem(entryset) => Some(entryset),
            })
            .next()
            .map(|entryset| entryset.info().commit_info.commit_ver_hi_incl)
    }
}

mod entryset_opers;
mod gc_cleanup;
mod gc_modify_helpers;
mod gc_modify_main;
mod get_opers;
