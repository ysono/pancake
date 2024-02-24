use crate::{
    db_state::DbState,
    ds_n_a::{
        atomic_linked_list::{ListNode, ListSnapshot},
        ordered_dict::Neighbors,
        send_ptr::NonNullSendPtr,
    },
    lsm::{
        unit::{CommitVer, CommittedUnit},
        Boundary, LsmState,
    },
    opers::fc::{
        fc_compaction::CompactionResult,
        gc::{DanglingNodeSet, DanglingNodeSetsDeque},
        FlushingAndCompactionWorker,
    },
    DB,
};
use anyhow::Result;
use std::ptr::NonNull;
use std::sync::atomic::Ordering;
use tokio::sync::{MutexGuard, RwLockReadGuard};

impl FlushingAndCompactionWorker {
    pub(super) async fn flush_and_compact(&mut self, probe_commit_ver: CommitVer) -> Result<()> {
        /* Hold a shared guard on `db_state` while working on one segment of the LL. */
        let db_state_guard = self.db.db_state().read().await;

        let mut segm_params;
        {
            let mut lsm_state = self.db.lsm_state().lock().await;

            let segm_res = derive::derive_one_segment(&mut lsm_state, probe_commit_ver);
            match segm_res {
                SegmDefnResult::NoOp => return Ok(()),
                /* Even if the segm doesn't straddle any non-held boundary,
                the segm might contain MemLog(s), which are F+C'able. */
                SegmDefnResult::NotStraddlingNonHeld(segm_params_)
                | SegmDefnResult::StraddlingNonHeld(segm_params_) => {
                    segm_params = segm_params_;
                }
            }
        }

        let mut job = FCJob {
            db: &self.db,
            db_state_guard,
            dangling_nodes: &mut self.dangling_nodes,
        };

        loop {
            let segm_res = job.flush_and_compact_one_segment(segm_params).await?;
            match segm_res {
                /* If the new segm doesn't straddle any non-held boundary, then
                the segm must have remained unchanged. */
                SegmDefnResult::NoOp | SegmDefnResult::NotStraddlingNonHeld(_) => break,
                SegmDefnResult::StraddlingNonHeld(segm_params_) => {
                    segm_params = segm_params_;
                }
            }
        }

        Ok(())
    }
}

mod derive {
    use super::*;

    pub fn derive_one_segment(
        lsm_state: &mut MutexGuard<LsmState>,
        probe_commit_ver: CommitVer,
    ) -> SegmDefnResult {
        let initial_probe_res = derive::get_probe_adjacent_boundaries(lsm_state, probe_commit_ver);
        let (mut new_end_cmt_ver, mut old_end_cmt_ver) = match initial_probe_res {
            Err(e) => return e,
            Ok((a, b)) => (a, b),
        };

        let straddles_nonheld =
            derive::walk_nonheld_boundaries(lsm_state, &mut new_end_cmt_ver, &mut old_end_cmt_ver);

        let segm_params = derive::translate_boundaries_to_segm_params(
            lsm_state,
            new_end_cmt_ver,
            old_end_cmt_ver,
        );

        if straddles_nonheld {
            SegmDefnResult::StraddlingNonHeld(segm_params)
        } else {
            SegmDefnResult::NotStraddlingNonHeld(segm_params)
        }
    }

    fn get_probe_adjacent_boundaries(
        lsm_state: &MutexGuard<LsmState>,
        probe_commit_ver: CommitVer,
    ) -> Result<(Option<CommitVer>, Option<CommitVer>), SegmDefnResult> {
        let (newer_cmt_ver, older_cmt_ver);
        if probe_commit_ver == lsm_state.curr_commit_ver() {
            newer_cmt_ver = None;
            older_cmt_ver = lsm_state.boundaries().get_newest_key().cloned();
        } else {
            match lsm_state.boundaries().get_neighbors(&probe_commit_ver) {
                None => return Err(SegmDefnResult::NoOp),
                Some(Neighbors { older, .. }) => {
                    newer_cmt_ver = Some(probe_commit_ver);

                    older_cmt_ver = older.map(|(older_k, _older_bound)| *older_k);
                }
            }
        }
        return Ok((newer_cmt_ver, older_cmt_ver));
    }

    fn walk_nonheld_boundaries(
        lsm_state: &mut MutexGuard<LsmState>,
        new_end_cmt_ver: &mut Option<CommitVer>,
        old_end_cmt_ver: &mut Option<CommitVer>,
    ) -> bool {
        let mut straddles_nonheld = false;

        loop {
            if let Some(cmt_ver) = new_end_cmt_ver {
                let Boundary { hold_count, .. } = lsm_state.boundaries().get(cmt_ver).unwrap();
                if *hold_count == 0 {
                    let rm_res = lsm_state.boundaries_mut().remove(cmt_ver).unwrap();
                    *new_end_cmt_ver = rm_res.neighbors.newer.map(|(cmt_ver, _bound)| *cmt_ver);
                    straddles_nonheld = true;
                    continue;
                }
            }
            break;
        }
        loop {
            if let Some(cmt_ver) = old_end_cmt_ver {
                let Boundary { hold_count, .. } = lsm_state.boundaries().get(cmt_ver).unwrap();
                if *hold_count == 0 {
                    let rm_res = lsm_state.boundaries_mut().remove(cmt_ver).unwrap();
                    *old_end_cmt_ver = rm_res.neighbors.older.map(|(cmt_ver, _bound)| *cmt_ver);
                    straddles_nonheld = true;
                    continue;
                }
            }
            break;
        }

        straddles_nonheld
    }

    fn translate_boundaries_to_segm_params(
        lsm_state: &MutexGuard<LsmState>,
        new_end_cmt_ver: Option<CommitVer>,
        old_end_cmt_ver: Option<CommitVer>,
    ) -> SegmParams {
        let (newer_node, first_node, first_commit_ver) = match new_end_cmt_ver {
            None => (
                None,
                lsm_state.list().head_node_ptr(),
                lsm_state.curr_commit_ver(),
            ),
            Some(cmt_ver) => {
                let Boundary { node_newer, .. } = lsm_state.boundaries().get(&cmt_ver).unwrap();

                let node_newer_ref = unsafe { node_newer.as_ref() };
                let first_node = node_newer_ref.next.load(Ordering::SeqCst);
                let first_node = NonNull::new(first_node).map(NonNullSendPtr::from);

                (Some(*node_newer), first_node, cmt_ver)
            }
        };

        let (last_node, older_commit_ver) = match old_end_cmt_ver {
            None => (None, None),
            Some(cmt_ver) => {
                let Boundary { node_newer, .. } = lsm_state.boundaries().get(&cmt_ver).unwrap();
                (Some(*node_newer), Some(cmt_ver))
            }
        };

        SegmParams {
            newer_node,
            first_node,
            first_commit_ver,
            last_node,
            older_commit_ver,
        }
    }
}

/// A struct that contains references that are used over the course of one run of flushing+compaction.
///
/// This type is necessary iff the run makes
/// 1+ const references and 1+ mut references
/// to fields within struct [`FlushingAndCompactionWorker`].
pub(super) struct FCJob<'job> {
    pub(super) db: &'job DB,
    pub(super) db_state_guard: RwLockReadGuard<'job, DbState>,
    pub(super) dangling_nodes: &'job mut DanglingNodeSetsDeque,
}

impl<'job> FCJob<'job> {
    async fn flush_and_compact_one_segment(
        &mut self,
        segm_params: SegmParams,
    ) -> Result<SegmDefnResult> {
        let SegmParams {
            newer_node: _,
            first_node,
            first_commit_ver: _,
            last_node,
            older_commit_ver: _,
        } = segm_params;

        let first_node = match first_node {
            None => {
                /* There is nothing to do on the current segm.
                We don't attempt to expand the current segm, and return saying that the current segm couldn't be expanded. */
                return Ok(SegmDefnResult::NoOp);
            }
            Some(p) => p,
        };

        let older_node = last_node.and_then(|last_node_ptr| {
            let last_node_ref = unsafe { last_node_ptr.as_ref() };
            let older_node_ptr = last_node_ref.next.load(Ordering::SeqCst);
            NonNull::new(older_node_ptr).map(NonNullSendPtr::from)
        });

        let unit_nodes = Self::collect_segm_nodes(first_node, older_node);

        let units = unit_nodes
            .iter()
            .map(|node_ptr| {
                let node_ref = unsafe { node_ptr.as_ref() };
                &node_ref.elem
            })
            .collect::<Vec<_>>();
        let skip_tombstones = older_node.is_none();
        let fc_res = self.do_flush_and_compact(units, skip_tombstones)?;

        self.activate_compaction_result(&segm_params, older_node, unit_nodes, fc_res)
            .await
    }

    fn collect_segm_nodes(
        first_node: NonNullSendPtr<ListNode<CommittedUnit>>,
        older_node: Option<NonNullSendPtr<ListNode<CommittedUnit>>>,
    ) -> Vec<NonNullSendPtr<ListNode<CommittedUnit>>> {
        let mut nodes = vec![];
        let snap = ListSnapshot::new_unchecked(Some(first_node), older_node);
        let mut iter = snap.iter();
        while let Some(node) = iter.next_node() {
            let node = node as *const ListNode<CommittedUnit>;
            let node = NonNullSendPtr::from(unsafe { NonNull::new_unchecked(node.cast_mut()) });
            nodes.push(node);
        }
        nodes
    }

    async fn activate_compaction_result(
        &mut self,
        segm_params: &SegmParams,
        older_node: Option<NonNullSendPtr<ListNode<CommittedUnit>>>,
        unit_nodes: Vec<NonNullSendPtr<ListNode<CommittedUnit>>>,
        fc_res: CompactionResult,
    ) -> Result<SegmDefnResult> {
        let SegmParams {
            first_commit_ver,
            older_commit_ver,
            ..
        } = segm_params;
        let first_commit_ver = *first_commit_ver;

        match fc_res {
            CompactionResult::NoChange => {
                let mut lsm_state = self.db.lsm_state().lock().await;

                return Ok(derive::derive_one_segment(&mut lsm_state, first_commit_ver));
            }
            CompactionResult::Empty => {
                /* We must maintain the invariant that
                each adjacent pair of boundaries are separated by 1+ LL nodes (actually, exactly 1 node).
                To cut existing nodes, we must replace them with 1 node containing empty data.
                TODO do this. */
                {
                    let mut lsm_state = self.db.lsm_state().lock().await;

                    return Ok(derive::derive_one_segment(&mut lsm_state, first_commit_ver));
                }
            }
            CompactionResult::Some(replc_unit) => {
                let replc_node_own = ListNode::new(replc_unit);

                let older_node = NonNullSendPtr::as_ptr(older_node).cast_mut();
                replc_node_own.next.store(older_node, Ordering::SeqCst);

                let replc_node_ptr = Box::into_raw(replc_node_own);
                let replc_node_ptr =
                    NonNullSendPtr::from(unsafe { NonNull::new_unchecked(replc_node_ptr) });

                let (penult_list_ver, updated_mhlv);
                let new_segm_res;
                {
                    let mut lsm_state = self.db.lsm_state().lock().await;

                    if first_commit_ver == lsm_state.curr_commit_ver() {
                        lsm_state
                            .list_mut()
                            .set_head_node_ptr_noncontested(Some(replc_node_ptr));
                    } else {
                        let newer_node_ptr = lsm_state
                            .boundaries()
                            .get(&first_commit_ver)
                            .unwrap()
                            .node_newer;
                        let newer_node_ref = unsafe { newer_node_ptr.as_ref() };

                        /* TODO If `newer_node` was discovered to be `Some` at the time we walked the OrderedDict, then
                        we don't have to re-discover `newer_node`, and
                        we can assign `newer_node.next.store(...)` outside the mutex guard. */
                        newer_node_ref
                            .next
                            .store(replc_node_ptr.as_ptr(), Ordering::SeqCst);
                    }

                    if let Some(older_commit_ver) = older_commit_ver {
                        lsm_state
                            .boundaries_mut()
                            .get_mut(older_commit_ver)
                            .unwrap()
                            .node_newer = replc_node_ptr;
                    }

                    (penult_list_ver, updated_mhlv) = lsm_state.fetch_inc_curr_list_ver();

                    new_segm_res = derive::derive_one_segment(&mut lsm_state, first_commit_ver);
                }

                let dang_set = DanglingNodeSet {
                    max_incl_traversable_list_ver: penult_list_ver,
                    nodes: unit_nodes,
                };
                self.dangling_nodes.push_back(dang_set);

                if let Some(mhlv) = updated_mhlv {
                    self.dangling_nodes.gc_old_nodes(mhlv)?;
                }

                return Ok(new_segm_res);
            }
        }
    }
}

enum SegmDefnResult {
    NoOp,
    NotStraddlingNonHeld(SegmParams),
    StraddlingNonHeld(SegmParams),
}

struct SegmParams {
    /// The node immediately newer than the segment.
    #[allow(dead_code)] // See another comment re: how this info can be used.
    pub newer_node: Option<NonNullSendPtr<ListNode<CommittedUnit>>>,

    /// The newest node included in the segment.
    pub first_node: Option<NonNullSendPtr<ListNode<CommittedUnit>>>,
    /// The newest commit ver included in the segment.
    pub first_commit_ver: CommitVer,

    /// The oldest node included in the segment.
    pub last_node: Option<NonNullSendPtr<ListNode<CommittedUnit>>>,

    /// The commit ver immediately older than the segment.
    pub older_commit_ver: Option<CommitVer>,
}
