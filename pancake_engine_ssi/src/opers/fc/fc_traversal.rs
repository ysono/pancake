use crate::ds_n_a::{atomic_linked_list::ListNode, send_ptr::SendPtr};
use crate::{
    db_state::DbState,
    lsm::{lsm_state_utils, LsmElem},
    opers::fc::{
        fc_compaction::CompactionResult,
        gc::{DanglingNodeSet, DanglingNodeSetsDeque},
        FlushingAndCompactionWorker,
    },
    DB,
};
use anyhow::Result;
use std::ptr;
use std::sync::atomic::Ordering;
use tokio::sync::RwLockReadGuard;

impl FlushingAndCompactionWorker {
    /// If @arg `maybe_snap_head_ptr` is `Some`, then it is assumed that
    /// the ptr is already pushed to the linked list.
    pub(super) async fn flush_and_compact(
        &mut self,
        maybe_snap_head_ptr: Option<SendPtr<ListNode<LsmElem>>>,
    ) -> Result<()> {
        /* Hold a shared guard on `db_state` over the whole traversal of the LL. */
        let db_state_guard = self.db.db_state().read().await;

        let snap_head_ref = match maybe_snap_head_ptr {
            Some(ptr) => unsafe { ptr.as_ref() },
            None => self.establish_snap_head().await,
        };

        let mut job = FCJob {
            db: &self.db,
            db_state_guard,
            dangling_nodes: &mut self.dangling_nodes,
        };
        job.traverse_and_compact(snap_head_ref).await?;

        Ok(())
    }

    async fn establish_snap_head<'a>(&self) -> &'a ListNode<LsmElem> {
        /* The new_head is malloc'd and free'd outside the mutex guard.
        Don't `move` the prepped new_head into the lambda. */
        let mut prepped_new_head = Some(lsm_state_utils::new_dummy_node(0, false));
        let update_or_provide_head = |elem: Option<&LsmElem>| match elem {
            Some(LsmElem::Dummy { .. }) => return None,
            _ => {
                return prepped_new_head.take();
            }
        };
        let snap_head_ptr = {
            let lsm_state = self.db.lsm_state().lock().await;

            lsm_state.update_or_push(update_or_provide_head)
        };
        let snap_head_ref = unsafe { &*snap_head_ptr };
        snap_head_ref
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
    pub(super) async fn traverse_and_compact(
        &mut self,
        mut prev_ref: &ListNode<LsmElem>,
    ) -> Result<()> {
        loop {
            let curr_info = self.compact_one_segment(prev_ref).await?;
            match curr_info {
                NodeInfo::BoundaryDummy(curr_ptr) | NodeInfo::CommittedUnit(curr_ptr) => {
                    // It's impossible for the curr elem to be a CommittedUnit.
                    prev_ref = unsafe { curr_ptr.as_ref() };
                    continue;
                }
                NodeInfo::FenceDummy(_) | NodeInfo::EndOfList => break,
            }
        }
        Ok(())
    }

    async fn compact_one_segment(&mut self, prev_ref: &ListNode<LsmElem>) -> Result<NodeInfo> {
        let (cut_dummy_nodes, unit_nodes, curr_info) = Self::collect_one_segment(prev_ref);

        let units = unit_nodes
            .iter()
            .filter_map(|node_ptr| {
                let node_ref = unsafe { node_ptr.as_ref() };
                match &node_ref.elem {
                    // It's guaranteed to be a CommittedUnit.
                    LsmElem::Unit(unit) => Some(unit),
                    LsmElem::Dummy { .. } => None,
                }
            })
            .collect::<Vec<_>>();
        let skip_tombstones = match curr_info {
            NodeInfo::CommittedUnit(_) | NodeInfo::BoundaryDummy(_) | NodeInfo::FenceDummy(_) => {
                false
            }
            NodeInfo::EndOfList => true,
        };
        let fc_result = self.do_flush_and_compact(units, skip_tombstones)?;

        let did_cut_unit_nodes =
            Self::potentially_replace_segment(fc_result, || unit_nodes.len(), prev_ref, &curr_info);

        let mut cut_nodes = Vec::with_capacity(2);
        if cut_dummy_nodes.is_empty() == false {
            cut_nodes.push(cut_dummy_nodes);
        }
        if did_cut_unit_nodes {
            cut_nodes.push(unit_nodes);
        }
        if cut_nodes.is_empty() == false {
            self.record_list_ver_change(cut_nodes).await?;
        }

        Ok(curr_info)
    }

    fn collect_one_segment(
        mut prev_ref: &ListNode<LsmElem>,
    ) -> (
        Vec<SendPtr<ListNode<LsmElem>>>,
        Vec<SendPtr<ListNode<LsmElem>>>,
        NodeInfo,
    ) {
        let mut cut_dummy_nodes = vec![];
        let mut unit_nodes = vec![];

        let mut curr_info;
        loop {
            curr_info = Self::cut_contiguous_non_boundary_dummies(prev_ref, &mut cut_dummy_nodes);
            match curr_info {
                NodeInfo::CommittedUnit(curr_ptr) => {
                    unit_nodes.push(curr_ptr);
                    prev_ref = unsafe { curr_ptr.as_ref() };
                    continue;
                }
                NodeInfo::BoundaryDummy(_) | NodeInfo::FenceDummy(_) | NodeInfo::EndOfList => break,
            };
        }

        (cut_dummy_nodes, unit_nodes, curr_info)
    }

    fn cut_contiguous_non_boundary_dummies(
        prev_ref: &ListNode<LsmElem>,
        cut_nodes: &mut Vec<SendPtr<ListNode<LsmElem>>>,
    ) -> NodeInfo {
        let mut has_cuttable = false;

        let mut curr_ptr = prev_ref.next.load(Ordering::SeqCst);
        let curr_info = loop {
            if curr_ptr.is_null() {
                break NodeInfo::EndOfList;
            } else {
                let curr_ref = unsafe { &*curr_ptr };
                let curr_sendptr = SendPtr::from(curr_ptr);
                match &curr_ref.elem {
                    LsmElem::Unit(_) => break NodeInfo::CommittedUnit(curr_sendptr),
                    LsmElem::Dummy {
                        hold_count,
                        is_fence,
                    } => {
                        /* A held fence node must be considered a fence node, not a mere segment-boundary node.
                        Hence we must check `is_fence` first, then `hold_count. */
                        if is_fence.load(Ordering::SeqCst) == true {
                            break NodeInfo::FenceDummy(curr_sendptr);
                        } else if hold_count.load(Ordering::SeqCst) != 0 {
                            break NodeInfo::BoundaryDummy(curr_sendptr);
                        } else {
                            cut_nodes.push(curr_sendptr);
                            curr_ptr = curr_ref.next.load(Ordering::SeqCst);
                            has_cuttable = true;
                        }
                    }
                }
            }
        };

        if has_cuttable == true {
            prev_ref.next.store(curr_ptr, Ordering::SeqCst);
        }

        curr_info
    }

    fn potentially_replace_segment(
        fc_result: CompactionResult,
        unit_nodes_len: impl Fn() -> usize,
        segm_head_ref: &ListNode<LsmElem>,
        segm_tail_info: &NodeInfo,
    ) -> bool {
        let segm_tail_ptr = match segm_tail_info {
            NodeInfo::CommittedUnit(ptr)
            | NodeInfo::BoundaryDummy(ptr)
            | NodeInfo::FenceDummy(ptr) => ptr.as_ptr_mut(),
            NodeInfo::EndOfList => ptr::null_mut(),
        };

        match fc_result {
            CompactionResult::NoChange => {
                return false;
            }
            CompactionResult::Empty => {
                if unit_nodes_len() == 0 {
                    return false;
                } else {
                    segm_head_ref.next.store(segm_tail_ptr, Ordering::SeqCst);
                    return true;
                }
            }
            CompactionResult::Some(replc_unit) => {
                let replc_node_own = lsm_state_utils::new_unit_node(replc_unit);
                replc_node_own.next.store(segm_tail_ptr, Ordering::SeqCst);

                let replc_node_ptr = Box::into_raw(replc_node_own);
                segm_head_ref.next.store(replc_node_ptr, Ordering::SeqCst);

                return true;
            }
        }
    }

    async fn record_list_ver_change(
        &mut self,
        cut_nodes: Vec<Vec<SendPtr<ListNode<LsmElem>>>>,
    ) -> Result<()> {
        let (penult_list_ver, updated_mhlv) = {
            let mut lsm_state = self.db.lsm_state().lock().await;

            lsm_state.fetch_inc_curr_list_ver()
        };

        let dang_set = DanglingNodeSet {
            max_incl_traversable_list_ver: penult_list_ver,
            nodes: cut_nodes,
        };
        self.dangling_nodes.push_back(dang_set);

        if let Some(mhlv) = updated_mhlv {
            self.dangling_nodes.gc_old_nodes(mhlv)?;
        }

        Ok(())
    }
}

/// This struct encodes semantic info gained
/// from the first time an `AtomicPtr` was `load()`ed,
/// so that we don't have to `load()` it again.
enum NodeInfo {
    CommittedUnit(SendPtr<ListNode<LsmElem>>),
    BoundaryDummy(SendPtr<ListNode<LsmElem>>),
    FenceDummy(SendPtr<ListNode<LsmElem>>),
    EndOfList,
}
