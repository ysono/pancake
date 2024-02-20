use crate::ds_n_a::{atomic_linked_list::ListNode, send_ptr::NonNullSendPtr};
use crate::{
    db_state::DbState,
    lsm::{lsm_state_utils, LsmElem, LsmElemType},
    opers::fc::{
        fc_compaction::CompactionResult,
        gc::{DanglingNodeSet, DanglingNodeSetsDeque},
        FlushingAndCompactionWorker,
    },
    DB,
};
use anyhow::Result;
use std::ptr::{self, NonNull};
use std::sync::atomic::Ordering;
use tokio::sync::RwLockReadGuard;

impl FlushingAndCompactionWorker {
    /// If @arg `maybe_snap_head_ptr` is `Some`, then it is assumed that
    /// the node was already pushed to the linked list.
    pub(super) async fn flush_and_compact(
        &mut self,
        maybe_snap_head_ptr: Option<NonNullSendPtr<ListNode<LsmElem>>>,
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

    /// Ensure the presence of a node which is prev wrt (i.e. outside of) the first segment.
    async fn establish_snap_head<'a>(&self) -> &'a ListNode<LsmElem> {
        let mut cand_snap_head = Some(lsm_state_utils::new_dummy_node(false, 0));

        {
            let lsm_state = self.db.lsm_state().lock().await;

            if let Some(head_ptr) = lsm_state.list().head_node_ptr() {
                let head_ref = unsafe { head_ptr.as_ref() };
                if let LsmElemType::Dummy { is_fence, .. } = &head_ref.elem.elem_type {
                    if is_fence.load(Ordering::SeqCst) == false {
                        return head_ref;
                    }
                }
            }
            let snap_head_own = cand_snap_head.take().unwrap();
            let snap_head_ptr = lsm_state.list().push_head_node(snap_head_own);
            let snap_head_ref = unsafe { snap_head_ptr.as_ref() };
            return snap_head_ref;
        }

        /* TODO if cand_snap_head was unused, return it to a pool of dummies. */
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
    /// Starting from the node after @arg `snap_head_ref`,
    /// discover F+C'able segments and flush+compact them.
    pub(super) async fn traverse_and_compact(
        &mut self,
        snap_head_ref: &ListNode<LsmElem>,
    ) -> Result<()> {
        let mut segm_prev_ref = snap_head_ref;
        let mut segm_head_info = Self::traverse_one_node(segm_prev_ref);

        loop {
            match self
                .compact_one_segment(segm_prev_ref, segm_head_info)
                .await?
            {
                None => break,
                Some((segm_last_ref, segm_next_info)) => {
                    (segm_prev_ref, segm_head_info) = (segm_last_ref, segm_next_info);
                }
            }
        }

        Ok(())
    }

    /// @return `Some` iff the current segment was F+C'able (and was F+C'd).
    ///     Contains the next (potentially F+C'able) segment's [prev_of_head, head] nodes.
    async fn compact_one_segment<'ret>(
        &mut self,
        segm_prev_ref: &'ret ListNode<LsmElem>,
        segm_head_info: NodeInfo,
    ) -> Result<Option<(&'ret ListNode<LsmElem>, NodeInfo)>> {
        let (cut_dummy_nodes, unit_nodes, segm_last_ref, segm_next_info) =
            match Self::collect_one_segment(segm_head_info) {
                None => return Ok(None),
                Some(one_segm) => one_segm,
            };

        let units = unit_nodes
            .iter()
            .filter_map(|node_ptr| {
                let node_ref = unsafe { node_ptr.as_ref() };
                match &node_ref.elem.elem_type {
                    /* It's guaranteed to be a CommittedUnit. */
                    LsmElemType::CommittedUnit(unit) => Some(unit),
                    LsmElemType::Dummy { .. } => None,
                }
            })
            .collect::<Vec<_>>();
        let skip_tombstones = match segm_next_info {
            NodeInfo::EndOfList => true,
            NodeInfo::Fence(_) | NodeInfo::Held(_) | NodeInfo::NonFenceNonHeld(_) => false,
        };
        let fc_result = self.do_flush_and_compact(units, skip_tombstones)?;

        let replc_segm_last_ref = Self::replace_segment(
            fc_result,
            || unit_nodes.len(),
            segm_prev_ref,
            &segm_next_info,
        );

        let mut cut_nodes = Vec::with_capacity(2);
        if cut_dummy_nodes.is_empty() == false {
            cut_nodes.push(cut_dummy_nodes);
        }
        if replc_segm_last_ref.is_some() {
            cut_nodes.push(unit_nodes);
        }
        if cut_nodes.is_empty() == false {
            self.record_list_ver_change(cut_nodes).await?;
        }

        let actual_segm_last_ref = match replc_segm_last_ref {
            None => segm_last_ref,
            Some(node) => node,
        };

        Ok(Some((actual_segm_last_ref, segm_next_info)))
    }

    /// @return `Some` iff the segment starting with @arg `segm_head_info` is F+C'able.
    fn collect_one_segment<'ret>(
        segm_head_info: NodeInfo,
    ) -> Option<(
        Vec<NonNullSendPtr<ListNode<LsmElem>>>,
        Vec<NonNullSendPtr<ListNode<LsmElem>>>,
        &'ret ListNode<LsmElem>,
        NodeInfo,
    )> {
        let mut cut_dummy_nodes = vec![];
        let mut unit_nodes = vec![];

        /*
        1. Determine whether the segment is F+C'able.
        1. If F+C'able, and if the head node is a CommittedUnit, then save this node. */
        let head_ref = match segm_head_info {
            NodeInfo::EndOfList | NodeInfo::Fence(_) => return None,
            NodeInfo::Held(head_ptr) | NodeInfo::NonFenceNonHeld(head_ptr) => {
                /* Only the first segment's head could be non-held. */
                /* Each segment's head could be a CommittedUnit or a Dummy. */
                let head_ref = unsafe { head_ptr.as_ref() };
                match &head_ref.elem.elem_type {
                    LsmElemType::CommittedUnit(_) => {
                        unit_nodes.push(head_ptr);
                    }
                    LsmElemType::Dummy { .. } => {}
                }
                head_ref
            }
        };

        /* Collect the remaining (i.e. after-head) nodes of the current segment.
        While traversing, cut non-fence non-held dummy nodes. */
        let mut prev_ref = head_ref;
        let mut curr_info;
        loop {
            curr_info = Self::cut_contiguous_non_boundary_dummies(prev_ref, &mut cut_dummy_nodes);

            match curr_info {
                NodeInfo::EndOfList | NodeInfo::Fence(_) | NodeInfo::Held(_) => break,
                NodeInfo::NonFenceNonHeld(curr_ptr) => {
                    let curr_ref = unsafe { curr_ptr.as_ref() };
                    match &curr_ref.elem.elem_type {
                        /* A non-fence non-held node that was not cut is guaranteed to contain a CommittedUnit. */
                        LsmElemType::CommittedUnit(_) => {
                            unit_nodes.push(curr_ptr);
                        }
                        LsmElemType::Dummy { .. } => {}
                    }
                    prev_ref = curr_ref;
                }
            }
        }

        Some((cut_dummy_nodes, unit_nodes, prev_ref, curr_info))
    }

    /// Cut 0-or-more contiguous non-fence non-held dummy nodes that exist after @arg `prev_ref`.
    fn cut_contiguous_non_boundary_dummies(
        prev_ref: &ListNode<LsmElem>,
        cut_nodes: &mut Vec<NonNullSendPtr<ListNode<LsmElem>>>,
    ) -> NodeInfo {
        let mut has_cuttable = false;

        let mut curr_info = Self::traverse_one_node(prev_ref);
        loop {
            match curr_info {
                NodeInfo::EndOfList | NodeInfo::Fence(_) | NodeInfo::Held(_) => break,
                NodeInfo::NonFenceNonHeld(curr_ptr) => {
                    let curr_ref = unsafe { curr_ptr.as_ref() };
                    match &curr_ref.elem.elem_type {
                        LsmElemType::CommittedUnit(_) => break,
                        LsmElemType::Dummy { .. } => {
                            cut_nodes.push(curr_ptr);

                            has_cuttable = true;

                            curr_info = Self::traverse_one_node(curr_ref);
                        }
                    }
                }
            }
        }

        if has_cuttable == true {
            let curr_ptr = match curr_info {
                NodeInfo::EndOfList => ptr::null(),
                NodeInfo::Fence(p) | NodeInfo::Held(p) | NodeInfo::NonFenceNonHeld(p) => p.as_ptr(),
            };
            prev_ref.next.store(curr_ptr.cast_mut(), Ordering::SeqCst);
        }

        return curr_info;
    }

    fn traverse_one_node(prev_ref: &ListNode<LsmElem>) -> NodeInfo {
        let curr_ptr = prev_ref.next.load(Ordering::SeqCst);
        match NonNull::new(curr_ptr) {
            None => return NodeInfo::EndOfList,
            Some(curr_nnptr) => {
                let curr_ref = unsafe { curr_nnptr.as_ref() };
                let curr_sendptr = NonNullSendPtr::from(curr_nnptr);

                /* 1) Assess whether fence. */
                if let LsmElemType::Dummy { is_fence } = &curr_ref.elem.elem_type {
                    if is_fence.load(Ordering::SeqCst) == true {
                        return NodeInfo::Fence(curr_sendptr);
                    }
                }

                /* 2) Assess whether held. */
                if curr_ref.elem.hold_count.load(Ordering::SeqCst) != 0 {
                    return NodeInfo::Held(curr_sendptr);
                }

                /* 3) The remaining case. */
                return NodeInfo::NonFenceNonHeld(curr_sendptr);
            }
        }
    }

    /// @return
    ///     If we didn't replace, then None.
    ///     If we did replace, then the node that is newly before @arg `segm_next_info`.
    fn replace_segment<'arg>(
        fc_result: CompactionResult,
        unit_nodes_len: impl Fn() -> usize,
        segm_prev_ref: &'arg ListNode<LsmElem>,
        segm_next_info: &NodeInfo,
    ) -> Option<&'arg ListNode<LsmElem>> {
        let segm_next_ptr = match segm_next_info {
            NodeInfo::EndOfList => ptr::null_mut(),
            NodeInfo::Fence(p) | NodeInfo::Held(p) | NodeInfo::NonFenceNonHeld(p) => {
                p.as_ptr().cast_mut()
            }
        };

        match fc_result {
            CompactionResult::NoChange => {
                return None;
            }
            CompactionResult::Empty => {
                if unit_nodes_len() == 0 {
                    /* Given the guarantees made by `Self::do_flush_and_compact()`, this case is impossible.
                    But, checking this case is cheaper than `store()`ing an atomic variable unnecessarily. */
                    return None;
                } else {
                    segm_prev_ref.next.store(segm_next_ptr, Ordering::SeqCst);
                    return Some(segm_prev_ref);
                }
            }
            CompactionResult::Some(replc_unit) => {
                let replc_node_own = lsm_state_utils::new_unit_node(replc_unit, 0);
                replc_node_own.next.store(segm_next_ptr, Ordering::SeqCst);

                let replc_node_ptr = Box::into_raw(replc_node_own);
                segm_prev_ref.next.store(replc_node_ptr, Ordering::SeqCst);

                let replc_node_ref = unsafe { &*replc_node_ptr };
                return Some(replc_node_ref);
            }
        }
    }

    async fn record_list_ver_change(
        &mut self,
        cut_nodes: Vec<Vec<NonNullSendPtr<ListNode<LsmElem>>>>,
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
    NonFenceNonHeld(NonNullSendPtr<ListNode<LsmElem>>),
    Held(NonNullSendPtr<ListNode<LsmElem>>),
    Fence(NonNullSendPtr<ListNode<LsmElem>>),
    EndOfList,
}
