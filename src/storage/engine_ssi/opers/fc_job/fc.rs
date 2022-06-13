use crate::ds_n_a::atomic_linked_list::ListNode;
use crate::ds_n_a::send_ptr::SendPtr;
use crate::storage::engine_ssi::{
    lsm_state::{unit::unit_utils, LsmElem},
    opers::fc_job::{DanglingNodeSet, FlushingAndCompactionJob},
};
use anyhow::Result;
use std::sync::atomic::{AtomicPtr, Ordering};

impl FlushingAndCompactionJob {
    pub(super) async fn flush_and_compact(&mut self) -> Result<()> {
        /* Malloc for new_head outside the mutex guard.
        Free new_head outside the mutex guard, thanks to Option<>. */
        let mut prepped_new_head = Some(unit_utils::new_dummy_node(0, false));
        let snap_head_excl;
        {
            let lsm_state = self.db.lsm_state().lock().await;

            let update_or_provide_head = |elem: Option<&LsmElem>| match elem {
                Some(LsmElem::Dummy { .. }) => return None,
                _ => {
                    return prepped_new_head.take();
                }
            };
            snap_head_excl = SendPtr::from(lsm_state.update_or_push(update_or_provide_head));
        }

        self.traverse_and_compact(unsafe { snap_head_excl.as_ref() })
            .await?;

        Ok(())
    }

    pub(super) async fn traverse_and_compact(
        &mut self,
        snap_head_excl: &ListNode<LsmElem>,
    ) -> Result<()> {
        let mut segm_head_excl = snap_head_excl;
        loop {
            let curr_node = self.compact_one_segment(segm_head_excl).await?;
            match curr_node.status {
                CurrNodeStatus::Fence | CurrNodeStatus::EndOfList => break,
                CurrNodeStatus::NonFenceBoundary | CurrNodeStatus::CommittedUnit => {
                    segm_head_excl = unsafe { curr_node.ptr.as_ref() };
                }
            }
        }
        Ok(())
    }
    async fn compact_one_segment(
        &mut self,
        segm_head_excl: &ListNode<LsmElem>,
    ) -> Result<CurrNode> {
        let mut slice = vec![];

        let mut prev_ref = segm_head_excl;

        let curr_node = loop {
            let curr_node = self.cut_non_boundary_dummies(prev_ref).await;
            match curr_node.status {
                CurrNodeStatus::CommittedUnit => {
                    let curr_ref = unsafe { curr_node.ptr.as_ref() };
                    slice.push(curr_ref);
                    prev_ref = curr_ref;
                }
                CurrNodeStatus::NonFenceBoundary
                | CurrNodeStatus::Fence
                | CurrNodeStatus::EndOfList => break curr_node,
            }
        };

        let units = slice
            .iter()
            .filter_map(|node| match &node.elem {
                LsmElem::Unit(unit) => Some(unit),
                LsmElem::Dummy { .. } => None,
            })
            .collect::<Vec<_>>();
        let skip_tombstones = curr_node.status == CurrNodeStatus::EndOfList;
        let compacted_unit = self.do_flush_and_compact(units, skip_tombstones).await?;
        if let Some(unit) = compacted_unit {
            let node = unit_utils::new_unit_node(unit);
            self.replace(segm_head_excl, curr_node.ptr, Some(node), slice)
                .await;
        }

        Ok(curr_node)
    }
    async fn cut_non_boundary_dummies(&mut self, prior_excl: &ListNode<LsmElem>) -> CurrNode {
        let mut slice = vec![];

        let mut curr_ptr = SendPtr::from(prior_excl.next.load(Ordering::SeqCst));

        let curr_node_status = loop {
            if curr_ptr.as_ptr().is_null() {
                break CurrNodeStatus::EndOfList;
            } else {
                let curr_ref = unsafe { curr_ptr.as_ref() };
                match &curr_ref.elem {
                    LsmElem::Unit(_) => break CurrNodeStatus::CommittedUnit,
                    LsmElem::Dummy {
                        hold_count,
                        is_fence,
                    } => {
                        if is_fence.load(Ordering::SeqCst) == true {
                            break CurrNodeStatus::Fence;
                        } else if hold_count.load(Ordering::SeqCst) != 0 {
                            break CurrNodeStatus::NonFenceBoundary;
                        } else {
                            slice.push(curr_ref);
                            curr_ptr = SendPtr::from(curr_ref.next.load(Ordering::SeqCst));
                        }
                    }
                }
            }
        };

        if !slice.is_empty() {
            self.replace(prior_excl, curr_ptr, None, slice).await;
        }

        CurrNode {
            ptr: curr_ptr,
            status: curr_node_status,
        }
    }

    async fn replace(
        &mut self,
        slice_head_excl: &ListNode<LsmElem>,
        slice_tail_excl: SendPtr<ListNode<LsmElem>>,
        replacement_node: Option<Box<ListNode<LsmElem>>>,
        slice: Vec<&ListNode<LsmElem>>,
    ) {
        if let Some(mut r_own) = replacement_node {
            r_own.next = AtomicPtr::new(slice_tail_excl.as_ptr_mut());
            let r_ptr = Box::into_raw(r_own);
            slice_head_excl.next.store(r_ptr, Ordering::SeqCst);
        } else {
            slice_head_excl
                .next
                .store(slice_tail_excl.as_ptr_mut(), Ordering::SeqCst);
        }

        let penult_list_ver;
        {
            let mut lsm_state = self.db.lsm_state().lock().await;

            penult_list_ver = lsm_state.bump_curr_list_ver();
        }

        let nodes = slice
            .into_iter()
            .map(|node_ref| SendPtr::from(node_ref))
            .collect::<Vec<_>>();
        let dangling_node_set = DanglingNodeSet {
            max_incl_traversable_list_ver: penult_list_ver,
            nodes,
        };
        self.dangling_nodes.push_back(dangling_node_set);
    }
}

/// `status` property encodes the node's last known status.
/// This obviates the need to load a dummy's atomic properties more than once.
struct CurrNode {
    ptr: SendPtr<ListNode<LsmElem>>,
    status: CurrNodeStatus,
}
#[derive(PartialEq, Eq)]
enum CurrNodeStatus {
    CommittedUnit,
    NonFenceBoundary,
    Fence,
    EndOfList,
}
