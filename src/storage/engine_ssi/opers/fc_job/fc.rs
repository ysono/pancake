use crate::ds_n_a::atomic_linked_list::ListNode;
use crate::ds_n_a::send_ptr::SendPtr;
use crate::storage::engine_ssi::{
    lsm_state::{unit::unit_utils, LsmElem, LsmElemContent, LIST_VER_PLACEHOLDER},
    opers::fc_job::{FlushingAndCompactionJob, NodeListVerInterval},
};
use anyhow::Result;
use std::sync::atomic::{AtomicPtr, Ordering};

impl FlushingAndCompactionJob {
    pub(super) async fn flush_and_compact(&mut self) -> Result<()> {
        /* Malloc for new_head outside the mutex guard.
        Free new_head outside the mutex guard, thanks to Option<>. */
        let mut prepped_new_head = Some(unit_utils::new_dummy_node(LIST_VER_PLACEHOLDER, 0, false));
        let snap_head_excl;
        {
            let lsm_state = self.db.lsm_state().lock().await;

            let update_or_provide_head = |content: Option<&LsmElemContent>| match content {
                Some(LsmElemContent::Dummy { .. }) => return None,
                _ => {
                    let mut new_head = prepped_new_head.take().unwrap();
                    new_head.elem.traversable_list_ver_lo_incl = lsm_state.curr_list_ver;
                    Some(new_head)
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
            .filter_map(|node| match &node.elem.content {
                LsmElemContent::Unit(unit) => Some(unit),
                LsmElemContent::Dummy { .. } => None,
            })
            .collect::<Vec<_>>();
        let skip_tombstones = curr_node.status == CurrNodeStatus::EndOfList;
        let compacted_unit = self.do_flush_and_compact(units, skip_tombstones).await?;
        if let Some(unit) = compacted_unit {
            let node = unit_utils::new_unit_node(unit, LIST_VER_PLACEHOLDER);
            self.replace(segm_head_excl, curr_node.ptr, Some(node), slice)
                .await;
        }

        Ok(curr_node)
    }
    async fn cut_non_boundary_dummies(&mut self, prior_excl: &ListNode<LsmElem>) -> CurrNode {
        let mut slice = vec![];

        let mut curr_ptr = SendPtr::from(prior_excl.next.load(Ordering::SeqCst));

        let curr_node_status = loop {
            // let curr_ptr = prev_ref.next.load(Ordering::SeqCst);
            if curr_ptr.as_ptr().is_null() {
                break CurrNodeStatus::EndOfList;
            } else {
                let curr_ref = unsafe { curr_ptr.as_ref() };
                match &curr_ref.elem.content {
                    LsmElemContent::Unit(_) => break CurrNodeStatus::CommittedUnit,
                    LsmElemContent::Dummy {
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
        let penult_list_ver;
        {
            let mut lsm_state = self.db.lsm_state().lock().await;

            penult_list_ver = lsm_state.curr_list_ver;
            *lsm_state.curr_list_ver += 1;

            if let Some(mut r_own) = replacement_node {
                r_own.elem.traversable_list_ver_lo_incl = lsm_state.curr_list_ver;
                r_own.next = AtomicPtr::new(slice_tail_excl.as_ptr_mut());
                let r_ptr = Box::into_raw(r_own);
                slice_head_excl.next.store(r_ptr, Ordering::SeqCst);
            } else {
                slice_head_excl
                    .next
                    .store(slice_tail_excl.as_ptr_mut(), Ordering::SeqCst);
            }
        }

        for dangl_node_ref in slice.into_iter() {
            let node_itv = NodeListVerInterval {
                lo_incl: dangl_node_ref.elem.traversable_list_ver_lo_incl,
                hi_incl: penult_list_ver,
            };
            /* Here, we could check whether the node_itv is already deletable.
            But doing this would hold the mutex guard longer. So, skip it. */
            let dangl_nodes = self.dangling_nodes.entry(node_itv).or_default();
            dangl_nodes.push(SendPtr::from(dangl_node_ref));
        }
    }
}

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
