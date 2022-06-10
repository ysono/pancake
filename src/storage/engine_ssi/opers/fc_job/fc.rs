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

        self.merge_segments(unsafe { snap_head_excl.as_ref() })
            .await?;

        Ok(())
    }

    pub(super) async fn merge_segments(
        &mut self,
        snap_head_excl: &ListNode<LsmElem>,
    ) -> Result<()> {
        let mut segm_head_excl = snap_head_excl;
        let mut curr_ptr = SendPtr::from(segm_head_excl.next.load(Ordering::SeqCst));

        loop {
            let slice = self
                .collect_one_segment(segm_head_excl, &mut curr_ptr)
                .await;

            // Note, each node in slice should have a CommittedUnit.
            let units = slice.iter().filter_map(|node| match &node.elem.content {
                LsmElemContent::Unit(unit) => Some(unit),
                LsmElemContent::Dummy { .. } => None,
            });
            let skip_tombstones = curr_ptr.as_ptr().is_null();
            if let Some(unit) = self.do_flush_and_compact(units, skip_tombstones).await? {
                let node = unit_utils::new_unit_node(unit, LIST_VER_PLACEHOLDER);
                self.replace(segm_head_excl, curr_ptr, Some(node), slice)
                    .await;
            }

            if curr_ptr.as_ptr().is_null() {
                break;
            } else {
                segm_head_excl = unsafe { curr_ptr.as_ref() };
                curr_ptr = SendPtr::from(segm_head_excl.next.load(Ordering::SeqCst));
            }
        }

        Ok(())
    }

    async fn collect_one_segment(
        &mut self,
        segm_head_excl: &ListNode<LsmElem>,
        curr_ptr_inplace: &mut SendPtr<ListNode<LsmElem>>,
    ) -> Vec<&'static ListNode<LsmElem>> {
        let mut slice = vec![];
        loop {
            self.cut_non_boundary_dummies(segm_head_excl, curr_ptr_inplace)
                .await;

            if curr_ptr_inplace.as_ptr().is_null() {
                break;
            } else {
                let curr_ref = unsafe { curr_ptr_inplace.as_ref() };
                match &curr_ref.elem.content {
                    LsmElemContent::Dummy { .. } => {
                        // The curr dummy could not be cut. Therefore it must be a boundary.
                        break;
                    }
                    LsmElemContent::Unit(_) => {
                        slice.push(curr_ref);
                        *curr_ptr_inplace = SendPtr::from(curr_ref.next.load(Ordering::SeqCst));
                    }
                }
            }
        }
        slice
    }

    async fn cut_non_boundary_dummies(
        &mut self,
        segm_head_excl: &ListNode<LsmElem>,
        curr_ptr_inplace: &mut SendPtr<ListNode<LsmElem>>,
    ) {
        let mut slice = vec![];
        loop {
            if curr_ptr_inplace.as_ptr().is_null() {
                break;
            } else {
                let curr_ref = unsafe { curr_ptr_inplace.as_ref() };
                match &curr_ref.elem.content {
                    LsmElemContent::Dummy {
                        hold_count,
                        is_fence,
                    } => {
                        if hold_count.load(Ordering::SeqCst) == 0
                            && is_fence.load(Ordering::SeqCst) == false
                        {
                            slice.push(curr_ref);
                            *curr_ptr_inplace = SendPtr::from(curr_ref.next.load(Ordering::SeqCst));
                        } else {
                            break;
                        }
                    }
                    LsmElemContent::Unit(_) => break,
                }
            }
        }
        if !slice.is_empty() {
            self.replace(segm_head_excl, *curr_ptr_inplace, None, slice)
                .await;
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
