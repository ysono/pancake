use crate::ds_n_a::atomic_linked_list::ListNode;
use crate::ds_n_a::send_ptr::SendPtr;
use crate::storage::engine_ssi::{
    lsm_state::{unit::unit_utils, LsmElem, LsmElemContent, LsmState, LIST_VER_PLACEHOLDER},
    opers::txn::Txn,
};
use std::sync::atomic::Ordering;
use tokio::sync::MutexGuard;

impl<'txn> Txn<'txn> {
    pub(super) fn prep_boundary_node() -> Option<Box<ListNode<LsmElem>>> {
        Some(unit_utils::new_dummy_node(LIST_VER_PLACEHOLDER, 1, false))
    }

    /// This helper ensures that the head is a dummy with one additional hold_count.
    ///
    /// The arg `prepped_boundary_node`:
    /// - Caller should malloc'd the node outside the mutex guard.
    /// - The arg is an `&mut Option<_>` so that it can be freed outside the mutex guard.
    ///
    /// Returns a non-null ptr to the resulting head.
    pub(super) fn hold_boundary_at_head<'a>(
        lsm_state: &mut MutexGuard<'a, LsmState>,
        prepped_boundary_node: &mut Option<Box<ListNode<LsmElem>>>,
    ) -> SendPtr<ListNode<LsmElem>> {
        let update_or_provide_head = |content: Option<&LsmElemContent>| match content {
            Some(LsmElemContent::Dummy { hold_count, .. }) => {
                hold_count.fetch_add(1, Ordering::SeqCst);
                return None;
            }
            _ => {
                let mut new_head = prepped_boundary_node.take().unwrap();
                new_head.elem.traversable_list_ver_lo_incl = lsm_state.curr_list_ver;
                return Some(new_head);
            }
        };
        let snap_head_excl = SendPtr::from(lsm_state.update_or_push(update_or_provide_head));
        snap_head_excl
    }

    /// Returns whether either node became non-held, hence LL replacement can be done.
    pub(super) fn unhold_boundary_node(node_ptrs: &[Option<SendPtr<ListNode<LsmElem>>>]) -> bool {
        let mut is_replace_avail = false;

        let mut do_unhold = |node_ptr: SendPtr<ListNode<LsmElem>>| {
            let node_ref = unsafe { node_ptr.as_ref() };
            if let LsmElemContent::Dummy { hold_count, .. } = &node_ref.elem.content {
                let prior_hold_count = hold_count.fetch_sub(1, Ordering::SeqCst);
                if prior_hold_count == 1 {
                    is_replace_avail |= true;
                }
            }
        };

        for node_ptr in node_ptrs {
            if let Some(node_ptr) = node_ptr {
                do_unhold(node_ptr.clone());
            }
        }

        is_replace_avail
    }
}
