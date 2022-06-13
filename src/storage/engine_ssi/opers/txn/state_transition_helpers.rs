use crate::ds_n_a::atomic_linked_list::ListNode;
use crate::ds_n_a::send_ptr::SendPtr;
use crate::storage::engine_ssi::{
    lsm_state::{unit::unit_utils, ListVer, LsmElem, LsmState},
    opers::txn::Txn,
};
use std::sync::atomic::Ordering;
use tokio::sync::MutexGuard;

impl<'txn> Txn<'txn> {
    pub(super) fn prep_boundary_node() -> Option<Box<ListNode<LsmElem>>> {
        Some(unit_utils::new_dummy_node(1, false))
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
        let update_or_provide_head = |elem: Option<&LsmElem>| match elem {
            Some(LsmElem::Dummy { hold_count, .. }) => {
                hold_count.fetch_add(1, Ordering::SeqCst);
                return None;
            }
            _ => {
                return prepped_boundary_node.take();
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
            if let LsmElem::Dummy { hold_count, .. } = &node_ref.elem {
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

    pub(super) fn notify_fc_job(&self, updated_mhlv: Option<ListVer>, is_replace_avail: bool) {
        // Send info about min_held_list_ver first, b/c it's cheaper for the F+C job to process.
        self.send_updated_min_held_list_ver(updated_mhlv);
        self.send_replace_avail(is_replace_avail);
    }
    fn send_updated_min_held_list_ver(&self, updated_mhlv: Option<ListVer>) {
        if let Some(mhlv) = updated_mhlv {
            self.db
                .min_held_list_ver_tx()
                .send_if_modified(|prior_mhlv| {
                    if *prior_mhlv < mhlv {
                        *prior_mhlv = mhlv;
                        true
                    } else {
                        false
                    }
                });
        }
    }
    fn send_replace_avail(&self, is_replace_avail: bool) {
        if is_replace_avail {
            self.db.replace_avail_tx().send(()).ok();
        }
    }
}
