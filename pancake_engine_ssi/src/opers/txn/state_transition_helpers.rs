use crate::ds_n_a::atomic_linked_list::ListNode;
use crate::ds_n_a::send_ptr::NonNullSendPtr;
use crate::{
    lsm::{lsm_state_utils, ListVer, LsmElem, LsmState},
    opers::txn::Txn,
};
use std::sync::atomic::Ordering;
use tokio::sync::MutexGuard;

impl<'txn> Txn<'txn> {
    pub(super) fn hold_snap_head(
        lsm_state: &MutexGuard<LsmState>,
    ) -> NonNullSendPtr<ListNode<LsmElem>> {
        match lsm_state.list().head_node_ptr() {
            Some(snap_head_ptr) => {
                let snap_head_ref = unsafe { snap_head_ptr.as_ref() };
                snap_head_ref.elem.hold_count.fetch_add(1, Ordering::SeqCst);
                NonNullSendPtr::from(snap_head_ptr)
            }
            None => {
                let dummy = lsm_state_utils::new_dummy_node(false, 1);
                let snap_head_ptr = lsm_state.list().push_head_node(dummy);
                NonNullSendPtr::from(snap_head_ptr)
            }
        }
    }

    /// @return Whether any of the arg nodes became non-held, i.e. whether the LL became F+C'able.
    pub(super) fn unhold_boundary_nodes<const LEN: usize>(
        node_ptrs: [Option<NonNullSendPtr<ListNode<LsmElem>>>; LEN],
    ) -> bool {
        let mut is_fc_avail = false;

        for node_ptr in node_ptrs {
            if let Some(node_ptr) = node_ptr {
                let node_ref = unsafe { node_ptr.as_ref() };
                let prior_hold_count = node_ref.elem.hold_count.fetch_sub(1, Ordering::SeqCst);
                if prior_hold_count == 1 {
                    is_fc_avail = true;
                }
            }
        }

        is_fc_avail
    }

    pub(super) fn notify_fc_worker(&self, updated_mhlv: Option<ListVer>, is_fc_avail: bool) {
        /* Send info about min_held_list_ver first, b/c it's cheaper for the F+C worker to process. */
        self.send_updated_min_held_list_ver(updated_mhlv);
        self.send_fc_avail(is_fc_avail);
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
    fn send_fc_avail(&self, is_fc_avail: bool) {
        if is_fc_avail {
            self.db.fc_avail_tx().send(()).ok();
        }
    }
}
