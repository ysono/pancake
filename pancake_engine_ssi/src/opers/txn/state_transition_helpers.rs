use crate::ds_n_a::atomic_linked_list::ListNode;
use crate::ds_n_a::send_ptr::NonNullSendPtr;
use crate::{
    lsm::{lsm_state_utils, ListVer, LsmElem},
    opers::txn::Txn,
};
use std::sync::atomic::Ordering;

impl<'txn> Txn<'txn> {
    pub(super) fn create_boundary_node() -> Option<Box<ListNode<LsmElem>>> {
        Some(lsm_state_utils::new_dummy_node(1, false))
    }

    pub(super) fn should_push_boundary_head(elem: &LsmElem) -> bool {
        match elem {
            LsmElem::Dummy { hold_count, .. } => {
                hold_count.fetch_add(1, Ordering::SeqCst);
                false
            }
            LsmElem::CommittedUnit(_) => true,
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
                if let LsmElem::Dummy { hold_count, .. } = &node_ref.elem {
                    let prior_hold_count = hold_count.fetch_sub(1, Ordering::SeqCst);
                    if prior_hold_count == 1 {
                        is_fc_avail = true;
                    }
                }
            }
        }

        is_fc_avail
    }

    pub(super) fn notify_fc_worker(&self, updated_mhlv: Option<ListVer>, is_fc_avail: bool) {
        // Send info about min_held_list_ver first, b/c it's cheaper for the F+C worker to process.
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
