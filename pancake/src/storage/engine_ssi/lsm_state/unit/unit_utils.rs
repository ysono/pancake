use crate::ds_n_a::atomic_linked_list::ListNode;
use crate::storage::engine_ssi::lsm_state::{unit::CommittedUnit, LsmElem};
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize};

pub fn new_dummy_node(hold_count: usize, is_fence: bool) -> Box<ListNode<LsmElem>> {
    let elem = LsmElem::Dummy {
        hold_count: AtomicUsize::from(hold_count),
        is_fence: AtomicBool::from(is_fence),
    };
    let node = ListNode {
        elem,
        next: AtomicPtr::default(),
    };
    Box::new(node)
}

pub fn new_unit_node(unit: CommittedUnit) -> Box<ListNode<LsmElem>> {
    let elem = LsmElem::Unit(unit);
    let node = ListNode {
        elem,
        next: AtomicPtr::default(),
    };
    Box::new(node)
}
