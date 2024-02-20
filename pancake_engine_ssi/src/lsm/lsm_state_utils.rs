use crate::ds_n_a::atomic_linked_list::ListNode;
use crate::lsm::{unit::CommittedUnit, LsmElem, LsmElemType};
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64};

pub fn new_dummy_node(is_fence: bool, hold_count: u64) -> Box<ListNode<LsmElem>> {
    let elem_type = LsmElemType::Dummy {
        is_fence: AtomicBool::from(is_fence),
    };
    let elem = LsmElem {
        elem_type,
        hold_count: AtomicU64::from(hold_count),
    };
    let node = ListNode {
        elem,
        next: AtomicPtr::default(),
    };
    Box::new(node)
}

pub fn new_unit_node(unit: CommittedUnit, hold_count: u64) -> Box<ListNode<LsmElem>> {
    let elem_type = LsmElemType::CommittedUnit(unit);
    let elem = LsmElem {
        elem_type,
        hold_count: AtomicU64::from(hold_count),
    };
    let node = ListNode {
        elem,
        next: AtomicPtr::default(),
    };
    Box::new(node)
}
