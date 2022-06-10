use crate::ds_n_a::atomic_linked_list::ListNode;
use crate::storage::engine_ssi::lsm_state::{
    unit::CommittedUnit, ListVer, LsmElem, LsmElemContent,
};
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize};

pub fn new_dummy_node(
    traversable_list_ver_lo_incl: ListVer,
    hold_count: usize,
    is_fence: bool,
) -> Box<ListNode<LsmElem>> {
    let content = LsmElemContent::Dummy {
        hold_count: AtomicUsize::from(hold_count),
        is_fence: AtomicBool::from(is_fence),
    };
    let elem = LsmElem {
        content,
        traversable_list_ver_lo_incl,
    };
    let node = ListNode {
        elem,
        next: AtomicPtr::default(),
    };
    Box::new(node)
}

pub fn new_unit_node(
    unit: CommittedUnit,
    traversable_list_ver_lo_incl: ListVer,
) -> Box<ListNode<LsmElem>> {
    let content = LsmElemContent::Unit(unit);
    let elem = LsmElem {
        content,
        traversable_list_ver_lo_incl,
    };
    let node = ListNode {
        elem,
        next: AtomicPtr::default(),
    };
    Box::new(node)
}
