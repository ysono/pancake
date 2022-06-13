use crate::ds_n_a::atomic_linked_list::{AtomicLinkedList, ListNode};
use crate::storage::engine_ssi::lsm_state::unit::{CommitVer, CommittedUnit};
use derive_more::{Deref, DerefMut};
use std::collections::HashMap;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicUsize};

#[derive(Deref, DerefMut, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ListVer(u64);

pub const LIST_VER_INITIAL: ListVer = ListVer(0);

pub enum LsmElem {
    Unit(CommittedUnit),
    Dummy {
        hold_count: AtomicUsize,
        is_fence: AtomicBool,
    },
}

pub struct LsmState {
    pub list: AtomicLinkedList<LsmElem>,

    pub next_commit_ver: CommitVer,

    curr_list_ver: ListVer,
    held_list_vers: HashMap<ListVer, usize>,
    min_held_list_ver: ListVer,
}

impl LsmState {
    pub fn new(committed_units: Vec<CommittedUnit>, next_commit_ver: CommitVer) -> Self {
        let list_elems = committed_units.into_iter().map(LsmElem::Unit);
        let list = AtomicLinkedList::from_elems(list_elems);

        Self {
            list,

            next_commit_ver,

            curr_list_ver: LIST_VER_INITIAL,
            held_list_vers: Default::default(),
            min_held_list_ver: LIST_VER_INITIAL,
        }
    }

    /// Returns the previously curr, now penultimate, list_ver.
    pub fn bump_curr_list_ver(&mut self) -> ListVer {
        let penult = self.curr_list_ver;
        *self.curr_list_ver += 1;
        penult
    }

    /// Returns the curr_list_ver.
    pub fn hold_curr_list_ver(&mut self) -> ListVer {
        self.held_list_vers
            .entry(self.curr_list_ver)
            .or_insert_with(|| 1);
        self.curr_list_ver
    }

    /// Returns
    /// - an updated min_held_list_ver, if updated.
    pub fn unhold_list_ver(&mut self, arg_ver: ListVer) -> Option<ListVer> {
        match self.held_list_vers.get_mut(&arg_ver) {
            None => {
                return None;
            }
            Some(count) => {
                if *count != 1 {
                    *count -= 1;
                    return None;
                } else {
                    self.held_list_vers.remove(&arg_ver);

                    let orig_mhlv = self.min_held_list_ver;
                    while self.min_held_list_ver < self.curr_list_ver
                        && self.held_list_vers.get(&self.min_held_list_ver).is_none()
                    {
                        *self.min_held_list_ver += 1;
                    }
                    if orig_mhlv != self.min_held_list_ver {
                        return Some(self.min_held_list_ver);
                    } else {
                        return None;
                    }
                }
            }
        }
    }

    /// Returns
    /// - the curr_list_ver.
    /// - an updated min_held_list_ver, if updated.
    pub fn hold_and_unhold_list_ver<'a>(&mut self, prior: ListVer) -> (ListVer, Option<ListVer>) {
        let mut updated_mhlv = None;
        if prior != self.curr_list_ver {
            updated_mhlv = self.unhold_list_ver(prior);
            self.hold_curr_list_ver();
        }
        return (self.curr_list_ver, updated_mhlv);
    }

    pub fn is_held_list_vers_empty(&self) -> bool {
        self.held_list_vers.is_empty()
    }

    /// Non-atomically does the following:
    /// 1. Get the head.
    /// 1. Callback reads the head and determines whether to push a new node.
    /// 1. If there is a new node to push, push it.
    /// Returns the resulting head.
    pub fn update_or_push<Cb>(&self, update_or_provide_head: Cb) -> *const ListNode<LsmElem>
    where
        Cb: FnOnce(Option<&LsmElem>) -> Option<Box<ListNode<LsmElem>>>,
    {
        let maybe_head_ref = self.list.head();
        let maybe_head_elem = maybe_head_ref.map(|head_ref| &head_ref.elem);
        if let Some(new_node) = update_or_provide_head(maybe_head_elem) {
            let new_head_ptr = self.list.push_node(new_node);
            return new_head_ptr;
        } else {
            match maybe_head_ref {
                None => return ptr::null(),
                Some(head_ref) => return head_ref as *const _,
            }
        }
    }
}
