use crate::ds_n_a::{
    atomic_linked_list::{AtomicLinkedList, ListNode},
    multiset::Multiset,
};
use crate::lsm::unit::{CommitVer, CommittedUnit};
use anyhow::Result;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicUsize};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ListVer(u64);

impl ListVer {
    pub const AT_BOOTUP: Self = Self(0);

    pub fn inc(self) -> Self {
        Self(self.0 + 1)
    }
}

pub enum LsmElem {
    Unit(CommittedUnit),
    Dummy {
        hold_count: AtomicUsize,
        is_fence: AtomicBool,
    },
}

pub struct LsmState {
    /// From newer to older.
    pub list: AtomicLinkedList<LsmElem>,

    next_commit_ver: CommitVer,

    curr_list_ver: ListVer,
    held_list_vers: Multiset<ListVer>,
    min_held_list_ver: ListVer,
}

impl LsmState {
    /// @arg committed_units: From newer to older.
    pub fn new(
        committed_units: impl IntoIterator<Item = CommittedUnit>,
        next_commit_ver: CommitVer,
    ) -> Self {
        let list_elems = committed_units.into_iter().map(LsmElem::Unit);
        let list = AtomicLinkedList::from_elems(list_elems);

        Self {
            list,

            next_commit_ver,

            curr_list_ver: ListVer::AT_BOOTUP,
            held_list_vers: Multiset::default(),
            min_held_list_ver: ListVer::AT_BOOTUP,
        }
    }

    pub fn next_commit_ver(&self) -> CommitVer {
        self.next_commit_ver
    }

    /// Returns the previously "next", newly "curr", CommitVer.
    pub fn fetch_inc_next_commit_ver(&mut self) -> CommitVer {
        let curr = self.next_commit_ver;
        self.next_commit_ver = self.next_commit_ver.inc();
        curr
    }

    /// Returns the previously "curr", newly "penultimate", ListVer.
    pub fn fetch_inc_curr_list_ver(&mut self) -> ListVer {
        let penult = self.curr_list_ver;
        self.curr_list_ver = self.curr_list_ver.inc();
        penult
    }

    /// Returns the curr_list_ver.
    pub fn hold_curr_list_ver(&mut self) -> ListVer {
        self.held_list_vers.add(self.curr_list_ver);
        self.curr_list_ver
    }

    /// Returns the updated min_held_list_ver, iff updated.
    pub fn unhold_list_ver(&mut self, arg_ver: ListVer) -> Result<Option<ListVer>> {
        let count = self.held_list_vers.remove(&arg_ver)?;
        if (count == 0) && (arg_ver == self.min_held_list_ver) {
            return Ok(self.advance_min_held_list_ver());
        }
        return Ok(None);
    }

    fn advance_min_held_list_ver(&mut self) -> Option<ListVer> {
        let mut did_change = false;
        while (self.min_held_list_ver < self.curr_list_ver)
            && (self.held_list_vers.contains(&self.min_held_list_ver) == false)
        {
            self.min_held_list_ver = self.min_held_list_ver.inc();
            did_change = true;
        }

        if did_change {
            Some(self.min_held_list_ver)
        } else {
            None
        }
    }

    /// Returns
    /// - the curr_list_ver.
    /// - the updated min_held_list_ver, iff updated.
    pub fn hold_and_unhold_list_ver(
        &mut self,
        prior: ListVer,
    ) -> Result<(ListVer, Option<ListVer>)> {
        let mut updated_mhlv = None;
        if prior != self.curr_list_ver {
            updated_mhlv = self.unhold_list_ver(prior)?;
            self.hold_curr_list_ver();
        }
        return Ok((self.curr_list_ver, updated_mhlv));
    }

    pub fn is_held_list_vers_empty(&self) -> bool {
        self.held_list_vers.len() == 0
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
