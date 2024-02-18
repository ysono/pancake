use crate::ds_n_a::{
    atomic_linked_list::{AtomicLinkedList, ListNode},
    multiset::Multiset,
    send_ptr::NonNullSendPtr,
};
use crate::lsm::unit::{CommitVer, CommittedUnit};
use anyhow::Result;
use std::sync::atomic::{AtomicBool, AtomicUsize};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ListVer(u64);

impl ListVer {
    pub const AT_BOOTUP: Self = Self(0);

    pub fn mut_inc(&mut self) {
        self.0 += 1;
    }
}

pub enum LsmElem {
    CommittedUnit(CommittedUnit),
    Dummy {
        hold_count: AtomicUsize,
        is_fence: AtomicBool,
    },
}

pub struct LsmState {
    /// From newer to older.
    list: AtomicLinkedList<LsmElem>,

    next_commit_ver: CommitVer,

    curr_list_ver: ListVer,
    held_list_vers: Multiset<ListVer>,
    min_held_list_ver: ListVer,
}

impl LsmState {
    /// @arg `committed_units`: From newer to older.
    pub fn new(
        committed_units: impl IntoIterator<Item = CommittedUnit>,
        next_commit_ver: CommitVer,
    ) -> Self {
        let elems = committed_units.into_iter().map(LsmElem::CommittedUnit);
        let list = AtomicLinkedList::from_elems(elems);

        Self {
            list,

            next_commit_ver,

            curr_list_ver: ListVer::AT_BOOTUP,
            held_list_vers: Multiset::default(),
            min_held_list_ver: ListVer::AT_BOOTUP,
        }
    }

    pub fn list(&self) -> &AtomicLinkedList<LsmElem> {
        &self.list
    }

    /// Non-atomically does the following:
    /// 1. Retrieve the head.
    /// 1. If there is no head, or if @arg `should_push` inspects the head element and returns `true`,
    ///     then push @arg `candidate_head` at the head.
    ///
    /// When @arg `should_push` is given a [`LsmElem`] to read,
    /// the callback is allowed to modify the [`LsmElem`], using interior mutability.
    ///
    /// [`LsmState`] is, in practice, guarded by a mutex.
    /// The caller should allocate the candidate node outside the mutex guard.
    /// This is why @arg `candidate_head` is required.
    ///
    /// @return
    /// - tup.0 = Pointer to the latest head node, guaranteed to be non-null.
    /// - tup.1 = The @arg `candidate_head`, iff it was not pushed.
    ///     The caller should subsequently free it outside the mutex guard.
    ///     (Or, return it to a pool of nodes. TODO.)
    pub fn update_or_push_head<F>(
        &self,
        should_push: F,
        candidate_head: Box<ListNode<LsmElem>>,
    ) -> (
        NonNullSendPtr<ListNode<LsmElem>>,
        Option<Box<ListNode<LsmElem>>>,
    )
    where
        F: FnOnce(&LsmElem) -> bool,
    {
        let maybe_head_ptr = self.list.head_node_ptr();
        match maybe_head_ptr {
            None => (self.list.push_head_node(candidate_head).into(), None),
            Some(head_ptr) => {
                let head_ref = unsafe { head_ptr.as_ref() };
                let do_push = should_push(&head_ref.elem);
                if do_push {
                    (self.list.push_head_node(candidate_head).into(), None)
                } else {
                    (head_ptr.into(), Some(candidate_head))
                }
            }
        }
    }

    pub fn next_commit_ver(&self) -> CommitVer {
        self.next_commit_ver
    }

    /// @return The previously "next", newly "curr", CommitVer.
    pub fn fetch_inc_next_commit_ver(&mut self) -> CommitVer {
        let curr = self.next_commit_ver;
        self.next_commit_ver.mut_inc();
        curr
    }

    /// @return
    /// - tup.0 = The previously "curr", newly "penultimate", ListVer.
    /// - tup.1 = The updated min_held_list_ver, iff updated.
    pub fn fetch_inc_curr_list_ver(&mut self) -> (ListVer, Option<ListVer>) {
        let penult = self.curr_list_ver;
        self.curr_list_ver.mut_inc();

        let updated_mhlv = self.advance_min_held_list_ver();

        (penult, updated_mhlv)
    }

    /// @return The curr_list_ver.
    pub fn hold_curr_list_ver(&mut self) -> ListVer {
        self.held_list_vers.add(self.curr_list_ver);
        self.curr_list_ver
    }

    /// @return The updated min_held_list_ver, iff updated.
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
            self.min_held_list_ver.mut_inc();
            did_change = true;
        }

        if did_change {
            Some(self.min_held_list_ver)
        } else {
            None
        }
    }

    /// @return
    /// - tup.0 = The curr_list_ver.
    /// - tup.1 = The updated min_held_list_ver, iff updated.
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
}
