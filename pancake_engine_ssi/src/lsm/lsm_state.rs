use crate::ds_n_a::{atomic_linked_list::AtomicLinkedList, multiset::Multiset};
use crate::lsm::unit::{CommitVer, CommittedUnit};
use anyhow::Result;
use std::sync::atomic::{AtomicBool, AtomicU64};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ListVer(u64);

impl ListVer {
    pub const AT_BOOTUP: Self = Self(0);

    pub fn mut_inc(&mut self) {
        self.0 += 1;
    }
}

pub enum LsmElemType {
    CommittedUnit(CommittedUnit),
    Dummy { is_fence: AtomicBool },
}

pub struct LsmElem {
    pub elem_type: LsmElemType,
    pub hold_count: AtomicU64,
}

pub struct LsmState {
    /// From newer to older.
    list: AtomicLinkedList<LsmElem>,

    curr_commit_ver: CommitVer,

    curr_list_ver: ListVer,
    held_list_vers: Multiset<ListVer>,
    min_held_list_ver: ListVer,
}

impl LsmState {
    /// @arg `committed_units`: From newer to older.
    pub fn new(
        committed_units: impl IntoIterator<Item = CommittedUnit>,
        curr_commit_ver: CommitVer,
    ) -> Self {
        let elems = committed_units.into_iter().map(|unit| LsmElem {
            elem_type: LsmElemType::CommittedUnit(unit),
            hold_count: AtomicU64::from(0),
        });
        let list = AtomicLinkedList::from_elems(elems);

        Self {
            list,

            curr_commit_ver,

            curr_list_ver: ListVer::AT_BOOTUP,
            held_list_vers: Multiset::default(),
            min_held_list_ver: ListVer::AT_BOOTUP,
        }
    }

    pub fn list(&self) -> &AtomicLinkedList<LsmElem> {
        &self.list
    }

    pub fn curr_commit_ver(&self) -> CommitVer {
        self.curr_commit_ver
    }

    /// @return The post-bump CommitVer.
    pub fn inc_fetch_curr_commit_ver(&mut self) -> CommitVer {
        self.curr_commit_ver.mut_inc();
        self.curr_commit_ver
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
