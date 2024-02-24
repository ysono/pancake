use crate::ds_n_a::{
    atomic_linked_list::{AtomicLinkedList, ListNode},
    multiset::Multiset,
    ordered_dict::OrderedDict,
    send_ptr::NonNullSendPtr,
};
use crate::lsm::unit::{CommitVer, CommittedUnit, StagingUnit};
use anyhow::{anyhow, Result};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ListVer(u64);

impl ListVer {
    pub const AT_BOOTUP: Self = Self(0);

    pub fn mut_inc(&mut self) {
        self.0 += 1;
    }
}

pub struct Boundary {
    pub hold_count: u32,
    pub node_newer: NonNullSendPtr<ListNode<CommittedUnit>>,
}

pub struct LsmState {
    /// From newer to older.
    list: AtomicLinkedList<CommittedUnit>,

    curr_commit_ver: CommitVer,
    curr_commit_ver_hold_count: u32,
    boundaries: OrderedDict<CommitVer, Boundary>,

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
        let list = AtomicLinkedList::from_elems(committed_units.into_iter());

        Self {
            list,

            curr_commit_ver,
            curr_commit_ver_hold_count: 0,
            boundaries: OrderedDict::new(),

            curr_list_ver: ListVer::AT_BOOTUP,
            held_list_vers: Multiset::default(),
            min_held_list_ver: ListVer::AT_BOOTUP,
        }
    }

    pub fn list(&self) -> &AtomicLinkedList<CommittedUnit> {
        &self.list
    }

    pub fn list_mut(&mut self) -> &mut AtomicLinkedList<CommittedUnit> {
        &mut self.list
    }

    pub fn boundaries(&self) -> &OrderedDict<CommitVer, Boundary> {
        &self.boundaries
    }

    pub fn boundaries_mut(&mut self) -> &mut OrderedDict<CommitVer, Boundary> {
        &mut self.boundaries
    }

    pub fn curr_commit_ver(&self) -> CommitVer {
        self.curr_commit_ver
    }

    pub fn hold_curr_commit_ver(&mut self) -> CommitVer {
        self.curr_commit_ver_hold_count += 1;
        self.curr_commit_ver
    }

    /// @return [`CommitVer`]s that are members of newly-found isolated sequences of non-held boundaries.
    pub fn unhold_commit_vers<const LEN: usize>(
        &mut self,
        mut arg_vers: [Option<CommitVer>; LEN],
    ) -> Result<[Option<CommitVer>; LEN]> {
        for maybe_arg_ver in arg_vers.iter_mut() {
            if let Some(arg_ver) = maybe_arg_ver {
                let is_fc_able = self.unhold_commit_ver(arg_ver)?;
                if is_fc_able == false {
                    *maybe_arg_ver = None;
                }
            }
        }
        Ok(arg_vers)
    }

    /// @return Whether (the unholding caused a non-head boundary to become non-held) &&
    /// and (the non-held boundary is isolatedly non-held).
    ///
    /// We must unhold and check for isolated-non-held-ness back-to-back,
    /// when we're unholding multiple CommitVers,
    /// to ensure that among 2 adjacent unheld CommitVers that become isolated-non-held, 1 of them is detected.
    pub fn unhold_commit_ver(&mut self, arg_ver: &CommitVer) -> Result<bool> {
        if arg_ver == &self.curr_commit_ver {
            self.curr_commit_ver_hold_count -= 1;

            return Ok(false);
        } else {
            let boundary = self
                .boundaries
                .get_mut(arg_ver)
                .ok_or_else(|| anyhow!("Unholding a non-existent boundary."))?;
            boundary.hold_count -= 1;

            if boundary.hold_count == 0 {
                let neibs = self.boundaries.get_neighbors(arg_ver).unwrap();
                let is_isolated_from = |neib: Option<(&CommitVer, &Boundary)>| match neib {
                    None => true,
                    Some((_, Boundary { hold_count, .. })) => *hold_count > 0,
                };
                if is_isolated_from(neibs.newer) && is_isolated_from(neibs.older) {
                    return Ok(true);
                }
            }
            return Ok(false);
        }
    }

    pub fn bump_commit_ver(&mut self, staging_unit: StagingUnit) -> Result<()> {
        /* Save pre-bump info. */

        let penult_commit_ver = self.curr_commit_ver;
        let new_commit_ver = self.curr_commit_ver.new_inc();

        let penult_commit_ver_hold_count = self.curr_commit_ver_hold_count;

        /* Write CommitInfo. This is the only I/O operation, which can fail, so do this first. */

        let committed_unit = CommittedUnit::from_staging(staging_unit, new_commit_ver)?;

        let new_node_own = ListNode::new(committed_unit);

        /* Initialize new_commit_ver. */

        self.curr_commit_ver = new_commit_ver;

        self.curr_commit_ver_hold_count = 0;

        let new_node_ptr = self.list.push_head_node_noncontested(new_node_own);

        /* Create a boundary at penult_commit_ver.
        Do this even if penult_commit_ver_hold_count == 0. */

        self.boundaries.insert(
            penult_commit_ver,
            Boundary {
                hold_count: penult_commit_ver_hold_count,
                node_newer: new_node_ptr,
            },
        );

        Ok(())
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
