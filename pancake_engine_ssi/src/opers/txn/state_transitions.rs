use crate::ds_n_a::atomic_linked_list::{AtomicLinkedListSnapshot, ListNode};
use crate::ds_n_a::interval_set::IntervalSet;
use crate::{
    db_state::DbState,
    lsm::{unit::CommittedUnit, LsmElem, LsmState},
    opers::txn::Txn,
    DB,
};
use anyhow::Result;
use std::collections::HashMap;
use tokio::sync::{MutexGuard, RwLockReadGuard};

impl<'txn> Txn<'txn> {
    pub(super) async fn new(
        db: &'txn DB,
        db_state_guard: RwLockReadGuard<'txn, DbState>,
    ) -> Txn<'txn> {
        let mut prepped_boundary_node = Self::prep_boundary_node();
        let snap_head_excl;
        let snap_next_commit_ver;
        let snap_list_ver;
        {
            let mut lsm_state = db.lsm_state().lock().await;

            snap_head_excl =
                Self::hold_boundary_at_head(&mut lsm_state, &mut prepped_boundary_node);

            snap_next_commit_ver = lsm_state.next_commit_ver();

            snap_list_ver = lsm_state.hold_curr_list_ver();
        }

        let snap = AtomicLinkedListSnapshot {
            head_excl_ptr: snap_head_excl,
            tail_excl_ptr: None,
        };

        Self {
            db,
            db_state_guard,

            snap,
            snap_next_commit_ver,
            snap_list_ver,

            snap_vec: None,

            dependent_itvs_prim: IntervalSet::new(),
            dependent_itvs_scnds: HashMap::new(),

            staging: None,
        }
    }

    pub(super) async fn try_commit(mut self) -> Result<TryCommitResult<'txn>> {
        match &mut self.staging {
            None => {
                self.close().await?;
                return Ok(TryCommitResult::DidCommit);
            }
            Some(stg) => {
                stg.flush()?;
            }
        }

        loop {
            let prepped_boundary_node = Self::prep_boundary_node();
            {
                let lsm_state = self.db.lsm_state().lock().await;

                if self.snap_next_commit_ver != lsm_state.next_commit_ver() {
                    self.update_snapshot_for_conflict_checking(lsm_state, prepped_boundary_node)?;
                    if self.has_conflict()? {
                        return Ok(TryCommitResult::Conflict(self));
                    }
                } else {
                    self.do_commit(lsm_state)?;
                    return Ok(TryCommitResult::DidCommit);
                }
            }
        }
    }

    fn update_snapshot_for_conflict_checking(
        &mut self,
        mut lsm_state: MutexGuard<LsmState>,
        mut prepped_boundary_node: Option<Box<ListNode<LsmElem>>>,
    ) -> Result<()> {
        let snap_head_excl =
            Self::hold_boundary_at_head(&mut lsm_state, &mut prepped_boundary_node);

        self.snap_next_commit_ver = lsm_state.next_commit_ver();

        let updated_mhlv;
        (self.snap_list_ver, updated_mhlv) =
            lsm_state.hold_and_unhold_list_ver(self.snap_list_ver)?;

        drop(lsm_state);

        let is_fc_avail = Self::unhold_boundary_node(&[self.snap.tail_excl_ptr]);
        self.snap.tail_excl_ptr = Some(self.snap.head_excl_ptr);
        self.snap.head_excl_ptr = snap_head_excl;

        self.notify_fc_worker(updated_mhlv, is_fc_avail);

        Ok(())
    }

    pub(super) async fn reset(&mut self) -> Result<()> {
        let mut prepped_boundary_node = Self::prep_boundary_node();
        let snap_head_excl;
        let updated_mhlv;
        {
            let mut lsm_state = self.db.lsm_state().lock().await;

            snap_head_excl =
                Self::hold_boundary_at_head(&mut lsm_state, &mut prepped_boundary_node);

            self.snap_next_commit_ver = lsm_state.next_commit_ver();

            (self.snap_list_ver, updated_mhlv) =
                lsm_state.hold_and_unhold_list_ver(self.snap_list_ver)?;
        }

        let is_fc_avail =
            Self::unhold_boundary_node(&[Some(self.snap.head_excl_ptr), self.snap.tail_excl_ptr]);
        self.snap.tail_excl_ptr = None;
        self.snap.head_excl_ptr = snap_head_excl;

        self.notify_fc_worker(updated_mhlv, is_fc_avail);

        self.snap_vec = None;
        self.dependent_itvs_prim.clear();
        self.dependent_itvs_scnds.clear();
        if let Some(stg) = self.staging.as_mut() {
            stg.clear()?;
        }

        Ok(())
    }

    pub(super) async fn close(self) -> Result<()> {
        let updated_mhlv;
        {
            let mut lsm_state = self.db.lsm_state().lock().await;

            updated_mhlv = lsm_state.unhold_list_ver(self.snap_list_ver)?;
        }

        let is_fc_avail =
            Self::unhold_boundary_node(&[Some(self.snap.head_excl_ptr), self.snap.tail_excl_ptr]);

        self.notify_fc_worker(updated_mhlv, is_fc_avail);

        if let Some(staging) = self.staging {
            staging.remove_dir()?;
        }

        Ok(())
    }

    fn do_commit(mut self, mut lsm_state: MutexGuard<LsmState>) -> Result<()> {
        let commit_ver = lsm_state.fetch_inc_next_commit_ver();

        /* Push a node with CommittedUnit.
        Note, moving Staging to CommittedUnit is an expensive operation,
        and we're doing it under a mutex guard. */
        let committed_unit = CommittedUnit::from_staging(self.staging.take().unwrap(), commit_ver)?;
        let elem = LsmElem::Unit(committed_unit);
        lsm_state.list.push_elem(elem);

        let updated_mhlv = lsm_state.unhold_list_ver(self.snap_list_ver)?;

        drop(lsm_state);

        Self::unhold_boundary_node(&[Some(self.snap.head_excl_ptr), self.snap.tail_excl_ptr]);

        // We just pushed a MemLog. Hence the list became replaceable.
        self.notify_fc_worker(updated_mhlv, true);

        Ok(())
    }
}

pub(super) enum TryCommitResult<'txn> {
    Conflict(Txn<'txn>),
    DidCommit,
}
