use crate::ds_n_a::atomic_linked_list::ListSnapshot;
use crate::ds_n_a::interval_set::IntervalSet;
use crate::{
    db_state::DbState,
    lsm::{lsm_state_utils, unit::CommittedUnit, LsmState},
    opers::txn::{CachedSnap, Txn},
    DB,
};
use anyhow::Result;
use std::collections::HashMap;
use tokio::sync::{MutexGuard, RwLockReadGuard};

impl<'txn> Txn<'txn> {
    pub(super) async fn new(db: &'txn DB, db_state_guard: RwLockReadGuard<'txn, DbState>) -> Self {
        let snap_head;
        let snap_commit_ver;
        let snap_list_ver;
        {
            let mut lsm_state = db.lsm_state().lock().await;

            snap_head = Self::hold_snap_head(&lsm_state);

            snap_commit_ver = lsm_state.curr_commit_ver();

            snap_list_ver = lsm_state.hold_curr_list_ver();
        }

        let list_snap = ListSnapshot::new(snap_head, None);
        let snap = CachedSnap::new(list_snap);

        Self {
            db,
            db_state_guard,

            snap,
            snap_commit_ver,
            snap_list_ver,

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
            let lsm_state = self.db.lsm_state().lock().await;

            if self.snap_commit_ver != lsm_state.curr_commit_ver() {
                self.update_snapshot_for_conflict_checking(lsm_state)?;
                if self.has_conflict()? {
                    return Ok(TryCommitResult::Conflict(self));
                }
            } else {
                self.do_commit(lsm_state)?;
                return Ok(TryCommitResult::DidCommit);
            }
        }
    }

    fn update_snapshot_for_conflict_checking(
        &mut self,
        mut lsm_state: MutexGuard<LsmState>,
    ) -> Result<()> {
        let new_snap_head = Self::hold_snap_head(&lsm_state);

        self.snap_commit_ver = lsm_state.curr_commit_ver();

        let updated_mhlv;
        (self.snap_list_ver, updated_mhlv) =
            lsm_state.hold_and_unhold_list_ver(self.snap_list_ver)?;

        drop(lsm_state);

        let is_fc_avail = Self::unhold_boundary_nodes([self.snap.tail_ptr()]);
        let list_snap = ListSnapshot::new(new_snap_head, Some(self.snap.head_ptr()));
        self.snap = CachedSnap::new(list_snap);

        self.notify_fc_worker(updated_mhlv, is_fc_avail);

        Ok(())
    }

    pub(super) async fn reset(&mut self) -> Result<()> {
        let new_snap_head;
        let updated_mhlv;
        {
            let mut lsm_state = self.db.lsm_state().lock().await;

            new_snap_head = Self::hold_snap_head(&lsm_state);

            self.snap_commit_ver = lsm_state.curr_commit_ver();

            (self.snap_list_ver, updated_mhlv) =
                lsm_state.hold_and_unhold_list_ver(self.snap_list_ver)?;
        }

        let is_fc_avail =
            Self::unhold_boundary_nodes([Some(self.snap.head_ptr()), self.snap.tail_ptr()]);
        let list_snap = ListSnapshot::new(new_snap_head, None);
        self.snap = CachedSnap::new(list_snap);

        self.notify_fc_worker(updated_mhlv, is_fc_avail);

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
            Self::unhold_boundary_nodes([Some(self.snap.head_ptr()), self.snap.tail_ptr()]);

        self.notify_fc_worker(updated_mhlv, is_fc_avail);

        if let Some(staging) = self.staging {
            staging.remove_dir()?;
        }

        Ok(())
    }

    fn do_commit(mut self, mut lsm_state: MutexGuard<LsmState>) -> Result<()> {
        let commit_ver = lsm_state.inc_fetch_curr_commit_ver();

        /* Note, converting StagingUnit to CommittedUnit involves writing a file,
        which is not cheap, and we're doing it under a mutex guard. */
        let committed_unit = CommittedUnit::from_staging(self.staging.take().unwrap(), commit_ver)?;

        let node = lsm_state_utils::new_unit_node(committed_unit, 0);
        lsm_state.list().push_head_node(node);

        let updated_mhlv = lsm_state.unhold_list_ver(self.snap_list_ver)?;

        drop(lsm_state);

        Self::unhold_boundary_nodes([Some(self.snap.head_ptr()), self.snap.tail_ptr()]);

        let is_fc_avail = true;
        self.notify_fc_worker(updated_mhlv, is_fc_avail);

        Ok(())
    }
}

pub(super) enum TryCommitResult<'txn> {
    Conflict(Txn<'txn>),
    DidCommit,
}
