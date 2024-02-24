use crate::ds_n_a::interval_set::IntervalSet;
use crate::{
    db_state::DbState,
    lsm::LsmState,
    opers::txn::{CachedSnap, Txn},
    DB,
};
use anyhow::Result;
use std::collections::HashMap;
use tokio::sync::{MutexGuard, RwLockReadGuard};

impl<'txn> Txn<'txn> {
    pub(super) async fn new(db: &'txn DB, db_state_guard: RwLockReadGuard<'txn, DbState>) -> Self {
        let snap_commit_ver_hi_incl;
        let list_snap;
        let snap_list_ver;
        {
            let mut lsm_state = db.lsm_state().lock().await;

            snap_commit_ver_hi_incl = lsm_state.hold_curr_commit_ver();

            list_snap = lsm_state.list().snap();

            snap_list_ver = lsm_state.hold_curr_list_ver();
        }

        let snap = CachedSnap::new(snap_commit_ver_hi_incl, None, list_snap);

        Self {
            db,
            db_state_guard,

            snap,
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

            if self.snap.commit_ver_hi_incl != lsm_state.curr_commit_ver() {
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
        let snap_commit_ver_hi_incl = lsm_state.hold_curr_commit_ver();

        let fc_able_commit_vers = lsm_state.unhold_commit_vers([self.snap.commit_ver_lo_excl])?;

        let list_snap = lsm_state.list().snap();

        let (snap_list_ver, updated_mhlv) =
            lsm_state.hold_and_unhold_list_ver(self.snap_list_ver)?;

        drop(lsm_state);

        self.snap = CachedSnap::new(
            snap_commit_ver_hi_incl,
            Some(self.snap.commit_ver_hi_incl),
            list_snap,
        );

        self.snap_list_ver = snap_list_ver;

        self.notify_fc_worker(updated_mhlv, fc_able_commit_vers);

        Ok(())
    }

    pub(super) async fn reset(&mut self) -> Result<()> {
        let snap_commit_ver_hi_incl;
        let fc_able_commit_vers;
        let list_snap;
        let (snap_list_ver, updated_mhlv);
        {
            let mut lsm_state = self.db.lsm_state().lock().await;

            snap_commit_ver_hi_incl = lsm_state.hold_curr_commit_ver();

            fc_able_commit_vers = lsm_state.unhold_commit_vers([
                Some(self.snap.commit_ver_hi_incl),
                self.snap.commit_ver_lo_excl,
            ])?;

            list_snap = lsm_state.list().snap();

            (snap_list_ver, updated_mhlv) =
                lsm_state.hold_and_unhold_list_ver(self.snap_list_ver)?;
        }

        self.snap = CachedSnap::new(snap_commit_ver_hi_incl, None, list_snap);

        self.snap_list_ver = snap_list_ver;

        self.notify_fc_worker(updated_mhlv, fc_able_commit_vers);

        self.dependent_itvs_prim.clear();
        self.dependent_itvs_scnds.clear();
        if let Some(stg) = self.staging.as_mut() {
            stg.clear()?;
        }

        Ok(())
    }

    pub(super) async fn close(self) -> Result<()> {
        let fc_able_commit_vers;
        let updated_mhlv;
        {
            let mut lsm_state = self.db.lsm_state().lock().await;

            fc_able_commit_vers = lsm_state.unhold_commit_vers([
                Some(self.snap.commit_ver_hi_incl),
                self.snap.commit_ver_lo_excl,
            ])?;

            updated_mhlv = lsm_state.unhold_list_ver(self.snap_list_ver)?;
        }

        self.notify_fc_worker(updated_mhlv, fc_able_commit_vers);

        if let Some(staging) = self.staging {
            staging.remove_dir()?;
        }

        Ok(())
    }

    fn do_commit(mut self, mut lsm_state: MutexGuard<LsmState>) -> Result<()> {
        let stg = self.staging.take().unwrap();
        lsm_state.bump_commit_ver(stg)?;

        let fc_able_commit_vers = lsm_state.unhold_commit_vers([
            Some(self.snap.commit_ver_hi_incl),
            self.snap.commit_ver_lo_excl,
        ])?;

        let updated_mhlv = lsm_state.unhold_list_ver(self.snap_list_ver)?;

        drop(lsm_state);

        self.notify_fc_worker(updated_mhlv, fc_able_commit_vers);

        Ok(())
    }
}

pub(super) enum TryCommitResult<'txn> {
    Conflict(Txn<'txn>),
    DidCommit,
}
