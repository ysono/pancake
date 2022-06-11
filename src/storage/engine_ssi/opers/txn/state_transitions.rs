use crate::ds_n_a::atomic_linked_list::{AtomicLinkedListSnapshot, ListNode};
use crate::ds_n_a::interval_set::IntervalSet;
use crate::storage::engine_ssi::{
    db_state::DbState,
    lsm_state::{unit::CommittedUnit, LsmElem, LsmElemContent, LsmState},
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

            snap_next_commit_ver = lsm_state.next_commit_ver;

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
        if self.staging.is_none() {
            self.close().await?;
            return Ok(TryCommitResult::DidCommit);
        }

        loop {
            let prepped_boundary_node = Self::prep_boundary_node();
            {
                let lsm_state = self.db.lsm_state().lock().await;

                if self.snap_next_commit_ver != lsm_state.next_commit_ver {
                    self.update_snapshot_for_conflict_checking(lsm_state, prepped_boundary_node);
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
    ) {
        let snap_head_excl =
            Self::hold_boundary_at_head(&mut lsm_state, &mut prepped_boundary_node);

        self.snap_next_commit_ver = lsm_state.next_commit_ver;

        let maybe_gc_itv;
        (self.snap_list_ver, maybe_gc_itv) = lsm_state.hold_and_unhold_list_ver(self.snap_list_ver);

        drop(lsm_state);

        let is_replace_avail = Self::unhold_boundary_node(&[self.snap.tail_excl_ptr]);
        self.snap.tail_excl_ptr = Some(self.snap.head_excl_ptr);
        self.snap.head_excl_ptr = snap_head_excl;

        if is_replace_avail {
            self.db.replace_avail_tx().send(()).ok();
        }
        if let Some(gc_itv) = maybe_gc_itv {
            self.db.gc_avail_tx().try_send(gc_itv).ok();
        }
    }

    pub(super) async fn reset(&mut self) -> Result<()> {
        let mut prepped_boundary_node = Self::prep_boundary_node();
        let snap_head_excl;
        let maybe_gc_itv;
        {
            let mut lsm_state = self.db.lsm_state().lock().await;

            snap_head_excl =
                Self::hold_boundary_at_head(&mut lsm_state, &mut prepped_boundary_node);

            self.snap_next_commit_ver = lsm_state.next_commit_ver;

            (self.snap_list_ver, maybe_gc_itv) =
                lsm_state.hold_and_unhold_list_ver(self.snap_list_ver);
        }

        let is_replace_avail =
            Self::unhold_boundary_node(&[Some(self.snap.head_excl_ptr), self.snap.tail_excl_ptr]);
        self.snap.tail_excl_ptr = None;
        self.snap.head_excl_ptr = snap_head_excl;

        if is_replace_avail {
            self.db.replace_avail_tx().send(()).ok();
        }
        if let Some(gc_itv) = maybe_gc_itv {
            self.db.gc_avail_tx().try_send(gc_itv).ok();
        }

        self.snap_vec = None;
        self.dependent_itvs_prim.clear();
        self.dependent_itvs_scnds.clear();
        if let Some(stg) = self.staging.as_mut() {
            stg.clear()?;
        }

        Ok(())
    }

    pub(super) async fn close(self) -> Result<()> {
        let maybe_gc_itv;
        {
            let mut lsm_state = self.db.lsm_state().lock().await;

            maybe_gc_itv = lsm_state.unhold_list_ver(self.snap_list_ver);
        }

        let is_replace_avail =
            Self::unhold_boundary_node(&[Some(self.snap.head_excl_ptr), self.snap.tail_excl_ptr]);

        if is_replace_avail {
            self.db.replace_avail_tx().send(()).ok();
        }
        if let Some(gc_itv) = maybe_gc_itv {
            self.db.gc_avail_tx().try_send(gc_itv).ok();
        }

        if let Some(staging) = self.staging {
            staging.remove_dir()?;
        }

        Ok(())
    }

    fn do_commit(mut self, mut lsm_state: MutexGuard<LsmState>) -> Result<()> {
        /* Push a node with CommittedUnit.
        Note, moving Staging to CommittedUnit is an expensive operation,
        and we're doing it under a mutex guard. */
        let committed_unit =
            CommittedUnit::from_staging(self.staging.take().unwrap(), lsm_state.next_commit_ver)?;
        let elem = LsmElem {
            content: LsmElemContent::Unit(committed_unit),
            traversable_list_ver_lo_incl: lsm_state.curr_list_ver,
        };
        lsm_state.list.push_elem(elem);

        *lsm_state.next_commit_ver += 1;

        let maybe_gc_itv = lsm_state.unhold_list_ver(self.snap_list_ver);

        drop(lsm_state);

        Self::unhold_boundary_node(&[Some(self.snap.head_excl_ptr), self.snap.tail_excl_ptr]);

        /* We just pushed a MemLog. Hence the list became replaceable. */
        self.db.replace_avail_tx().send(()).ok();
        if let Some(gc_itv) = maybe_gc_itv {
            /* If at capacity, we'd like to override an existing message.
            But tokio doesn't seem offer this. So we're willing to drop our new message instead. */
            self.db.gc_avail_tx().try_send(gc_itv).ok();
        }

        Ok(())
    }
}

pub(super) enum TryCommitResult<'txn> {
    Conflict(Txn<'txn>),
    DidCommit,
}
