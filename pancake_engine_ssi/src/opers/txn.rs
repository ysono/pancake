use crate::{
    db_state::{DbState, ScndIdxNum},
    ds_n_a::{
        atomic_linked_list::{ListIterator, ListSnapshot},
        interval_set::IntervalSet,
        iterator_cache::IteratorCache,
    },
    lsm::{
        unit::{CommitVer, CommittedUnit, StagingUnit},
        ListVer,
    },
    DB,
};
use anyhow::{anyhow, Result};
use pancake_types::types::{PrimaryKey, SubValue};
use std::collections::HashMap;
use tokio::sync::RwLockReadGuard;

mod conflict;
mod state_transition_helpers;
mod state_transitions;
mod stmt;

use state_transitions::TryCommitResult;

pub enum ClientCommitDecision<ClientOk> {
    Commit(ClientOk),
    Abort(ClientOk),
}

pub struct Txn<'txn> {
    db: &'txn DB,
    db_state_guard: RwLockReadGuard<'txn, DbState>,

    snap: CachedSnap,
    snap_list_ver: ListVer,

    dependent_itvs_prim: IntervalSet<&'txn PrimaryKey>,
    dependent_itvs_scnds: HashMap<ScndIdxNum, IntervalSet<&'txn SubValue>>,

    staging: Option<StagingUnit>,
}

impl<'txn> Txn<'txn> {
    pub async fn run<ClientOk>(
        db: &'txn DB,
        retry_limit: usize,
        mut client_fn: impl FnMut(&mut Self) -> Result<ClientCommitDecision<ClientOk>>,
    ) -> Result<ClientOk> {
        let db_state_guard = db.db_state().read().await;
        if db_state_guard.is_terminating == true {
            return Err(anyhow!("DB is terminating"));
        }

        let mut txn = Self::new(db, db_state_guard).await;

        let mut try_i = 0;
        loop {
            try_i += 1;

            let run_txn_res = client_fn(&mut txn);
            match run_txn_res {
                Err(client_err) => {
                    txn.close().await?;
                    return Err(client_err);
                }
                Ok(ClientCommitDecision::Abort(client_ok)) => {
                    txn.close().await?;
                    return Ok(client_ok);
                }
                Ok(ClientCommitDecision::Commit(client_ok)) => {
                    let try_commit_res = txn.try_commit().await?;
                    match try_commit_res {
                        TryCommitResult::Conflict(txn_) => {
                            txn = txn_;
                            if try_i <= retry_limit {
                                txn.reset().await?;
                                continue;
                            } else {
                                txn.close().await?;
                                return Err(anyhow!("Retry limit exceeded"));
                            }
                        }
                        TryCommitResult::DidCommit => {
                            return Ok(client_ok);
                        }
                    }
                }
            }
        }
    }
}

struct CachedSnap {
    commit_ver_hi_incl: CommitVer,
    commit_ver_lo_excl: Option<CommitVer>,

    /// The lifetime is marked as `'static` for our convenience.
    iter: IteratorCache<TxnSnapIterator, &'static CommittedUnit>,
}
impl CachedSnap {
    fn new(
        commit_ver_hi_incl: CommitVer,
        commit_ver_lo_excl: Option<CommitVer>,
        list_snap: ListSnapshot<CommittedUnit>,
    ) -> Self {
        let iter = TxnSnapIterator::new(list_snap.iter(), commit_ver_lo_excl);
        let iter = IteratorCache::new(iter);

        Self {
            commit_ver_hi_incl,
            commit_ver_lo_excl,

            iter,
        }
    }

    fn iter<'s>(&'s mut self) -> impl 's + Iterator<Item = &'static CommittedUnit> {
        self.iter.iter().cloned()
    }
}

/// A named [`std::iter::TakeWhile`].
struct TxnSnapIterator {
    iter: ListIterator<'static, CommittedUnit>,
    commit_ver_lo_excl: Option<CommitVer>,
    iter_reached_end: bool,
}
impl TxnSnapIterator {
    fn new(
        iter: ListIterator<'static, CommittedUnit>,
        commit_ver_lo_excl: Option<CommitVer>,
    ) -> Self {
        Self {
            iter,
            commit_ver_lo_excl,
            iter_reached_end: false,
        }
    }
}
impl Iterator for TxnSnapIterator {
    type Item = &'static CommittedUnit;
    fn next(&mut self) -> Option<Self::Item> {
        if self.iter_reached_end == false {
            self.iter.next().and_then(|unit| {
                let is_included = match self.commit_ver_lo_excl {
                    None => true,
                    Some(cmt_ver_lo) => cmt_ver_lo < unit.commit_info.commit_ver_hi_incl,
                };
                if is_included {
                    return Some(unit);
                } else {
                    self.iter_reached_end = true;
                    return None;
                }
            })
        } else {
            return None;
        }
    }
}
