use crate::ds_n_a::atomic_linked_list::{ListNode, ListSnapshot, ListSnapshotIterator};
use crate::ds_n_a::interval_set::IntervalSet;
use crate::ds_n_a::send_ptr::NonNullSendPtr;
use crate::{
    db_state::{DbState, ScndIdxNum},
    lsm::{
        unit::{CommitVer, CommittedUnit, StagingUnit},
        ListVer, LsmElem, LsmElemType,
    },
    DB,
};
use anyhow::{anyhow, Result};
use pancake_types::types::{PrimaryKey, SubValue};
use std::collections::HashMap;
use std::iter;
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
    snap_commit_ver: CommitVer,
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
    /// The lifetime is marked as `'static` for our convenience.
    list_iter: ListSnapshotIterator<'static, LsmElem>,

    /// Lazily populated.
    units_cache: Vec<&'static CommittedUnit>,
}
impl CachedSnap {
    fn new(list_snap: ListSnapshot<LsmElem>) -> Self {
        let list_iter = list_snap.into_iter_including_head_excluding_tail();
        Self {
            list_iter,
            units_cache: vec![],
        }
    }

    fn head_ptr(&self) -> NonNullSendPtr<ListNode<LsmElem>> {
        self.list_iter.snap().head_ptr()
    }
    fn tail_ptr(&self) -> Option<NonNullSendPtr<ListNode<LsmElem>>> {
        self.list_iter.snap().tail_ptr()
    }

    fn iter<'a>(&'a mut self) -> impl 'a + Iterator<Item = &'a CommittedUnit> {
        let mut cache_i = 0;
        let iter_fn = move || {
            if cache_i < self.units_cache.len() {
                let ret = self.units_cache[cache_i];
                cache_i += 1;
                return Some(ret);
            } else {
                loop {
                    match self.list_iter.next() {
                        Some(elem) => match &elem.elem_type {
                            LsmElemType::CommittedUnit(unit) => {
                                self.units_cache.push(unit);
                                cache_i += 1;
                                return Some(unit);
                            }
                            LsmElemType::Dummy { .. } => continue,
                        },
                        None => return None,
                    }
                }
            }
        };
        iter::from_fn(iter_fn)
    }
}
