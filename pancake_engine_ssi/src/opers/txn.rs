mod conflict;
mod state_transition_helpers;
mod state_transitions;
mod stmt;

use state_transitions::TryCommitResult;

use crate::ds_n_a::atomic_linked_list::ListSnapshot;
use crate::ds_n_a::interval_set::IntervalSet;
use crate::{
    db_state::{DbState, ScndIdxNum},
    lsm::{
        unit::{CommitVer, CommittedUnit, StagingUnit},
        ListVer, LsmElem,
    },
    DB,
};
use anyhow::{anyhow, Result};
use pancake_types::types::{PrimaryKey, SubValue};
use std::collections::HashMap;
use tokio::sync::RwLockReadGuard;

pub enum ClientCommitDecision<ClientOk> {
    Commit(ClientOk),
    Abort(ClientOk),
}

pub struct Txn<'txn> {
    db: &'txn DB,
    db_state_guard: RwLockReadGuard<'txn, DbState>,

    snap: ListSnapshot<LsmElem>,
    snap_next_commit_ver: CommitVer,
    snap_list_ver: ListVer,

    /// The Vec version of `snap`. Lazily initialized and used by range queries only.
    snap_vec: Option<Vec<&'txn CommittedUnit>>,

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
