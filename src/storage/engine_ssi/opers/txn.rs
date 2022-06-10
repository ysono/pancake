mod conflict;
mod state_transition_helpers;
mod state_transitions;
mod stmt;

use state_transitions::TryCommitResult;

use crate::ds_n_a::atomic_linked_list::AtomicLinkedListSnapshot;
use crate::ds_n_a::interval_set::IntervalSet;
use crate::storage::engine_ssi::{
    db_state::{DbState, ScndIdxNum},
    lsm_state::{
        unit::{CommitVer, StagingUnit},
        ListVer, LsmElem,
    },
    DB,
};
use crate::storage::types::{PrimaryKey, SubValue};
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use tokio::sync::RwLockReadGuard;

const RETRY_LIMIT: usize = 5;

pub enum ClientCommitDecision<T> {
    Commit(T),
    Abort(T),
}

pub struct Txn<'txn> {
    db: &'txn DB,
    db_state_guard: RwLockReadGuard<'txn, DbState>,

    snap: AtomicLinkedListSnapshot<LsmElem>,
    snap_next_commit_ver: CommitVer,
    snap_list_ver: ListVer,

    /// The Vec version of `snap`. Lazily initialized and used by range queries only.
    snap_vec: Option<Vec<&'txn LsmElem>>,

    dependent_itvs_prim: IntervalSet<&'txn PrimaryKey>,
    dependent_itvs_scnds: HashMap<ScndIdxNum, IntervalSet<&'txn SubValue>>,

    staging: Option<StagingUnit>,
}

impl<'txn> Txn<'txn> {
    pub async fn run<RunTxn, ClientOk>(db: &'txn DB, run_txn: RunTxn) -> Result<ClientOk>
    where
        RunTxn: Fn(&mut Self) -> Result<ClientCommitDecision<ClientOk>>,
    {
        let db_state_guard = db.db_state().read().await;
        if db_state_guard.is_terminating == true {
            return Err(anyhow!("DB is terminating"));
        }

        let mut txn = Self::new(db, db_state_guard).await;

        let mut retry_i = 0;
        loop {
            retry_i += 1;

            let client_ok = match run_txn(&mut txn) {
                Err(client_err) => {
                    txn.close().await?;
                    return Err(client_err);
                }
                Ok(ClientCommitDecision::Abort(client_ok)) => {
                    txn.close().await?;
                    return Ok(client_ok);
                }
                Ok(ClientCommitDecision::Commit(client_ok_)) => client_ok_,
            };

            match txn.try_commit().await? {
                TryCommitResult::Conflict(txn_) => {
                    txn = txn_;
                    if retry_i <= RETRY_LIMIT {
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
