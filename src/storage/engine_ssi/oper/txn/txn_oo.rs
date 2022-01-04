use crate::ds_n_a::interval_set::IntervalSet;
use crate::storage::engine_ssi::container::{SecondaryIndex, DB};
use crate::storage::engine_ssi::entryset::{CommitVer, WritableMemLog};
use crate::storage::types::{PKShared, PVShared, PrimaryKey, SVPKShared, SubValue, SubValueSpec};
use anyhow::{anyhow, Result};
use derive_more::Into;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::RwLockReadGuard;

pub struct Txn<'txn> {
    db: &'txn DB,
    scnd_idxs_guard: RwLockReadGuard<'txn, HashMap<Arc<SubValueSpec>, SecondaryIndex>>,

    dep_itvs_prim: IntervalSet<&'txn PrimaryKey>,
    dep_itvs_scnds: HashMap<Arc<SubValueSpec>, IntervalSet<&'txn SubValue>>,

    written_prim: Option<WritableMemLog<PKShared, PVShared>>,
    written_scnds: HashMap<Arc<SubValueSpec>, WritableMemLog<SVPKShared, PVShared>>,

    gap_ver_lo: CommitVer,
    snap_ver_ceil: CommitVer,
}

impl<'txn> Txn<'txn> {
    async fn new(db: &'txn DB) -> Txn<'txn> {
        let scnd_idxs_guard = db.scnd_idxs().read().await;

        let snap_ver_ceil = db.commit_ver_state().hold_leading().await;

        Self {
            db,
            scnd_idxs_guard,

            dep_itvs_prim: IntervalSet::new(),
            dep_itvs_scnds: HashMap::new(),

            written_prim: None,
            written_scnds: HashMap::new(),

            gap_ver_lo: CommitVer::from(0), // The initial value is never read.
            snap_ver_ceil,
        }
    }

    pub async fn close<CbRetOk>(self, cb_ret: Result<CbRetOk>) -> CloseResult<CbRetOk> {
        self.db
            .commit_ver_state()
            .unhold(self.snap_ver_ceil, || self.db.send_job_cv())
            .await;

        let close_res: Result<()> = async {
            for (_spec, w_memlog) in self.written_scnds.into_iter() {
                w_memlog.remove_entryset_dir()?;
            }

            if let Some(w_memlog) = self.written_prim {
                w_memlog.remove_entryset_dir()?;
            }

            Ok(())
        }
        .await;

        let final_res: Result<CbRetOk> = close_res.and_then(|()| cb_ret);
        CloseResult(final_res)
    }

    /// The callback is required to call `txn.close()` before returning.
    /// This pattern enforces RAII on `Txn`, whose async and fallible (Result<_>)
    ///     dropping function needs to be called explicitly.
    ///
    /// Because `txn.close()` returns a non-std `CloseResult`, it is impossible
    ///     for the callback to mistakenly early-terminate via the `?` operator.
    pub async fn run<Cb, CbRetOk>(db: &'txn DB, cb: Cb) -> CloseResult<CbRetOk>
    where
        Cb: Send
            + FnOnce(Txn<'txn>) -> Pin<Box<dyn 'txn + Send + Future<Output = CloseResult<CbRetOk>>>>,
    {
        if db.is_terminating().load(Ordering::SeqCst) == true {
            return CloseResult(Err(anyhow!("DB is terminating.")));
        }

        let txn: Txn<'txn> = Txn::new(db).await;

        cb(txn).await
    }
}

#[derive(Into)]
pub struct CloseResult<T>(Result<T>);

mod commit;
mod stmt;

pub use commit::CommitResult;
