use crate::{
    db::DB,
    db_state::{ScndIdxNewDefnResult, ScndIdxNum, ScndIdxState},
    ds_n_a::{atomic_linked_list::ListNode, send_ptr::NonNullSendPtr},
    lsm::{
        lsm_state_utils,
        unit::{CommitVer, CommittedUnit},
        LsmElem,
    },
    opers::fc::FlushAndCompactRequest,
};
use anyhow::{anyhow, Result};
use derive_more::Display;
use pancake_types::types::SubValueSpec;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::oneshot;

mod creation;
mod paths;

pub use paths::ScndIdxCreationsDir;
use paths::*;

impl DB {
    pub async fn create_scnd_idx(
        &self,
        sv_spec: &Arc<SubValueSpec>,
    ) -> Result<(), ScndIdxCreationJobErr> {
        let mut job = ScndIdxCreationJob {
            db: self,
            job_dir: None,
        };
        job.run(sv_spec).await
    }
}

struct ScndIdxCreationJob<'job> {
    db: &'job DB,
    job_dir: Option<ScndIdxCreationJobDir>,
}

impl<'job> ScndIdxCreationJob<'job> {
    async fn run(&mut self, sv_spec: &Arc<SubValueSpec>) -> Result<(), ScndIdxCreationJobErr> {
        let guard = match self.db.si_cr_mutex().try_lock() {
            /* Stylistic gotcha:
            We either mark `guard` as an unused variable, or explicitly drop it at the end.
            If we explicitly drop it, if we forget to handle the `Err(_)` case, rust would not catch it.
            Therefore, here, we prefer `match` + `return` to `?`. */
            Err(_e) => return Err(ScndIdxCreationJobErr::Busy),
            Ok(guard) => guard,
        };

        let (si_num, snap_head_ptr, output_commit_ver) =
            self.prepare_db_state_and_lsm_state(sv_spec).await?;
        let snap_head_ref = unsafe { snap_head_ptr.as_ref() };

        self.prepare_linked_list(snap_head_ptr).await?;

        let committed_unit = self.create_unit(snap_head_ptr, sv_spec, si_num, output_commit_ver)?;

        if let Some(committed_unit) = committed_unit {
            self.insert_node(snap_head_ref, committed_unit);
        }

        self.notify_completion(snap_head_ref, sv_spec).await?;

        drop(guard);

        Ok(())
    }

    async fn prepare_db_state_and_lsm_state(
        &self,
        sv_spec: &Arc<SubValueSpec>,
    ) -> Result<(ScndIdxNum, NonNullSendPtr<ListNode<LsmElem>>, CommitVer), ScndIdxCreationJobErr>
    {
        {
            let db_state = self.db.db_state().read().await;

            if db_state.is_terminating == true {
                return Err(anyhow!("DB is terminating").into());
            }

            match db_state.get_scnd_idx_defn(sv_spec) {
                Some(si_state) => return Err(si_state.into()),
                None => {}
            }
        }

        /* The new_head is malloc'd outside the two RwLock guards. */
        let prepped_new_head = lsm_state_utils::new_dummy_node(0, true);
        let si_num;
        let snap_head_ptr;
        let output_commit_ver;
        {
            let mut db_state = self.db.db_state().write().await;

            if db_state.is_terminating == true {
                return Err(anyhow!("DB is terminating").into());
            }

            match db_state.define_new_scnd_idx(sv_spec) {
                Err(e) => return Err(e.into()),
                Ok(ScndIdxNewDefnResult::Existent(si_state)) => return Err(si_state.into()),
                Ok(ScndIdxNewDefnResult::DidDefineNew(si_num_)) => si_num = si_num_,
            }

            {
                let mut lsm_state = self.db.lsm_state().lock().await;

                let snap_head_ptr_ = lsm_state.list().push_head_node(prepped_new_head);
                snap_head_ptr = NonNullSendPtr::from(snap_head_ptr_);

                output_commit_ver = lsm_state.fetch_inc_next_commit_ver();
            }
        }

        Ok((si_num, snap_head_ptr, output_commit_ver))
    }

    async fn prepare_linked_list(
        &self,
        snap_head: NonNullSendPtr<ListNode<LsmElem>>,
    ) -> Result<(), anyhow::Error> {
        let (response_tx, response_rx) = oneshot::channel();
        let fc_req_msg = FlushAndCompactRequest {
            snap_head,
            response_tx,
        };
        self.db
            .fc_request_tx()
            .send(fc_req_msg)
            .await
            .map_err(|_e| anyhow!("The F+C worker appears dead"))?;
        response_rx.await.map_err(|e| anyhow!(e))?;

        Ok(())
    }

    fn insert_node(&self, snap_head: &ListNode<LsmElem>, committed_unit: CommittedUnit) {
        let snap_second_ptr = snap_head.next.load(Ordering::SeqCst);

        let node_own = lsm_state_utils::new_unit_node(committed_unit);
        node_own.next.store(snap_second_ptr, Ordering::SeqCst);

        let node_ptr = Box::into_raw(node_own);
        snap_head.next.store(node_ptr, Ordering::SeqCst);
    }

    async fn notify_completion(
        &self,
        snap_head: &ListNode<LsmElem>,
        sv_spec: &SubValueSpec,
    ) -> Result<()> {
        if let LsmElem::Dummy { is_fence, .. } = &snap_head.elem {
            is_fence.store(false, Ordering::SeqCst);
        }

        {
            let mut db_state = self.db.db_state().write().await;

            db_state.set_scnd_idx_as_readable(sv_spec)?;
        }

        let send_res = self.db.fc_avail_tx().send(());
        send_res.ok(); // If F+C couldn't receive, the DB must be termianting. Ignore.

        Ok(())
    }
}

#[derive(Debug, Display)]
pub enum ScndIdxCreationJobErr {
    Busy,
    Existent { is_readable: bool },
    InternalError(anyhow::Error),
}

impl From<ScndIdxState> for ScndIdxCreationJobErr {
    fn from(si_state: ScndIdxState) -> Self {
        Self::Existent {
            is_readable: si_state.is_readable,
        }
    }
}
impl<E: Into<anyhow::Error>> From<E> for ScndIdxCreationJobErr {
    fn from(e: E) -> Self {
        Self::InternalError(e.into())
    }
}
