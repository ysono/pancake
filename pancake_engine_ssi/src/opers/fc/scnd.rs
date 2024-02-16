use crate::ds_n_a::send_ptr::SendPtr;
use crate::{
    lsm::{lsm_state_utils, LsmElem},
    opers::{
        fc::{fc_traversal::FCJob, FlushingAndCompactionWorker},
        sicr_job::{ScndIdxCreationRequest, ScndIdxCreationWork},
    },
};
use anyhow::{anyhow, Result};
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;

impl FlushingAndCompactionWorker {
    pub(super) async fn prep_for_scnd_idx_creation(
        &mut self,
        req: ScndIdxCreationRequest,
    ) -> Result<()> {
        /* Hold a shared guard on `db_state` over the whole traversal of the LL. */
        let db_state_guard = self.db.db_state().read().await;

        /* The new_head is malloc'd and free'd outside the mutex guard.
        Don't `move` the prepped new_head into the lambda. */
        let mut prepped_new_head = Some(lsm_state_utils::new_dummy_node(0, true));
        let update_or_provide_head = |elem: Option<&LsmElem>| {
            if let Some(LsmElem::Dummy { is_fence, .. }) = elem {
                let prior_is_fence = is_fence.fetch_or(true, Ordering::SeqCst);
                if prior_is_fence == false {
                    return None;
                }
            }
            return prepped_new_head.take();
        };
        let snap_head_ref;
        let output_commit_ver;
        {
            let mut lsm_state = self.db.lsm_state().lock().await;

            let snap_head_ptr = lsm_state.update_or_push(update_or_provide_head);
            snap_head_ref = unsafe { &*snap_head_ptr };

            output_commit_ver = lsm_state.fetch_inc_next_commit_ver();
        }

        let mut job = FCJob {
            db: &self.db,
            db_state_guard,
            dangling_nodes: &mut self.dangling_nodes,
        };
        job.traverse_and_compact(snap_head_ref).await?;

        let work = ScndIdxCreationWork {
            snap_head_excl: SendPtr::from(snap_head_ref),
            output_commit_ver,
            req,
        };
        let send_work_res = self.scnd_idx_work_tx.send(work).await;
        if let Err(mpsc::error::SendError(work)) = send_work_res {
            let respond_res = work.req.response_to_client.send(Err(anyhow!(
                "Too many secondary index creation requests in queue."
            )));
            respond_res.ok();
        }

        Ok(())
    }
}
