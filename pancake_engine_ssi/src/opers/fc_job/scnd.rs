use crate::ds_n_a::send_ptr::SendPtr;
use crate::{
    lsm::{lsm_state_utils, LsmElem},
    opers::{
        fc_job::FlushingAndCompactionJob,
        sicr_job::{ScndIdxCreationRequest, ScndIdxCreationWork},
    },
};
use anyhow::{anyhow, Result};
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;

impl FlushingAndCompactionJob {
    pub(super) async fn prep_for_scnd_idx_creation(
        &mut self,
        req: ScndIdxCreationRequest,
    ) -> Result<()> {
        /* Malloc for new_head outside the mutex guard.
        Free new_head outside the mutex guard, thanks to Option<>. */
        let mut prepped_new_head = Some(lsm_state_utils::new_dummy_node(0, true));
        let snap_head_excl;
        let output_commit_ver;
        {
            let mut lsm_state = self.db.lsm_state().lock().await;

            let update_or_provide_head = |elem: Option<&LsmElem>| {
                if let Some(LsmElem::Dummy { is_fence, .. }) = elem {
                    let prior_is_fence = is_fence.fetch_or(true, Ordering::SeqCst);
                    if prior_is_fence == false {
                        return None;
                    }
                }
                return prepped_new_head.take();
            };
            snap_head_excl = SendPtr::from(lsm_state.update_or_push(update_or_provide_head));

            output_commit_ver = lsm_state.fetch_inc_next_commit_ver();
        }

        self.traverse_and_compact(unsafe { snap_head_excl.as_ref() })
            .await?;

        let work = ScndIdxCreationWork {
            snap_head_excl,
            output_commit_ver,
            req,
        };
        let send_work_res = self.scnd_idx_creation_work.send(work).await;
        if let Err(mpsc::error::SendError(work)) = send_work_res {
            let respond_res = work.req.response_to_client.send(Err(anyhow!(
                "Too many secondary index creation requests in queue."
            )));
            respond_res.ok();
        }

        Ok(())
    }
}
