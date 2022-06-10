use crate::ds_n_a::send_ptr::SendPtr;
use crate::storage::engine_ssi::{
    lsm_state::{unit::unit_utils, LsmElemContent, LIST_VER_PLACEHOLDER},
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
        let mut prepped_new_head = Some(unit_utils::new_dummy_node(LIST_VER_PLACEHOLDER, 0, true));
        let snap_head_excl;
        let output_commit_ver;
        {
            let mut lsm_state = self.db.lsm_state().lock().await;

            let update_or_provide_head = |content: Option<&LsmElemContent>| {
                if let Some(LsmElemContent::Dummy { is_fence, .. }) = content {
                    let prior_is_fence = is_fence.fetch_or(true, Ordering::SeqCst);
                    if prior_is_fence == false {
                        return None;
                    }
                }
                let mut new_head = prepped_new_head.take().unwrap();
                new_head.elem.traversable_list_ver_lo_incl = lsm_state.curr_list_ver;
                return Some(new_head);
            };
            snap_head_excl = SendPtr::from(lsm_state.update_or_push(update_or_provide_head));

            output_commit_ver = lsm_state.next_commit_ver;
            *lsm_state.next_commit_ver += 1;
        }

        self.merge_segments(unsafe { snap_head_excl.as_ref() })
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
