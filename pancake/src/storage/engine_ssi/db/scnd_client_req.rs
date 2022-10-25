use crate::storage::engine_ssi::{
    db_state::{DeletionResult, NewDefnResult, ScndIdxState},
    opers::sicr_job::ScndIdxCreationRequest,
    DB,
};
use crate::storage::types::SubValueSpec;
use anyhow::{anyhow, Result};
use std::sync::Arc;
use tokio::sync::oneshot;

impl DB {
    pub async fn create_scnd_idx(&self, sv_spec: &Arc<SubValueSpec>) -> Result<()> {
        let err_comment = |si_state: ScndIdxState| {
            let sfx = if si_state.is_readable {
                "it's ready"
            } else {
                "it's being created"
            };
            format!(
                "Secondary index for {:?} already exists, and {}",
                sv_spec, sfx
            )
        };

        {
            let db_state = self.db_state.read().await;

            match db_state.can_new_scnd_idx_be_defined(sv_spec) {
                Some(si_state) => return Err(anyhow!(err_comment(si_state))),
                None => {}
            }
        }

        let scnd_idx_num;
        {
            let mut db_state = self.db_state.write().await;

            match db_state.define_new_scnd_idx(sv_spec)? {
                NewDefnResult::AlreadyExists(si_state) => {
                    return Err(anyhow!(err_comment(si_state)))
                }
                NewDefnResult::DidDefineNew(si_num) => scnd_idx_num = si_num,
            }
        }

        let (client_tx, client_rx) = oneshot::channel();
        let req = ScndIdxCreationRequest {
            sv_spec: Arc::clone(sv_spec),
            scnd_idx_num,
            response_to_client: client_tx,
        };
        self.scnd_idx_request_tx
            .send(req)
            .await
            .map_err(|_| anyhow!("Failed to send ScndIdxCreationRequest to the F+C job."))?;

        let resp = client_rx.await;
        let resp = resp.map_err(|e| anyhow!(e)).and_then(|inner| inner);
        resp
    }

    pub async fn delete_scnd_idx(&self, sv_spec: &SubValueSpec) -> Result<()> {
        let err_comment = || format!("Secondary index for {:?} is being created.", sv_spec);

        {
            let db_state = self.db_state.read().await;

            match db_state.can_scnd_idx_be_removed(sv_spec) {
                DeletionResult::DoesNotExist => return Ok(()),
                DeletionResult::CreationInProgress => return Err(anyhow!(err_comment())),
                DeletionResult::Deletable => {}
            }
        }

        {
            let mut db_state = self.db_state.write().await;

            match db_state.remove_scnd_idx(sv_spec)? {
                DeletionResult::DoesNotExist => return Ok(()),
                DeletionResult::CreationInProgress => return Err(anyhow!(err_comment())),
                DeletionResult::Deletable => return Ok(()),
            }
        }
    }
}
