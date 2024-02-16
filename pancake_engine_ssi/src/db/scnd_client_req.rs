use crate::{db_state::DeletionResult, DB};
use anyhow::{anyhow, Result};
use pancake_types::types::SubValueSpec;

impl DB {
    pub async fn delete_scnd_idx(&self, sv_spec: &SubValueSpec) -> Result<()> {
        let err_comment = || format!("Secondary index for {sv_spec:?} is being created.");

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
