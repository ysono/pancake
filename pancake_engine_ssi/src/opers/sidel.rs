use crate::{db_state::ScndIdxRemovalResult, DB};
use anyhow::Result;
use derive_more::Display;
use pancake_types::types::SubValueSpec;

impl DB {
    pub async fn delete_scnd_idx(
        &self,
        sv_spec: &SubValueSpec,
    ) -> Result<(), ScndIdxDeletionJobErr> {
        {
            let db_state = self.db_state().read().await;

            match db_state.can_scnd_idx_be_removed(sv_spec) {
                ScndIdxRemovalResult::DoesNotExist => return Ok(()),
                ScndIdxRemovalResult::CreationInProgress => {
                    return Err(ScndIdxDeletionJobErr::CreationInProgress)
                }
                ScndIdxRemovalResult::Deletable => {}
            };
        }

        {
            let mut db_state = self.db_state().write().await;

            match db_state.remove_scnd_idx(sv_spec) {
                Err(e) => return Err(ScndIdxDeletionJobErr::InternalError(e)),
                Ok(ScndIdxRemovalResult::DoesNotExist) => return Ok(()),
                Ok(ScndIdxRemovalResult::CreationInProgress) => {
                    return Err(ScndIdxDeletionJobErr::CreationInProgress)
                }
                Ok(ScndIdxRemovalResult::Deletable) => return Ok(()),
            }
        }
    }
}

#[derive(Debug, Display)]
pub enum ScndIdxDeletionJobErr {
    CreationInProgress,
    InternalError(anyhow::Error),
}
