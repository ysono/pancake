use crate::db_state::{ScndIdxNum, ScndIdxState, ScndIdxsState};
use anyhow::{anyhow, Context, Result};
use pancake_engine_common::fs_utils;
use pancake_types::types::SubValueSpec;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct DbState {
    scnd_idxs_state: ScndIdxsState,
    scnd_idxs_state_file_path: PathBuf,

    pub is_terminating: bool,
}

impl DbState {
    pub fn load_or_new<P: AsRef<Path>>(scnd_idxs_state_file_path: P) -> Result<Self> {
        let sis_path = scnd_idxs_state_file_path.as_ref();
        let scnd_idxs_state;
        if sis_path.exists() {
            scnd_idxs_state = ScndIdxsState::deser(sis_path).context(format!("{sis_path:?}"))?;
            for (sv_spec, si_state) in scnd_idxs_state.scnd_idxs.iter() {
                if si_state.is_readable == false {
                    return Err(anyhow!("Prior secondary index creation never completed for {sv_spec:?}. You should remove this secondary index's info manually from {sis_path:?}", ));
                }
            }
        } else {
            let parent_path = sis_path.parent().ok_or_else(|| anyhow!("Secondary index state file must be located under a parent directory. Invalid file path: {sis_path:?}"))?;
            fs_utils::create_dir_all(parent_path)?;

            scnd_idxs_state = ScndIdxsState::new_empty();
            scnd_idxs_state.ser(sis_path)?;
        }

        Ok(Self {
            scnd_idxs_state,
            scnd_idxs_state_file_path: sis_path.into(),

            is_terminating: false,
        })
    }

    pub fn scnd_idxs(&self) -> &HashMap<Arc<SubValueSpec>, ScndIdxState> {
        &self.scnd_idxs_state.scnd_idxs
    }

    pub fn get_scnd_idx_defn(&self, sv_spec: &SubValueSpec) -> Option<ScndIdxState> {
        self.scnd_idxs_state.scnd_idxs.get(sv_spec).cloned()
    }
    pub fn define_new_scnd_idx(
        &mut self,
        sv_spec: &Arc<SubValueSpec>,
    ) -> Result<ScndIdxNewDefnResult> {
        match self.get_scnd_idx_defn(sv_spec) {
            Some(si_state) => return Ok(ScndIdxNewDefnResult::Existent(si_state)),
            None => {}
        }

        let sis = &mut self.scnd_idxs_state;
        let scnd_idx_num = sis.next_scnd_idx_num.fetch_inc();
        let scnd_idx_state = ScndIdxState {
            scnd_idx_num,
            is_readable: false,
        };
        sis.scnd_idxs.insert(Arc::clone(sv_spec), scnd_idx_state);
        sis.ser(&self.scnd_idxs_state_file_path)?;
        Ok(ScndIdxNewDefnResult::DidDefineNew(scnd_idx_num))
    }

    pub fn set_scnd_idx_as_readable(&mut self, sv_spec: &SubValueSpec) -> Result<()> {
        let sis = &mut self.scnd_idxs_state;
        match sis.scnd_idxs.get_mut(sv_spec) {
            None => return Err(anyhow!("No state for {sv_spec:?}")),
            Some(si_state) => {
                si_state.is_readable = true;
                sis.ser(&self.scnd_idxs_state_file_path)?;
                return Ok(());
            }
        }
    }

    pub fn can_scnd_idx_be_removed(&self, sv_spec: &SubValueSpec) -> ScndIdxRemovalResult {
        let sis = &self.scnd_idxs_state;
        match sis.scnd_idxs.get(sv_spec) {
            None => ScndIdxRemovalResult::DoesNotExist,
            Some(si_state) if si_state.is_readable != true => {
                ScndIdxRemovalResult::CreationInProgress
            }
            Some(_) => ScndIdxRemovalResult::Deletable,
        }
    }
    pub fn remove_scnd_idx(&mut self, sv_spec: &SubValueSpec) -> Result<ScndIdxRemovalResult> {
        let eligibility = self.can_scnd_idx_be_removed(sv_spec);
        match eligibility {
            ScndIdxRemovalResult::DoesNotExist | ScndIdxRemovalResult::CreationInProgress => {}
            ScndIdxRemovalResult::Deletable => {
                let sis = &mut self.scnd_idxs_state;
                sis.scnd_idxs.remove(sv_spec);
                sis.ser(&self.scnd_idxs_state_file_path)?;
            }
        }
        Ok(eligibility)
    }
}

pub enum ScndIdxNewDefnResult {
    Existent(ScndIdxState),
    DidDefineNew(ScndIdxNum),
}

pub enum ScndIdxRemovalResult {
    DoesNotExist,
    CreationInProgress,
    Deletable,
}
