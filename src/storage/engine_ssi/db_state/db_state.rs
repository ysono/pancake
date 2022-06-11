use crate::storage::engine_ssi::db_state::{ScndIdxNum, ScndIdxState, ScndIdxsState};
use crate::storage::types::SubValueSpec;
use anyhow::{anyhow, Result};
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
        let path = scnd_idxs_state_file_path.as_ref();
        let mut scnd_idxs_state;
        if path.exists() {
            scnd_idxs_state = ScndIdxsState::deser(path)?;
            /* Ignore scnd idxs that previously failed to be created.
            This allows a new scnd idx for the same sv_spec to be created again. */
            scnd_idxs_state.scnd_idxs.retain(|sv_spec, si_state| {
                if !si_state.is_readable {
                    eprintln!(
                        "Secondary index creation for {:?} never completed last time.",
                        sv_spec
                    );
                }
                si_state.is_readable
            });
        } else {
            scnd_idxs_state = ScndIdxsState::default();
            scnd_idxs_state.ser(path)?;
        }

        Ok(Self {
            scnd_idxs_state,
            scnd_idxs_state_file_path: path.into(),
            is_terminating: false,
        })
    }

    pub fn scnd_idxs(&self) -> &HashMap<Arc<SubValueSpec>, ScndIdxState> {
        &self.scnd_idxs_state.scnd_idxs
    }

    pub fn can_new_scnd_idx_be_defined(&self, sv_spec: &SubValueSpec) -> Option<ScndIdxState> {
        let sis = &self.scnd_idxs_state;
        sis.scnd_idxs.get(sv_spec).cloned()
    }
    pub fn define_new_scnd_idx(&mut self, sv_spec: &Arc<SubValueSpec>) -> Result<NewDefnResult> {
        match self.can_new_scnd_idx_be_defined(sv_spec) {
            Some(si_state) => return Ok(NewDefnResult::AlreadyExists(si_state)),
            None => {}
        }

        let sis = &mut self.scnd_idxs_state;
        let scnd_idx_num = sis.next_scnd_idx_num.get_and_inc();
        let scnd_idx_state = ScndIdxState {
            scnd_idx_num,
            is_readable: false,
        };
        sis.scnd_idxs.insert(Arc::clone(sv_spec), scnd_idx_state);
        sis.ser(&self.scnd_idxs_state_file_path)?;
        Ok(NewDefnResult::DidDefineNew(scnd_idx_num))
    }

    pub fn set_scnd_idx_as_readable(&mut self, sv_spec: &SubValueSpec) -> Result<()> {
        let sis = &mut self.scnd_idxs_state;

        match sis.scnd_idxs.get_mut(sv_spec) {
            None => return Err(anyhow!("No state for {:?}", sv_spec)),
            Some(si_state) => {
                si_state.is_readable = true;
                sis.ser(&self.scnd_idxs_state_file_path)?;
                return Ok(());
            }
        }
    }

    pub fn can_scnd_idx_be_removed(&self, sv_spec: &SubValueSpec) -> DeletionResult {
        let sis = &self.scnd_idxs_state;
        match sis.scnd_idxs.get(sv_spec) {
            None => DeletionResult::DoesNotExist,
            Some(si_state) if si_state.is_readable == true => DeletionResult::CreationInProgress,
            Some(_) => DeletionResult::Deletable,
        }
    }
    pub fn remove_scnd_idx(&mut self, sv_spec: &SubValueSpec) -> Result<DeletionResult> {
        match self.can_scnd_idx_be_removed(sv_spec) {
            eligiblity @ DeletionResult::Deletable => {
                let sis = &mut self.scnd_idxs_state;
                sis.scnd_idxs.remove(sv_spec);
                sis.ser(&self.scnd_idxs_state_file_path)?;
                Ok(eligiblity)
            }
            eligibility => Ok(eligibility),
        }
    }
}

pub enum NewDefnResult {
    AlreadyExists(ScndIdxState),
    DidDefineNew(ScndIdxNum),
}

pub enum DeletionResult {
    DoesNotExist,
    CreationInProgress,
    Deletable,
}
