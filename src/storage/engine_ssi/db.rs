mod scnd_client_req;

use crate::storage::engine_ssi::{
    db_state::DbState,
    lsm_dir_mgr::LsmDirManager,
    lsm_state::{GcAbleInterval, LsmState},
    opers::{
        fc_job::FlushingAndCompactionJob,
        sicr_job::{ScndIdxCreationJob, ScndIdxCreationRequest},
    },
};
use anyhow::Result;
use shorthand::ShortHand;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{mpsc, watch, Mutex, RwLock};

const SCND_IDXS_STATE_FILE_NAME: &str = "scnd_idxs_state.txt";
const LSM_DIR_NAME: &str = "lsm";
const SCND_IDXS_CREATION_JOB_DIR_NAME: &str = "si_cr_job";

const GC_CHANNEL_CAPACITY: usize = 4096;
const SIREQ_CHANNEL_CAPACITY: usize = 4;

#[derive(ShortHand)]
pub struct DB {
    db_state: RwLock<DbState>,

    lsm_dir_mgr: LsmDirManager,
    lsm_state: Mutex<LsmState>,

    replace_avail_tx: watch::Sender<()>,
    gc_avail_tx: mpsc::Sender<GcAbleInterval>,
    scnd_idx_request_tx: mpsc::Sender<ScndIdxCreationRequest>,
    is_terminating_tx: watch::Sender<()>,
}

impl DB {
    pub fn load_or_new<P: AsRef<Path>>(
        db_dir_path: P,
    ) -> Result<(Arc<Self>, FlushingAndCompactionJob, ScndIdxCreationJob)> {
        let db_dir_path = db_dir_path.as_ref();
        let si_state_file_path = db_dir_path.join(SCND_IDXS_STATE_FILE_NAME);
        let lsm_dir_path = db_dir_path.join(LSM_DIR_NAME);
        let si_cr_dir_path = db_dir_path.join(SCND_IDXS_CREATION_JOB_DIR_NAME);
        fs::create_dir_all(&lsm_dir_path)?;
        fs::create_dir_all(&si_cr_dir_path)?;

        let db_state = DbState::load_or_new(&si_state_file_path)?;

        let (lsm_dir_mgr, lsm_state) = LsmDirManager::load_or_new_lsm_dir(lsm_dir_path)?;

        let (replace_avail_tx, replace_avail_rx) = watch::channel(());
        let (gc_avail_tx, gc_avail_rx) = mpsc::channel(GC_CHANNEL_CAPACITY);
        let (scnd_idx_request_tx, scnd_idx_request_rx) = mpsc::channel(SIREQ_CHANNEL_CAPACITY);
        let (scnd_idx_work_tx, scnd_idx_work_rx) = mpsc::channel(SIREQ_CHANNEL_CAPACITY);
        let (is_terminating_tx, is_terminating_rx) = watch::channel(());

        let db = Self {
            db_state: RwLock::new(db_state),

            lsm_dir_mgr,
            lsm_state: Mutex::new(lsm_state),

            replace_avail_tx,
            gc_avail_tx,
            scnd_idx_request_tx,
            is_terminating_tx,
        };
        let db = Arc::new(db);

        let fc_job = FlushingAndCompactionJob::new(
            Arc::clone(&db),
            HashMap::new(),
            gc_avail_rx,
            replace_avail_rx,
            scnd_idx_request_rx,
            is_terminating_rx.clone(),
            scnd_idx_work_tx,
        );

        let sicr_job = ScndIdxCreationJob::new(
            Arc::clone(&db),
            si_cr_dir_path,
            scnd_idx_work_rx,
            is_terminating_rx,
        );

        Ok((db, fc_job, sicr_job))
    }

    pub async fn terminate(&self) {
        {
            let mut db_state = self.db_state.write().await;

            db_state.is_terminating = true;
        }

        self.is_terminating_tx.send(()).ok();
    }
}
