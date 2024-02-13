mod scnd_client_req;

use crate::{
    db_state::DbState,
    lsm::{ListVer, LsmDir, LsmState, LIST_VER_INITIAL},
    opers::{
        fc_job::FlushingAndCompactionJob,
        sicr_job::{ScndIdxCreationJob, ScndIdxCreationRequest},
    },
};
use anyhow::Result;
use shorthand::ShortHand;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{mpsc, watch, Mutex, RwLock};

const SCND_IDXS_STATE_FILE_NAME: &str = "scnd_idxs_state.txt";
const LSM_DIR_NAME: &str = "lsm";
const SCND_IDXS_CREATION_JOB_DIR_NAME: &str = "si_cr_job";

const SIREQ_CHANNEL_CAPACITY: usize = 4;

#[derive(ShortHand)]
#[shorthand(visibility("pub(in crate)"))]
pub struct DB {
    db_state: RwLock<DbState>,

    lsm_dir: LsmDir,
    lsm_state: Mutex<LsmState>,

    min_held_list_ver_tx: watch::Sender<ListVer>,
    replace_avail_tx: watch::Sender<()>,
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

        let db_state = DbState::load_or_new(&si_state_file_path)?;

        let (lsm_dir, lsm_state) = LsmDir::load_or_new(lsm_dir_path)?;

        let (min_held_list_ver_tx, min_held_list_ver_rx) = watch::channel(LIST_VER_INITIAL);
        let (replace_avail_tx, replace_avail_rx) = watch::channel(());
        let (scnd_idx_request_tx, scnd_idx_request_rx) = mpsc::channel(SIREQ_CHANNEL_CAPACITY);
        let (scnd_idx_work_tx, scnd_idx_work_rx) = mpsc::channel(SIREQ_CHANNEL_CAPACITY);
        let (is_terminating_tx, is_terminating_rx) = watch::channel(());

        let db = Self {
            db_state: RwLock::new(db_state),

            lsm_dir,
            lsm_state: Mutex::new(lsm_state),

            min_held_list_ver_tx,
            replace_avail_tx,
            scnd_idx_request_tx,
            is_terminating_tx,
        };
        let db = Arc::new(db);

        let fc_job = FlushingAndCompactionJob::new(
            Arc::clone(&db),
            Default::default(),
            min_held_list_ver_rx,
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
