use crate::{
    db_state::DbState,
    lsm::{ListVer, LsmDir, LsmState},
    opers::{
        fc::{FlushAndCompactRequest, FlushingAndCompactionWorker},
        sicr::ScndIdxCreationsDir,
    },
};
use anyhow::Result;
use pancake_engine_common::fs_utils;
use shorthand::ShortHand;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{mpsc, watch, Mutex, RwLock};

const SCND_IDXS_STATE_FILE_NAME: &str = "scnd_idxs_state.txt";
const LSM_DIR_NAME: &str = "lsm";
const ALL_SCND_IDX_CREATION_JOBS_DIR_NAME: &str = "scnd_idx_creation";

/// Warning: If we want to allow concurrent count to be > 1, then we ought to modify the design.
/// See docs for details.
const SCND_IDX_CREATION_CONCURRENT_COUNT_MAX: usize = 1;

#[derive(ShortHand)]
#[shorthand(visibility("pub(in crate)"))]
pub struct DB {
    _lock_dir: File,

    db_state: RwLock<DbState>,

    lsm_dir: LsmDir,
    lsm_state: Mutex<LsmState>,

    si_cr_dir: ScndIdxCreationsDir,
    si_cr_mutex: Mutex<()>,

    fc_avail_tx: watch::Sender<()>,
    fc_request_tx: mpsc::Sender<FlushAndCompactRequest>,
    min_held_list_ver_tx: watch::Sender<ListVer>,
    is_terminating_tx: watch::Sender<()>,
}

impl DB {
    pub fn load_or_new<P: AsRef<Path>>(
        db_dir_path: P,
    ) -> Result<(Arc<Self>, FlushingAndCompactionWorker)> {
        let db_dir_path = db_dir_path.as_ref();

        fs_utils::create_dir_all(db_dir_path)?;
        let lock_dir = fs_utils::lock_file(db_dir_path)?;

        let si_state_file_path = db_dir_path.join(SCND_IDXS_STATE_FILE_NAME);
        let lsm_dir_path = db_dir_path.join(LSM_DIR_NAME);
        let si_cr_dir_path = db_dir_path.join(ALL_SCND_IDX_CREATION_JOBS_DIR_NAME);

        let db_state = DbState::load_or_new(si_state_file_path)?;

        let (lsm_dir, lsm_state) = LsmDir::load_or_new(lsm_dir_path)?;

        let si_cr_dir = ScndIdxCreationsDir::load_or_new(si_cr_dir_path)?;
        let si_cr_mutex = Mutex::new(());

        let (fc_avail_tx, fc_avail_rx) = watch::channel(());
        let (fc_request_tx, fc_request_rx) = mpsc::channel(SCND_IDX_CREATION_CONCURRENT_COUNT_MAX);
        let (min_held_list_ver_tx, min_held_list_ver_rx) = watch::channel(ListVer::AT_BOOTUP);
        let (is_terminating_tx, is_terminating_rx) = watch::channel(());

        let db = Self {
            _lock_dir: lock_dir,

            db_state: RwLock::new(db_state),

            lsm_dir,
            lsm_state: Mutex::new(lsm_state),

            si_cr_dir,
            si_cr_mutex,

            fc_avail_tx,
            fc_request_tx,
            min_held_list_ver_tx,
            is_terminating_tx,
        };
        let db = Arc::new(db);

        let fc_worker = FlushingAndCompactionWorker {
            db: Arc::clone(&db),

            dangling_nodes: Default::default(),

            fc_avail_rx,
            fc_request_rx,
            min_held_list_ver_rx,
            is_terminating_rx,
        };

        Ok((db, fc_worker))
    }

    pub async fn terminate(&self) {
        {
            let mut db_state = self.db_state.write().await;

            db_state.is_terminating = true;
        }

        self.is_terminating_tx.send(()).ok();
    }
}
