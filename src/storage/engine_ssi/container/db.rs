use crate::ds_n_a::persisted_u64::PersistedU64;
use crate::storage::engine_ssi::container::{LSMTree, SecondaryIndex, VersionState};
use crate::storage::engine_ssi::entryset::{CommitVer, CLEAN_SLATE_NEXT_COMMIT_VER};
use crate::storage::engines_common::fs_utils::{self, UniqueId};
use crate::storage::types::{PKShared, PVShared, SubValueSpec};
use anyhow::Result;
use shorthand::ShortHand;
use std::collections::HashMap;
use std::fs::{self};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::{watch, Mutex, RwLock};

const UNIQUE_ID_FILE_NAME: &str = "unique_id.u64";
const PRIM_LSM_DIR_NAME: &str = "prim_lsm";
const ALL_SCND_IDXS_DIR_NAME: &str = "scnd_idxs";
const SCND_IDX_DIR_NAME_PFX: &str = "scnd_idx-";

#[derive(ShortHand)]
pub struct DB {
    #[shorthand(disable(get))]
    db_dir_path: PathBuf,
    #[shorthand(disable(get))]
    unique_id: Mutex<PersistedU64<UniqueId>>,

    prim_lsm: LSMTree<PKShared, PVShared>,
    scnd_idxs: RwLock<HashMap<Arc<SubValueSpec>, SecondaryIndex>>,

    commit_ver_state: VersionState<CommitVer>,
    commit_mutex: Mutex<()>,
    is_terminating: AtomicBool,
    job_cv_tx: watch::Sender<()>,
}

impl DB {
    fn load_or_new<P: AsRef<Path>>(db_dir_path: P) -> Result<Self> {
        let unique_id_path = db_dir_path.as_ref().join(UNIQUE_ID_FILE_NAME);
        let prim_lsm_dir_path = db_dir_path.as_ref().join(PRIM_LSM_DIR_NAME);
        let all_scnd_idxs_dir = db_dir_path.as_ref().join(ALL_SCND_IDXS_DIR_NAME);
        fs::create_dir_all(&all_scnd_idxs_dir)?;

        let unique_id = PersistedU64::load_or_new(unique_id_path)?;

        let prim_lsm = LSMTree::load_or_new(prim_lsm_dir_path)?;

        let scnd_idxs = fs_utils::read_dir(&all_scnd_idxs_dir)?
            .map(|res_path| {
                res_path.and_then(|scnd_idx_dir_path| {
                    let scnd_idx = SecondaryIndex::load(scnd_idx_dir_path)?;
                    let spec = scnd_idx.spec().clone();
                    Ok((spec, scnd_idx))
                })
            })
            .collect::<Result<HashMap<_, _>>>()?;

        let next_commit_ver = match prim_lsm.newest_commit_ver() {
            None => CLEAN_SLATE_NEXT_COMMIT_VER,
            Some(mut cmt_ver) => {
                *cmt_ver += 1;
                cmt_ver
            }
        };
        let commit_ver_state = VersionState::new(next_commit_ver);

        let (job_cv_tx, _rx) = watch::channel(());

        Ok(Self {
            db_dir_path: db_dir_path.as_ref().into(),
            unique_id: Mutex::new(unique_id),

            prim_lsm,
            scnd_idxs: RwLock::new(scnd_idxs),

            commit_ver_state,
            commit_mutex: Mutex::new(()),
            is_terminating: AtomicBool::new(false),
            job_cv_tx,
        })
    }

    pub async fn format_new_scnd_idx_dir_path(&self) -> Result<PathBuf> {
        let id = {
            let mut uniq_id_gen = self.unique_id.lock().await;
            uniq_id_gen.get_and_inc()?
        };
        let dirname = format!("{}{}", SCND_IDX_DIR_NAME_PFX, *id);
        let scnd_idx_dir_path = self.db_dir_path.join(ALL_SCND_IDXS_DIR_NAME).join(dirname);
        Ok(scnd_idx_dir_path)
    }

    pub fn send_job_cv(&self) {
        self.job_cv_tx.send(()).ok();
    }
}

mod gc_job;
