use crate::storage::engine_ssi::container::{LSMTree, SecondaryIndex, VersionState};
use crate::storage::engine_ssi::entryset::{CommitVer, CLEAN_SLATE_NEXT_COMMIT_VER};
use crate::storage::engines_common::fs_utils::{self, PathNameNum};
use crate::storage::types::{PKShared, PVShared, SubValueSpec};
use anyhow::{anyhow, Result};
use shorthand::ShortHand;
use std::cmp;
use std::collections::HashMap;
use std::fs::{self};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{watch, Mutex, RwLock};

const PRIM_LSM_DIR_NAME: &str = "prim_lsm";
const ALL_SCND_IDXS_DIR_NAME: &str = "scnd_idxs";

#[derive(ShortHand)]
pub struct DB {
    #[shorthand(disable(get))]
    db_dir_path: PathBuf,
    #[shorthand(disable(get))]
    next_scnd_idx_num: AtomicU64,

    prim_lsm: LSMTree<PKShared, PVShared>,
    scnd_idxs: RwLock<HashMap<Arc<SubValueSpec>, SecondaryIndex>>,

    commit_ver_state: VersionState<CommitVer>,
    commit_mutex: Mutex<()>,
    is_terminating: AtomicBool,
    job_cv_tx: watch::Sender<()>,
}

impl DB {
    fn load_or_new<P: AsRef<Path>>(db_dir_path: P) -> Result<Self> {
        let prim_lsm_dir_path = db_dir_path.as_ref().join(PRIM_LSM_DIR_NAME);
        let all_scnd_idxs_dir_path = db_dir_path.as_ref().join(ALL_SCND_IDXS_DIR_NAME);
        fs::create_dir_all(&all_scnd_idxs_dir_path)?;

        let prim_lsm = LSMTree::load_or_new(prim_lsm_dir_path)?;

        let mut scnd_idxs = HashMap::new();
        let mut max_scnd_idx_num = 0;
        for res_path in fs_utils::read_dir(&all_scnd_idxs_dir_path)? {
            let scnd_idx_dir_path = res_path?;

            let num = Self::parse_scnd_idx_dir_num(&scnd_idx_dir_path)?;
            max_scnd_idx_num = cmp::max(max_scnd_idx_num, *num);

            let scnd_idx = SecondaryIndex::load(scnd_idx_dir_path)?;
            let spec = scnd_idx.spec().clone();
            scnd_idxs.insert(spec, scnd_idx);
        }
        let next_scnd_idx_num = AtomicU64::from(max_scnd_idx_num + 1);

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
            next_scnd_idx_num,

            prim_lsm,
            scnd_idxs: RwLock::new(scnd_idxs),

            commit_ver_state,
            commit_mutex: Mutex::new(()),
            is_terminating: AtomicBool::new(false),
            job_cv_tx,
        })
    }

    pub fn format_new_scnd_idx_dir_path(&self) -> PathBuf {
        let num = self.next_scnd_idx_num.fetch_add(1, Ordering::SeqCst);
        let dir_name = PathNameNum::from(num).format_hex();
        let dir_path = self.db_dir_path.join(ALL_SCND_IDXS_DIR_NAME).join(dir_name);
        dir_path
    }
    fn parse_scnd_idx_dir_num<P: AsRef<Path>>(dir_path: P) -> Result<PathNameNum> {
        let dir_path = dir_path.as_ref();
        let maybe_file_name = dir_path.file_name().and_then(|os_str| os_str.to_str());
        let res_file_name =
            maybe_file_name.ok_or(anyhow!("Unexpected scnd_idx dir path {:?}", dir_path));
        res_file_name.and_then(|file_name| PathNameNum::parse_hex(file_name))
    }

    pub fn send_job_cv(&self) {
        self.job_cv_tx.send(()).ok();
    }
}

mod gc_job;
