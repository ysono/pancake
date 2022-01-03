use crate::storage::lsm::{MemLog, SSTable};
use crate::storage::serde::{OptDatum, Serializable};
use crate::storage::utils;
use anyhow::Result;
use std::path::{Path, PathBuf};

const COMMIT_LOG_FILE_NAME: &'static str = "commit_log.kv";
const SSTABLES_DIR_NAME: &'static str = "sstables";

pub struct LSMTree<K, V> {
    lsm_dir_path: PathBuf,
    memlog: MemLog<K, V>,
    sstables: Vec<SSTable<K, V>>,
}

impl<K, V> LSMTree<K, V>
where
    K: Serializable + Ord + Clone,
    OptDatum<V>: Serializable,
{
    pub fn load_or_new<P: AsRef<Path>>(lsm_dir_path: P) -> Result<Self> {
        let log_file_path = lsm_dir_path.as_ref().join(COMMIT_LOG_FILE_NAME);
        let ssts_dir_path = lsm_dir_path.as_ref().join(SSTABLES_DIR_NAME);
        std::fs::create_dir_all(&ssts_dir_path)?;

        let memlog = MemLog::load_or_new(&log_file_path)?;

        let sstables = utils::read_dir_sorted(ssts_dir_path)?
            .into_iter()
            .map(SSTable::load)
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            lsm_dir_path: lsm_dir_path.as_ref().into(),
            memlog,
            sstables,
        })
    }
}

mod gc;
mod opers;
