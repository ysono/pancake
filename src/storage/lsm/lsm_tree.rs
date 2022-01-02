use crate::storage::lsm::{MemLog, SSTable};
use crate::storage::serde::{OptDatum, Serializable};
use crate::storage::utils;
use anyhow::Result;
use std::path::{Path, PathBuf};

const COMMIT_LOG_FILE_NAME: &'static str = "commit_log.kv";
const SSTABLES_DIR_NAME: &'static str = "sstables";

#[allow(rustdoc::private_intra_doc_links)]
/// An LSMTree is an abstraction of a sorted dictionary.
///
/// ### API:
///
/// The exposed operations are: `put one`, `get one`, `get range`.
///
/// Values are immutable. They cannot be modified in-place, and must be replaced.
///
/// ### Internals:
///
/// One [`MemLog`] holds the most recently inserted `{key: value}` mapping in an in-memory table.
///
/// The [`MemLog`] is occasionally flushed into an [`SSTable`].
///
/// Multiple [`SSTable`]s are occasionally compacted into one [`SSTable`].
///
/// ### Querying:
///
/// A `put` operation accesses the Memtable of the [`MemLog`] only.
///
/// A `get` operation generally accesses the [`MemLog`] and all [`SSTable`]s.
///
/// When the same key exists in multiple internal tables, only the result from the newest table is retrieved.
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
