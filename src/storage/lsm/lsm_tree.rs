use crate::ds_n_a::persisted_u64::PersistedU64;
use crate::storage::fs_utils::{self, UniqueId};
use crate::storage::lsm::{MemLog, SSTable};
use crate::storage::serde::{OptDatum, Serializable};
use anyhow::Result;
use std::path::{Path, PathBuf};

const LOG_FILE_NAME: &str = "commit_log.kv";
const SSTABLES_DIR_NAME: &str = "sstables";
const UNIQUE_ID_FILE_NAME: &str = "unique_id.u64";

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
/// One [`MemLog`] holds the most recently inserted `{key: value}` in a sorted in-memory table.
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
    memlog: MemLog<K, V>,
    sstables: Vec<SSTable<K, V>>,
    sstables_dir_path: PathBuf,
    unique_id: PersistedU64<UniqueId>,
}

impl<K, V> LSMTree<K, V>
where
    K: Serializable + Ord + Clone,
    OptDatum<V>: Serializable,
{
    pub fn load_or_new<P: AsRef<Path>>(lsm_dir_path: P) -> Result<Self> {
        let log_file_path = lsm_dir_path.as_ref().join(LOG_FILE_NAME);
        let sstables_dir_path = lsm_dir_path.as_ref().join(SSTABLES_DIR_NAME);
        let unique_id_file_path = lsm_dir_path.as_ref().join(UNIQUE_ID_FILE_NAME);
        std::fs::create_dir_all(&sstables_dir_path)?;

        let memlog = MemLog::load_or_new(&log_file_path)?;

        let sstables = fs_utils::read_dir_sorted(&sstables_dir_path)?
            .into_iter()
            .map(SSTable::load)
            .collect::<Result<Vec<_>>>()?;

        let unique_id = PersistedU64::load_or_new(unique_id_file_path)?;

        Ok(Self {
            memlog,
            sstables,
            sstables_dir_path,
            unique_id,
        })
    }
}

mod gc;
mod opers;
