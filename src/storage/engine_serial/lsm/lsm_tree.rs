use crate::storage::engine_serial::lsm::{MemLog, SSTable};
use crate::storage::engines_common::fs_utils::{self, PathNameNum};
use crate::storage::serde::{OptDatum, Serializable};
use anyhow::{anyhow, Result};
use std::path::{Path, PathBuf};

const LOG_FILE_NAME: &str = "commit_log.kv";
const SSTABLES_DIR_NAME: &str = "sstables";

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
    next_sstable_num: PathNameNum,
}

impl<K, V> LSMTree<K, V>
where
    K: Serializable + Ord + Clone,
    OptDatum<V>: Serializable,
{
    pub fn load_or_new<P: AsRef<Path>>(lsm_dir_path: P) -> Result<Self> {
        let log_file_path = lsm_dir_path.as_ref().join(LOG_FILE_NAME);
        let sstables_dir_path = lsm_dir_path.as_ref().join(SSTABLES_DIR_NAME);
        std::fs::create_dir_all(&sstables_dir_path)?;

        let memlog = MemLog::load_or_new(&log_file_path)?;

        let sstables_file_paths = fs_utils::read_dir_sorted(&sstables_dir_path)?;
        let next_sstable_num = match sstables_file_paths.last() {
            None => PathNameNum::from(0),
            Some(file_path) => {
                let mut num = Self::parse_sstable_file_num(file_path)?;
                num.get_and_inc();
                num
            }
        };
        let sstables = sstables_file_paths
            .into_iter()
            .map(SSTable::load)
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            memlog,
            sstables,
            sstables_dir_path,
            next_sstable_num,
        })
    }

    pub fn format_new_sstable_file_path(&mut self) -> Result<PathBuf> {
        let num = self.next_sstable_num.get_and_inc();
        let sst_path = self
            .sstables_dir_path
            .join(format!("{}.kv", num.format_hex()));
        Ok(sst_path)
    }
    fn parse_sstable_file_num<P: AsRef<Path>>(file_path: P) -> Result<PathNameNum> {
        let file_path = file_path.as_ref();
        let maybe_file_stem = file_path.file_stem().and_then(|os_str| os_str.to_str());
        let res_file_stem =
            maybe_file_stem.ok_or(anyhow!("Unexpected SSTable file path {:?}", file_path));
        res_file_stem.and_then(|file_stem| PathNameNum::parse_hex(file_stem))
    }

    // pub(self) format_new_sstable_file_path(&)
}

mod gc;
mod opers;
