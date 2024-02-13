use anyhow::{Context, Result};
use pancake_engine_common::fs_utils::{self, AntiCollisionParentDir, NamePattern};
use pancake_engine_common::{SSTable, WritableMemLog};
use pancake_types::{serde::OptDatum, types::Serializable};
use std::path::Path;

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
/// One [`WritableMemLog`] holds the most recently inserted `{key: value}` in a sorted in-memory table.
///
/// The [`WritableMemLog`] is occasionally flushed into an [`SSTable`].
///
/// Multiple [`SSTable`]s are occasionally compacted into one [`SSTable`].
///
/// ### Querying:
///
/// A `put` operation accesses the Memtable of the [`WritableMemLog`] only.
///
/// A `get` operation generally accesses the [`WritableMemLog`] and all [`SSTable`]s.
///
/// When the same key exists in multiple internal tables, only the result from the newest table is retrieved.
pub struct LSMTree<K, V> {
    memlog: WritableMemLog<K, OptDatum<V>>,
    sstables: Vec<SSTable<K, OptDatum<V>>>,
    sstables_dir: AntiCollisionParentDir,
}

impl<K, V> LSMTree<K, V>
where
    K: Serializable + Ord + Clone,
    OptDatum<V>: Serializable,
{
    pub fn load_or_new<P: AsRef<Path>>(lsm_dir_path: P) -> Result<Self> {
        let log_file_path = lsm_dir_path.as_ref().join(LOG_FILE_NAME);
        let sstables_dir_path = lsm_dir_path.as_ref().join(SSTABLES_DIR_NAME);
        fs_utils::create_dir_all(&sstables_dir_path)?;

        let memlog = WritableMemLog::load_or_new(&log_file_path)?;

        let mut sstable_file_paths = vec![];
        let sstables_dir = AntiCollisionParentDir::load_or_new(
            sstables_dir_path,
            NamePattern::new("", ".kv"),
            |child_path, res_child_num| -> Result<()> {
                let child_num = res_child_num.with_context(|| {
                    format!("An sstables dir contains an unexpected child path {child_path:?}")
                })?;

                sstable_file_paths.push((child_path, child_num));

                Ok(())
            },
        )?;

        sstable_file_paths.sort_by_key(|(_child_path, child_num)| *child_num);
        let sstables = sstable_file_paths
            .into_iter()
            .map(|(child_path, _child_num)| SSTable::load(child_path))
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            memlog,
            sstables,
            sstables_dir,
        })
    }
}

mod gc;
mod opers;
