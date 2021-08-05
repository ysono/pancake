use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::mem;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};

use crate::storage::serde::{self, KeyValueIterator, Serializable};
use crate::storage::sstable::SSTable;
use crate::storage::utils;

static COMMIT_LOGS_DIR_PATH: &'static str = "commit_logs";
static SSTABLES_DIR_PATH: &'static str = "sstables";
static MEMTABLE_FLUSH_SIZE_THRESH: usize = 7;
static SSTABLE_COMPACT_COUNT_THRESH: usize = 4;

pub struct LSMTree<K, V> {
    path: PathBuf,
    memtable: BTreeMap<K, V>,
    commit_log_path: PathBuf,
    commit_log: File,
    memtable_in_flush: Option<BTreeMap<K, V>>,
    sstables: Vec<SSTable<K, V>>,
}

impl<K, V> LSMTree<K, V>
where
    K: Serializable + Ord + Clone,
    V: Serializable + Clone,
{
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        std::fs::create_dir_all(path.as_ref().join(COMMIT_LOGS_DIR_PATH))?;
        std::fs::create_dir_all(path.as_ref().join(SSTABLES_DIR_PATH))?;

        let mut memtable = BTreeMap::<K, V>::new();
        let mut commit_log_path = None;
        for path in utils::read_dir_sorted(path.as_ref().join(COMMIT_LOGS_DIR_PATH))? {
            Self::read_commit_log(&path, &mut memtable)?;
            commit_log_path = Some(path);
        }

        let commit_log_path = commit_log_path.unwrap_or(utils::new_timestamped_path(
            path.as_ref().join(COMMIT_LOGS_DIR_PATH),
        ));
        let commit_log = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&commit_log_path)?;

        let sstables = utils::read_dir_sorted(path.as_ref().join(SSTABLES_DIR_PATH))?
            .into_iter()
            .map(SSTable::read_from_file)
            .collect::<Result<Vec<_>>>()?;

        let ret = Self {
            path: path.as_ref().into(),
            memtable,
            commit_log_path,
            commit_log,
            memtable_in_flush: None,
            sstables,
        };
        Ok(ret)
    }

    fn read_commit_log(path: &PathBuf, memtable: &mut BTreeMap<K, V>) -> Result<()> {
        let file = File::open(path)?;
        let iter = KeyValueIterator::<K, V>::from(file);
        for file_data in iter {
            let (key, val) = file_data?;
            memtable.insert(key, val);
        }
        Ok(())
    }

    fn flush_memtable(&mut self) -> Result<()> {
        let new_cl_path = utils::new_timestamped_path(self.path.join(COMMIT_LOGS_DIR_PATH));
        let new_cl = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&new_cl_path)?;
        let old_cl_path: PathBuf;
        {
            // TODO MutexGuard here
            let old_mt = mem::replace(&mut self.memtable, Default::default());
            self.memtable_in_flush = Some(old_mt);

            self.commit_log = new_cl;
            old_cl_path = mem::replace(&mut self.commit_log_path, new_cl_path);
        }

        let mtf = self
            .memtable_in_flush
            .as_ref()
            .ok_or(anyhow!("Unexpected error: no memtable being flushed"))?;
        let new_sst = SSTable::write_from_mem(
            mtf,
            utils::new_timestamped_path(self.path.join(SSTABLES_DIR_PATH)),
        )?;

        {
            // TODO MutexGuard here
            self.sstables.push(new_sst);
            self.memtable_in_flush.take();
        }
        fs::remove_file(old_cl_path)?;

        if self.sstables.len() >= SSTABLE_COMPACT_COUNT_THRESH {
            self.compact_sstables()?;
        }

        Ok(())
    }

    fn compact_sstables(&mut self) -> Result<()> {
        let new_table_path = utils::new_timestamped_path(self.path.join(SSTABLES_DIR_PATH));
        let new_tables = SSTable::compact(new_table_path, &self.sstables)?;

        // TODO MutexGuard here
        // In async version, we will have to assume that new sstables may have been created while we were compacting, so we won't be able to just swap.
        let old_tables = mem::replace(&mut self.sstables, new_tables);
        for table in old_tables {
            table.remove_file()?;
        }

        Ok(())
    }

    fn put_impl(&mut self, k: K, v: V) -> Result<()> {
        serde::serialize_kv(&k, &v, &mut self.commit_log)?;

        self.memtable.insert(k, v);

        if self.memtable.len() >= MEMTABLE_FLUSH_SIZE_THRESH {
            self.flush_memtable()?;
        }

        Ok(())
    }

    pub fn put(&mut self, k: K, v: V) -> Result<()> {
        self.put_impl(k, v)
    }

    pub fn get(&self, k: K) -> Result<Option<V>> {
        if let Some(v) = self.memtable.get(&k) {
            return Ok(Some(v.clone()));
        }
        if let Some(mtf) = &self.memtable_in_flush {
            if let Some(v) = mtf.get(&k) {
                return Ok(Some(v.clone()));
            }
        }
        // TODO bloom filter here
        for ss in self.sstables.iter().rev() {
            let v = ss.get(&k)?;
            if v.is_some() {
                return Ok(v);
            }
        }
        Ok(None)
    }
}
