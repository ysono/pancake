use crate::storage::engine_ssi::entryset::{
    CommitInfo, CommitVer, CommittedEntrySetInfo, Memtable, Timestamp, WritableMemLog,
};
use crate::storage::serde::{Deser, KeyValueIterator, OptDatum};
use anyhow::Result;
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};

/// A MemLog is a sorted dictionary (called Memtable), backed up by a log file.
pub struct ReadonlyMemLog<K, V> {
    pub memtable: Memtable<K, V>,
    pub entryset_info: CommittedEntrySetInfo,
}

impl<K, V> ReadonlyMemLog<K, V>
where
    K: Deser + Ord,
    OptDatum<V>: Deser,
{
    pub fn from(mut w_memlog: WritableMemLog<K, V>, commit_ver: CommitVer) -> Result<Self> {
        w_memlog.log_writer.flush()?;

        let commit_info = CommitInfo {
            commit_ver_hi_incl: commit_ver,
            commit_ver_lo_incl: commit_ver,
            timestamp: Timestamp::default(),
        };
        let commit_info_path = w_memlog.entryset_dir.commit_info_file_path();
        let commit_info_file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&commit_info_path)?;
        let mut commit_info_writer = BufWriter::new(commit_info_file);
        commit_info.ser(&mut commit_info_writer)?;
        commit_info_writer.flush()?;

        let entryset_info = CommittedEntrySetInfo {
            commit_info,
            entryset_dir: w_memlog.entryset_dir,
        };

        Ok(ReadonlyMemLog {
            memtable: w_memlog.memtable,
            entryset_info,
        })
    }

    pub fn load(entryset_info: CommittedEntrySetInfo) -> Result<Self> {
        let log_path = entryset_info.entryset_dir.memlog_file_path();
        let log_file = File::open(&log_path)?;
        let file_iter = KeyValueIterator::<K, OptDatum<V>>::from(log_file);
        let mut memtable = BTreeMap::new();
        for res_kv in file_iter {
            let (k, v) = res_kv?;
            memtable.insert(k, v);
        }
        let memtable = Memtable::from(memtable);

        Ok(Self {
            memtable,
            entryset_info,
        })
    }
}

impl<K, V> ReadonlyMemLog<K, V> {
    pub fn remove_entryset_dir(self) -> Result<()> {
        fs::remove_dir_all(&*self.entryset_info.entryset_dir)?;
        Ok(())
    }
}
