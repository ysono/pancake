use crate::storage::engine_ssi::entryset::{CommitInfo, CommittedEntrySetInfo};
use anyhow::Result;
use derive_more::{Deref, From};
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

pub enum LoadCommitInfoResult {
    NotFound(EntrySetDir),
    Committed(CommittedEntrySetInfo),
}

#[derive(From, Deref, PartialEq, Eq)]
pub struct EntrySetDir(PathBuf);

impl EntrySetDir {
    const MEMLOG_FILE_NAME: &'static str = "memlog.kv";
    const SSTABLE_FILE_NAME: &'static str = "sstable.kv";
    const COMMIT_INFO_FILE_NAME: &'static str = "commit_info.datum";

    pub fn memlog_file_path(&self) -> PathBuf {
        self.0.join(Self::MEMLOG_FILE_NAME)
    }
    pub fn sstable_file_path(&self) -> PathBuf {
        self.0.join(Self::SSTABLE_FILE_NAME)
    }
    pub fn commit_info_file_path(&self) -> PathBuf {
        self.0.join(Self::COMMIT_INFO_FILE_NAME)
    }

    pub fn load_commit_info(self) -> Result<LoadCommitInfoResult> {
        let path = self.commit_info_file_path();
        if path.exists() {
            let file = File::open(&path)?;
            let mut reader = BufReader::new(file);
            let cmt_info = CommitInfo::deser(&mut reader)?;

            let es_info = CommittedEntrySetInfo {
                commit_info: cmt_info,
                entryset_dir: self,
            };

            return Ok(LoadCommitInfoResult::Committed(es_info));
        } else {
            return Ok(LoadCommitInfoResult::NotFound(self));
        }
    }
}
