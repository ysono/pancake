use crate::storage::engine_ssi::entryset::{CommitInfo, EntrySetDir};
use std::cmp::Ordering;

#[derive(PartialEq, Eq)]
pub struct CommittedEntrySetInfo {
    pub commit_info: CommitInfo,
    pub entryset_dir: EntrySetDir,
}

impl PartialOrd for CommittedEntrySetInfo {
    /// asc `commit_info`.
    fn partial_cmp(&self, other: &CommittedEntrySetInfo) -> Option<Ordering> {
        self.commit_info.partial_cmp(&other.commit_info)
    }
}
impl Ord for CommittedEntrySetInfo {
    fn cmp(&self, other: &CommittedEntrySetInfo) -> Ordering {
        self.commit_info.cmp(&other.commit_info)
    }
}
