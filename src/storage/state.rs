use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Default, Debug)]
pub struct State {
    pub memtable: BTreeMap<u32, String>,
    pub commit_log_path: PathBuf,
    pub ss_table_idxs: Vec<BTreeMap<u32, String>>,
    pub ss_table_paths: Vec<PathBuf>,
}
