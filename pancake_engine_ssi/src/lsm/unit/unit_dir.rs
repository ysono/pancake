use crate::{db_state::ScndIdxNum, lsm::unit::CommitInfo};
use anyhow::Result;
use derive_more::From;
use pancake_engine_common::fs_utils::{self, NamePattern, PathNameNum};
use std::path::{Path, PathBuf};

const PI_KV_FILE_NAME: &str = "pi.kv";
const SI_KV_FILE_NAME_PFX: &str = "si-";
const SI_KV_FILE_NAME_EXT: &str = ".kv";
const COMMIT_INFO_FILE_NAME: &str = "commit_info.txt";

#[derive(From, PartialEq, Eq)]
pub struct UnitDir(PathBuf);

impl UnitDir {
    pub fn path(&self) -> &PathBuf {
        &self.0
    }

    /* Primary index */
    pub fn format_prim_file_path(&self) -> PathBuf {
        self.0.join(PI_KV_FILE_NAME)
    }

    /* Secondary indexes */
    fn scnd_file_name_pattern() -> NamePattern {
        NamePattern::new(SI_KV_FILE_NAME_PFX, SI_KV_FILE_NAME_EXT)
    }
    pub fn format_scnd_file_path(&self, si_num: ScndIdxNum) -> PathBuf {
        let path_name_num: PathNameNum = si_num.into();
        let file_name = Self::scnd_file_name_pattern().format(path_name_num);
        let file_path = self.0.join(file_name);
        file_path
    }
    fn parse_scnd_file_num<P: AsRef<Path>>(file_path: P) -> Option<ScndIdxNum> {
        let file_path = file_path.as_ref();
        let file_name = file_path.file_name().and_then(|os_str| os_str.to_str());
        let path_name_num = file_name.and_then(|s| Self::scnd_file_name_pattern().parse(s).ok());
        let si_num = path_name_num.map(|n| ScndIdxNum::from(n));
        si_num
    }
    pub fn list_scnd_file_paths<'a>(
        &'a self,
    ) -> Result<impl 'a + Iterator<Item = Result<(PathBuf, ScndIdxNum)>>> {
        let ret_iter = fs_utils::read_dir(&self.0)?.filter_map(|res_path| {
            res_path
                .map(|path| Self::parse_scnd_file_num(&path).map(|si_num| (path, si_num)))
                .transpose()
        });
        Ok(ret_iter)
    }

    /* Commit info */
    pub fn format_commit_info_file_path(&self) -> PathBuf {
        self.0.join(COMMIT_INFO_FILE_NAME)
    }
    pub fn load_commit_info(&self) -> Result<CommitInfo> {
        let file_path = self.format_commit_info_file_path();
        CommitInfo::deser(file_path)
    }
}
