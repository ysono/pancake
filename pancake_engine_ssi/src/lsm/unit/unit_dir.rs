use crate::{db_state::ScndIdxNum, lsm::unit::CommitInfo};
use anyhow::Result;
use derive_more::{Deref, From};
use pancake_engine_common::fs_utils::{self, PathNameNum};
use std::path::{Path, PathBuf};

const PI_KV_FILE_NAME: &str = "pi.kv";
const SI_KV_FILE_NAME_PFX: &str = "si-";
const SI_KV_FILE_NAME_EXT: &str = ".kv";
const COMMIT_INFO_FILE_NAME: &str = "commit_info.txt";

#[derive(From, Deref, PartialEq, Eq)]
pub struct UnitDir(PathBuf);

impl UnitDir {
    pub fn format_prim_path(&self) -> PathBuf {
        self.join(PI_KV_FILE_NAME)
    }
    pub fn format_scnd_path(&self, num: ScndIdxNum) -> PathBuf {
        let numstr = PathNameNum::from(*num).format_hex();
        let file_name = format!("{}{}{}", SI_KV_FILE_NAME_PFX, numstr, SI_KV_FILE_NAME_EXT);
        self.join(file_name)
    }
    fn parse_scnd_file_num<P: AsRef<Path>>(file_path: P) -> Option<ScndIdxNum> {
        let file_path = file_path.as_ref();

        let ext = file_path.extension().and_then(|os_str| os_str.to_str());
        if ext != Some(SI_KV_FILE_NAME_EXT) {
            return None;
        }

        file_path
            .file_stem()
            .and_then(|os_str| os_str.to_str())
            .and_then(|stem| {
                if stem.starts_with(SI_KV_FILE_NAME_PFX) {
                    Some(&stem[SI_KV_FILE_NAME_PFX.len()..])
                } else {
                    None
                }
            })
            .and_then(|numstr| {
                PathNameNum::parse_hex(numstr)
                    .ok()
                    .map(|path_name_num| ScndIdxNum::from(*path_name_num))
            })
    }
    pub fn format_commit_info_path(&self) -> PathBuf {
        self.join(COMMIT_INFO_FILE_NAME)
    }

    pub fn load_commit_info(&self) -> Result<CommitInfo> {
        let file_path = self.format_commit_info_path();
        CommitInfo::deser(file_path)
    }

    pub fn list_scnd_paths<'a>(
        &'a self,
    ) -> Result<impl 'a + Iterator<Item = Result<(PathBuf, ScndIdxNum)>>> {
        let ret_iter = fs_utils::read_dir(&self.0)?.filter_map(|res_path| {
            res_path
                .map(|path| Self::parse_scnd_file_num(&path).map(|si_num| (path, si_num)))
                .transpose()
        });
        Ok(ret_iter)
    }
}
