use anyhow::{anyhow, Result};
use pancake_engine_common::fs_utils::{self, AntiCollisionParentDir, NamePattern};
use std::path::{Path, PathBuf};

pub struct ScndIdxCreationsDir {
    dir: AntiCollisionParentDir,
}

impl ScndIdxCreationsDir {
    pub fn load_or_new<P: AsRef<Path>>(dir_path: P) -> Result<Self> {
        let dir = AntiCollisionParentDir::load_or_new(
            dir_path,
            NamePattern::new("", ""),
            |child_path, _child_num| {
                eprintln!("A prior second index creation job failed to remove its working dir. You should remove this dir manually. {child_path:?}");
                Ok(())
            },
        )?;
        Ok(Self { dir })
    }

    pub(in crate::opers::sicr) fn create_new_job_dir(&self) -> Result<ScndIdxCreationJobDir> {
        let job_dir_path = self.dir.format_new_child_path();
        ScndIdxCreationJobDir::new(job_dir_path)
    }
}

pub(in crate::opers::sicr) struct ScndIdxCreationJobDir {
    dir: AntiCollisionParentDir,
}

impl ScndIdxCreationJobDir {
    fn new(dir_path: PathBuf) -> Result<Self> {
        let dir = AntiCollisionParentDir::load_or_new(
            dir_path,
            NamePattern::new("", ".kv"),
            |child_path, _child_num| {
                return Err(anyhow!("Found file inside what is expected to be a brand new empty dir. {child_path:?}"));
            },
        )?;
        Ok(Self { dir })
    }

    pub fn format_new_kv_file_path(&self) -> PathBuf {
        self.dir.format_new_child_path()
    }

    pub fn remove_dir(self) -> Result<()> {
        fs_utils::remove_dir_all(self.dir.parent_dir_path())?;
        Ok(())
    }
}
