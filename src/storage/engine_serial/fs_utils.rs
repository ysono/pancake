use anyhow::{anyhow, Result};
use derive_more::{Deref, DerefMut, From};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(From, Deref, DerefMut, Clone, Copy)]
pub struct UniqueId(u64);
impl UniqueId {
    pub fn to_alphanum_orderable_string(&self) -> String {
        format!("{:0>20}", self.0)
    }
    pub fn to_shortform_string(&self) -> String {
        format!("{}", self.0)
    }
}

pub fn read_dir<P: AsRef<Path>>(parent_path: P) -> Result<impl Iterator<Item = Result<PathBuf>>> {
    let iter = fs::read_dir(parent_path)
        .map_err(|e| anyhow!(e))?
        .map(|res_entry| res_entry.map_err(|e| anyhow!(e)).map(|entry| entry.path()));
    Ok(iter)
}

pub fn read_dir_sorted<P: AsRef<Path>>(parent_path: P) -> Result<Vec<PathBuf>> {
    let mut entries = read_dir(parent_path)?.collect::<Result<Vec<_>>>()?;
    entries.sort();
    Ok(entries)
}
