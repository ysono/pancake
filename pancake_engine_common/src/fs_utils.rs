use anyhow::{anyhow, Result};
use derive_more::{Deref, From};
use std::fs;
use std::iter::Iterator;
use std::path::{Path, PathBuf};

/// A strictly increasing `u64` that is used to prevent file/dir name collisions.
#[derive(From, Deref, PartialEq, Eq, PartialOrd, Ord)]
pub struct PathNameNum(u64);
impl PathNameNum {
    pub fn format_hex(&self) -> String {
        format!("{:016x}", self.0)
    }

    pub fn parse_hex<S: AsRef<str>>(s: S) -> Result<Self> {
        let i = u64::from_str_radix(&s.as_ref()[..16], 16).map_err(|e| anyhow!(e))?;
        Ok(Self(i))
    }

    pub fn get_and_inc(&mut self) -> Self {
        let ret = Self(self.0);
        self.0 += 1;
        ret
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
