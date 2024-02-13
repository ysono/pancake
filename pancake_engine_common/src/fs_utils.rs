use anyhow::{anyhow, Context, Result};
use derive_more::{Deref, From};
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom};
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

pub fn create_dir_all<P: AsRef<Path>>(path: P) -> Result<()> {
    let path = path.as_ref();
    fs::create_dir_all(path).with_context(|| format!("create_dir_all {path:?}"))
}

pub fn read_dir<'a>(parent_path: &'a Path) -> Result<impl 'a + Iterator<Item = Result<PathBuf>>> {
    let iter = fs::read_dir(parent_path).with_context(|| format!("read_dir {parent_path:?}"))?;
    let iter = iter.map(move |res_entry| {
        res_entry
            .with_context(|| format!("read_dir entry {parent_path:?}"))
            .map(|entry| entry.path())
    });
    Ok(iter)
}

pub fn read_dir_sorted<P: AsRef<Path>>(parent_path: P) -> Result<Vec<PathBuf>> {
    let mut entries = read_dir(parent_path.as_ref())?.collect::<Result<Vec<_>>>()?;
    entries.sort();
    Ok(entries)
}

pub fn open_file<P: AsRef<Path>>(path: P, oo: &OpenOptions) -> Result<File> {
    let path = path.as_ref();
    oo.open(path).with_context(|| format!("open {path:?}"))
}

pub fn seek<P: AsRef<Path>>(
    mut seekable: impl Seek,
    sf: SeekFrom,
    implicit_path: P,
) -> Result<u64> {
    seekable
        .seek(sf)
        .with_context(|| format!("seek {:?}", implicit_path.as_ref()))
}

pub fn remove_file<P: AsRef<Path>>(path: P) -> Result<()> {
    let path = path.as_ref();
    fs::remove_file(path).with_context(|| format!("remove_file {path:?}"))
}

pub fn remove_dir_all<P: AsRef<Path>>(path: P) -> Result<()> {
    let path = path.as_ref();
    fs::remove_dir_all(path).with_context(|| format!("remove_dir_all {path:?}"))
}
