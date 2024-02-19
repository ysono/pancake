use anyhow::{Context, Result};
use fs2::FileExt;
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom};
use std::iter::Iterator;
use std::path::{Path, PathBuf};

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

pub fn open_file<P: AsRef<Path>>(path: P, oo: &OpenOptions) -> Result<File> {
    let path = path.as_ref();
    oo.open(path).with_context(|| format!("open {path:?}"))
}

pub fn lock_file<P: AsRef<Path>>(path: P) -> Result<File> {
    let path = path.as_ref();
    let file = open_file(path, OpenOptions::new().read(true))?;
    file.try_lock_exclusive()
        .context(format!("try_lock_exclusive {path:?}"))?;
    Ok(file)
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

pub fn rename_file<P: AsRef<Path>, Q: AsRef<Path>>(from: P, to: Q) -> Result<()> {
    let from = from.as_ref();
    let to = to.as_ref();
    fs::rename(from, to).with_context(|| format!("rename {from:?} {to:?}"))
}

pub fn remove_file<P: AsRef<Path>>(path: P) -> Result<()> {
    let path = path.as_ref();
    fs::remove_file(path).with_context(|| format!("remove_file {path:?}"))
}

pub fn remove_dir_all<P: AsRef<Path>>(path: P) -> Result<()> {
    let path = path.as_ref();
    fs::remove_dir_all(path).with_context(|| format!("remove_dir_all {path:?}"))
}
