use crate::fs_utils;
use anyhow::{anyhow, Result};
use derive_more::{Constructor, Deref, From};
use std::cmp;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// A filesystem directory that formats unique child paths,
/// formatted using strictly increasing integers.
///
/// ```text
/// parent_dir/
///     <prefix>000...001<suffix>
///     <prefix>000...002<suffix>
///     ...
///     <prefix>fff...fff<suffix>
/// ```
pub struct AntiCollisionParentDir {
    parent_dir_path: PathBuf,
    child_name_pattern: NamePattern,
    next_child_num: AtomicU64,
}

impl AntiCollisionParentDir {
    pub fn load_or_new<P: AsRef<Path>>(
        parent_dir_path: P,
        child_name_pattern: NamePattern,
        mut handle_child_path: impl FnMut(PathBuf, Result<PathNameNum>) -> Result<()>,
    ) -> Result<Self> {
        let parent_dir_path = parent_dir_path.as_ref();

        fs_utils::create_dir_all(parent_dir_path)?;

        let mut max_child_num = PathNameNum(0);
        for child_path in fs_utils::read_dir(parent_dir_path)? {
            let child_path = child_path?;
            let child_name = child_path.file_name().unwrap();
            let child_name = child_name
                .to_str()
                .ok_or_else(|| anyhow!("Invalid file name. {child_name:?}"))?;

            let res_child_num = child_name_pattern.parse(child_name);

            if let Ok(child_num) = res_child_num {
                max_child_num = cmp::max(max_child_num, child_num);
            }

            handle_child_path(child_path, res_child_num)?;
        }
        let next_child_num = max_child_num.0 + 1;

        Ok(Self {
            parent_dir_path: parent_dir_path.into(),
            child_name_pattern,
            next_child_num: AtomicU64::from(next_child_num),
        })
    }

    pub fn parent_dir_path(&self) -> &PathBuf {
        &self.parent_dir_path
    }

    pub fn format_new_child_path(&self) -> PathBuf {
        let child_num = self.next_child_num.fetch_add(1, Ordering::SeqCst);
        let child_num = PathNameNum(child_num);
        let child_name = self.child_name_pattern.format(child_num);
        let child_path = self.parent_dir_path.join(child_name);
        child_path
    }
}

#[derive(Constructor)]
pub struct NamePattern {
    prefix: &'static str,
    suffix: &'static str,
}

impl NamePattern {
    pub fn format(&self, num: PathNameNum) -> String {
        format!("{}{}{}", self.prefix, num.format_hex(), self.suffix)
    }
    pub fn parse(&self, s: &str) -> Result<PathNameNum> {
        if s.starts_with(self.prefix) && s.ends_with(self.suffix) {
            let lo = self.prefix.len();
            let hi = s.len() - self.suffix.len();
            let middle = &s[lo..hi];
            return PathNameNum::parse_hex(middle);
        }
        Err(anyhow!("Wrong name format. {s}"))
    }
}

/// A strictly increasing `u64` that is used to prevent file/dir name collisions.
#[derive(From, Deref, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PathNameNum(u64);
impl PathNameNum {
    pub fn format_hex(&self) -> String {
        format!("{:016x}", self.0)
    }
    pub fn parse_hex<S: AsRef<str>>(s: S) -> Result<Self> {
        let s = s.as_ref();
        if s.len() != 16 {
            return Err(anyhow!("Not 16 chars long. {s}"));
        }
        let i = u64::from_str_radix(s, 16).map_err(|e| anyhow!(e))?;
        Ok(Self(i))
    }
}
