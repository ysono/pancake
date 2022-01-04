use anyhow::{anyhow, Result};
use derive_more::{Deref, DerefMut, From};
use std::cmp::{Ord, Ordering};
use std::io::{BufReader, BufWriter, Read, Write};

#[derive(From, Deref, DerefMut, Copy, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct CommitVer(u64);

/// A commit to an empty LSM Tree should be version 1, not 0.
/// 0 should be reserved for the output of a new secondary index's
/// async creation job.
/// (In practice this is not a problem.
/// If primary LSM is empty, scnd idx creation job would not produce commit ver 0.
/// If primary LSM is non-empty, the next commit ver is > 0, so there is no collision.
/// But it feels saner to separate them out.)
pub const CLEAN_SLATE_NEXT_COMMIT_VER: CommitVer = CommitVer(1);
pub const SCND_IDX_ASYNC_BUILT_COMMIT_VER: CommitVer = CommitVer(0);

/// A time-ordered integer that decides ordering among multiple [`CommitInfo`] instances
///     with overalpping commit ver ranges.
/// Its integer value does not correspond to the system clock.
#[derive(Default, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct Timestamp(u64);

impl Timestamp {
    pub fn inc_from(prevs: impl Iterator<Item = Self>) -> Self {
        let prev_max = prevs.map(|ts| ts.0).max();
        let next = match prev_max {
            None => 0,
            Some(prev) => prev + 1,
        };
        Self(next)
    }
}

#[derive(PartialEq, Eq)]
pub struct CommitInfo {
    pub commit_ver_hi_incl: CommitVer,
    pub commit_ver_lo_incl: CommitVer,
    pub timestamp: Timestamp,
}

impl CommitInfo {
    pub fn ser<W: Write>(&self, w: &mut BufWriter<W>) -> Result<()> {
        write!(
            w,
            "{},{},{}",
            self.commit_ver_hi_incl.0, self.commit_ver_lo_incl.0, self.timestamp.0
        )?;
        Ok(())
    }

    pub fn deser<R: Read>(r: &mut BufReader<R>) -> Result<Self> {
        let mut s = String::new();
        r.read_to_string(&mut s)?;

        let mut split = s.split(',');

        let hi = split.next().ok_or(anyhow!("Invalid CommitInfo file"))?;
        let hi = hi.parse::<u64>()?;

        let lo = split.next().ok_or(anyhow!("Invalid CommitInfo file"))?;
        let lo = lo.parse::<u64>()?;

        let ts = split.next().ok_or(anyhow!("Invalid CommitInfo file"))?;
        let ts = ts.parse::<u64>()?;

        if split.next().is_some() {
            return Err(anyhow!("Invalid CommitInfo file"));
        }

        Ok(Self {
            commit_ver_hi_incl: CommitVer(hi),
            commit_ver_lo_incl: CommitVer(lo),
            timestamp: Timestamp(ts),
        })
    }
}

impl PartialOrd for CommitInfo {
    /// asc `commit_ver_hi_incl`, then asc `timestamp`.
    fn partial_cmp(&self, other: &CommitInfo) -> Option<Ordering> {
        let cmt_ver_ord = self.commit_ver_hi_incl.cmp(&other.commit_ver_hi_incl);
        let ord = cmt_ver_ord.then_with(|| self.timestamp.cmp(&other.timestamp));
        Some(ord)
    }
}
impl Ord for CommitInfo {
    fn cmp(&self, other: &CommitInfo) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}
