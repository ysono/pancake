use crate::lsm::{
    unit::{CommitInfo, CommitVer, CommittedUnit, UnitDir},
    LsmState,
};
use anyhow::{anyhow, Context, Result};
use pancake_engine_common::fs_utils::{AntiCollisionParentDir, NamePattern};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::path::Path;

pub struct LsmDir {
    dir: AntiCollisionParentDir,
}

impl LsmDir {
    pub fn load_or_new<P: AsRef<Path>>(lsm_dir_path: P) -> Result<(Self, LsmState)> {
        let lsm_dir_path = lsm_dir_path.as_ref();

        let (pq, dir) = Self::collect_committed_unit_dirs(&lsm_dir_path)?;

        let (committed_units, next_commit_ver) = Self::load_committed_units(pq)?;

        let lsm_state = LsmState::new(committed_units, next_commit_ver);

        let lsm_dir = Self { dir };

        Ok((lsm_dir, lsm_state))
    }

    /// Returns:
    /// - tup.0 = Priority queue of unit dirs that contain valid commit info.
    /// - tup.1 = The anti-filename-collision dir abstraction.
    fn collect_committed_unit_dirs<P: AsRef<Path>>(
        lsm_dir_path: P,
    ) -> Result<(BinaryHeap<CIUD>, AntiCollisionParentDir)> {
        let mut ciuds = vec![];
        let lsm_dir = AntiCollisionParentDir::load_or_new(
            lsm_dir_path,
            NamePattern::new("", ""),
            |child_path, res_child_num| -> Result<()> {
                res_child_num.with_context(|| {
                    format!("Unexpected path under the lsm dir. {child_path:?}")
                })?;

                let unit_dir = UnitDir::from(child_path);

                let commit_info = unit_dir.load_commit_info()
                .with_context(|| format!("Error loading commit info for a unit dir. This dir contains non-committed data. A prior writer failed to remove this dir. You should remove this dir manually. {:?}", unit_dir.path()))?;

                ciuds.push(CIUD {
                    commit_info,
                    unit_dir,
                });

                Ok(())
            },
        )?;

        let pq = BinaryHeap::from(ciuds);

        Ok((pq, lsm_dir))
    }

    /// Returns:
    /// - Ordered vec of committed units, from highest to lowest commit versions.
    /// - The next commit ver.
    fn load_committed_units(mut pq: BinaryHeap<CIUD>) -> Result<(Vec<CommittedUnit>, CommitVer)> {
        let next_commit_ver = match pq.peek() {
            None => CommitVer::AT_EMPTY_DATASTORE,
            Some(committed_unit) => committed_unit.commit_info.commit_ver_hi_incl().inc(),
        };

        let mut committed_units = Vec::<CommittedUnit>::new();
        while !pq.is_empty() {
            let ciud = pq.pop().unwrap();
            if let Some(last_unit) = committed_units.last() {
                if last_unit.commit_info.commit_ver_lo_incl()
                    <= ciud.commit_info.commit_ver_hi_incl()
                {
                    return Err(anyhow!("An overlapping commit ver range was found. A prior F+C failed to remove this dir. You should remove this dir manually. {:?}", ciud.unit_dir.path()));
                }
            }

            let unit = CommittedUnit::load(ciud.unit_dir, ciud.commit_info)?;
            committed_units.push(unit);
        }

        Ok((committed_units, next_commit_ver))
    }

    pub fn format_new_unit_dir_path(&self) -> UnitDir {
        let unit_dir_path = self.dir.format_new_child_path();
        UnitDir::from(unit_dir_path)
    }
}

#[derive(PartialEq, Eq)]
struct CIUD {
    commit_info: CommitInfo,
    unit_dir: UnitDir,
}

impl PartialOrd for CIUD {
    /// Compare
    /// 1. [`CommitInfo::commit_ver_hi_incl`]
    /// 1. [`CommitInfo::replacement_num`]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let slf_commit_ver = &self.commit_info.commit_ver_hi_incl;
        let oth_commit_ver = &other.commit_info.commit_ver_hi_incl;
        let commit_ver_ord = slf_commit_ver.cmp(oth_commit_ver);

        let ord = commit_ver_ord.then_with(|| {
            let slf_rn = &self.commit_info.replacement_num;
            let oth_rn = &other.commit_info.replacement_num;
            slf_rn.cmp(oth_rn)
        });

        Some(ord)
    }
}
impl Ord for CIUD {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}
