use crate::ds_n_a::atomic_linked_list::AtomicLinkedList;
use crate::storage::engine_ssi::lsm_state::{
    unit::{CommitInfo, CommitVer, CommittedUnit, UnitDir},
    ListVer, LsmElem, LsmElemContent, LsmState,
};
use crate::storage::engines_common::fs_utils::{self, PathNameNum};
use anyhow::{anyhow, Result};
use std::cmp::{self, Ordering};
use std::collections::{BTreeMap, BinaryHeap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtmOrdering};

#[derive(PartialEq, Eq)]
struct CIUD {
    commit_info: CommitInfo,
    unit_dir: UnitDir,
}

impl PartialOrd for CIUD {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.commit_info.cmp(&other.commit_info))
    }
}
impl Ord for CIUD {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

pub struct LsmDir {
    lsm_dir_path: PathBuf,
    next_unit_dir_num: AtomicU64,
}

impl LsmDir {
    pub fn load_or_new_lsm_dir<P: AsRef<Path>>(lsm_dir_path: P) -> Result<(Self, LsmState)> {
        let lsm_dir_path = lsm_dir_path.as_ref();
        fs::create_dir_all(&lsm_dir_path)?;

        let (pq, next_unit_dir_num) = Self::collect_committed_unit_dirs(&lsm_dir_path)?;

        let (committed_units, next_commit_ver) = Self::load_committed_units(pq)?;

        let curr_list_ver = ListVer::from(0);

        let list = Self::build_list(committed_units, curr_list_ver);

        let lsm_state = LsmState::new(list, next_commit_ver, curr_list_ver, BTreeMap::new());

        let mgr = Self {
            lsm_dir_path: lsm_dir_path.into(),
            next_unit_dir_num: AtomicU64::new(*next_unit_dir_num),
        };

        Ok((mgr, lsm_state))
    }

    /// Returns:
    /// - Priority queue of unit dirs that contained valid commit info.
    /// - The next anti-filename-collision dir num.
    fn collect_committed_unit_dirs<P: AsRef<Path>>(
        lsm_dir_path: P,
    ) -> Result<(BinaryHeap<CIUD>, PathNameNum)> {
        let mut pq = BinaryHeap::<CIUD>::new();
        let mut max_unit_dir_num = PathNameNum::from(0);
        for res_unit_dir_path in fs_utils::read_dir(lsm_dir_path)? {
            let unit_dir_path = res_unit_dir_path?;

            let unit_dir_num = Self::parse_unit_dir_num(&unit_dir_path)?;
            max_unit_dir_num = cmp::max(max_unit_dir_num, unit_dir_num);

            let unit_dir = UnitDir::from(unit_dir_path);
            match unit_dir.load_commit_info() {
                Err(e) => {
                    eprintln!(
                        "Error loading commit info from {:?}. {:?} Maybe remove unit dir manually?",
                        e, &*unit_dir
                    );
                }
                Ok(commit_info) => {
                    pq.push(CIUD {
                        commit_info,
                        unit_dir,
                    });
                }
            }
        }
        let next_unit_dir_num = PathNameNum::from(*max_unit_dir_num + 1);

        Ok((pq, next_unit_dir_num))
    }
    /// Returns:
    /// - Ordered vec of committed units, from highest to lowest commit versions.
    /// - The next commit ver.
    fn load_committed_units(mut pq: BinaryHeap<CIUD>) -> Result<(Vec<CommittedUnit>, CommitVer)> {
        let next_commit_ver = match pq.peek() {
            None => CommitVer::from(0),
            Some(committed_unit) => {
                let max = committed_unit.commit_info.commit_ver_hi_incl();
                CommitVer::from(**max + 1)
            }
        };

        let mut committed_units = Vec::<CommittedUnit>::new();
        // pq pops in the order of 1) commit_ver_hi_incl desc, 2) timestamp_num desc.
        while !pq.is_empty() {
            let ciud = pq.pop().unwrap();
            if let Some(last_unit) = committed_units.last() {
                if last_unit.commit_info.commit_ver_lo_incl()
                    <= ciud.commit_info.commit_ver_hi_incl()
                {
                    eprintln!(
                        "An overlapping commit ver range was found in {:?}. Maybe remove unit dir manually?", &*ciud.unit_dir
                    );
                    continue;
                }
            }

            let unit = CommittedUnit::load(ciud.unit_dir, ciud.commit_info)?;
            committed_units.push(unit);
        }

        Ok((committed_units, next_commit_ver))
    }
    fn build_list(
        committed_units: Vec<CommittedUnit>,
        curr_list_ver: ListVer,
    ) -> AtomicLinkedList<LsmElem> {
        let list_elems = committed_units.into_iter().map(|unit| LsmElem {
            content: LsmElemContent::Unit(unit),
            traversable_list_ver_lo_incl: curr_list_ver,
        });
        AtomicLinkedList::from_elems(list_elems)
    }
}

impl LsmDir {
    pub fn format_new_unit_dir_path(&self) -> UnitDir {
        let num = self.next_unit_dir_num.fetch_add(1, AtmOrdering::SeqCst);
        let numstr = PathNameNum::from(num).format_hex();
        let path = self.lsm_dir_path.join(numstr);
        UnitDir::from(path)
    }
    fn parse_unit_dir_num<P: AsRef<Path>>(dir_path: P) -> Result<PathNameNum> {
        let dir_path = dir_path.as_ref();
        let maybe_file_name = dir_path.file_name().and_then(|os_str| os_str.to_str());
        let res_file_name =
            maybe_file_name.ok_or(anyhow!("Unexpected unit dir path {:?}", dir_path));
        res_file_name.and_then(|file_name| PathNameNum::parse_hex(file_name))
    }
}
