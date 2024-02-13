use crate::{
    db_state::ScndIdxNum,
    lsm::{
        entryset::CommittedEntrySet,
        unit::{
            CommitDataType, CommitInfo, CommitVer, CompactedUnit, StagingUnit, TimestampNum,
            UnitDir,
        },
    },
};
use anyhow::Result;
use pancake_engine_common::{ReadonlyMemLog, SSTable};
use pancake_types::{
    serde::OptDatum,
    types::{PKShared, PVShared, SVPKShared},
};
use std::collections::HashMap;
use std::fs;

pub struct CommittedUnit {
    pub prim: Option<CommittedEntrySet<PKShared, OptDatum<PVShared>>>,
    pub scnds: HashMap<ScndIdxNum, CommittedEntrySet<SVPKShared, OptDatum<PVShared>>>,
    pub dir: UnitDir,
    pub commit_info: CommitInfo,
}

impl CommittedUnit {
    /// Cost:
    /// - There should be no cost converting each `WritableMemLog` to `ReadonlyMemLog`.
    /// - There *is* a cost of serializing a CommitInfo.
    pub fn from_staging(stg: StagingUnit, commit_ver: CommitVer) -> Result<Self> {
        let prim: ReadonlyMemLog<PKShared, OptDatum<PVShared>> = stg.prim.into();
        let prim_entryset = CommittedEntrySet::RMemLog(prim);
        let scnds = stg
            .scnds
            .into_iter()
            .map(|(scnd_idx_num, w_memlog)| {
                let r_memlog: ReadonlyMemLog<SVPKShared, OptDatum<PVShared>> = w_memlog.into();
                let entryset = CommittedEntrySet::RMemLog(r_memlog);
                (scnd_idx_num, entryset)
            })
            .collect::<HashMap<_, _>>();

        let commit_info = CommitInfo {
            commit_ver_hi_incl: commit_ver,
            commit_ver_lo_incl: commit_ver,
            timestamp_num: TimestampNum::from(0),
            data_type: CommitDataType::MemLog,
        };
        let commit_info_path = stg.dir.format_commit_info_path();
        commit_info.ser(commit_info_path)?;

        Ok(Self {
            prim: Some(prim_entryset),
            scnds,
            dir: stg.dir,
            commit_info,
        })
    }

    /// Cost:
    /// - This constructor serializes CommitInfo (so caller shouldn't do it before).
    pub fn from_compacted(compacted: CompactedUnit, commit_info: CommitInfo) -> Result<Self> {
        let commit_info_path = compacted.dir.format_commit_info_path();
        commit_info.ser(commit_info_path)?;

        Ok(Self {
            prim: compacted.prim,
            scnds: compacted.scnds,
            dir: compacted.dir,
            commit_info,
        })
    }

    pub fn load(dir: UnitDir, commit_info: CommitInfo) -> Result<Self> {
        let prim = {
            let prim_path = dir.format_prim_path();
            if prim_path.exists() {
                let ces = match commit_info.data_type() {
                    CommitDataType::MemLog => {
                        CommittedEntrySet::RMemLog(ReadonlyMemLog::load(prim_path)?)
                    }
                    CommitDataType::SSTable => {
                        CommittedEntrySet::SSTable(SSTable::load(prim_path)?)
                    }
                };
                Some(ces)
            } else {
                None
            }
        };

        let mut scnds = HashMap::new();
        {
            for res in dir.list_scnd_paths()? {
                let (scnd_path, si_num) = res?;
                let ces = match commit_info.data_type() {
                    CommitDataType::MemLog => {
                        CommittedEntrySet::RMemLog(ReadonlyMemLog::load(scnd_path)?)
                    }
                    CommitDataType::SSTable => {
                        CommittedEntrySet::SSTable(SSTable::load(scnd_path)?)
                    }
                };
                scnds.insert(si_num, ces);
            }
        }

        Ok(Self {
            prim,
            scnds,
            dir,
            commit_info,
        })
    }

    pub fn remove_dir(self) -> Result<()> {
        fs::remove_dir_all(&*self.dir)?;
        Ok(())
    }
}
