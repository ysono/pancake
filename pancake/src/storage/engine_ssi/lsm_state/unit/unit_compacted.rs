use crate::storage::engine_ssi::{
    db_state::ScndIdxNum,
    lsm_state::{entryset::CommittedEntrySet, unit::UnitDir},
};
use crate::storage::types::{PKShared, PVShared, SVPKShared};
use anyhow::Result;
use std::collections::HashMap;
use std::fs;

pub struct CompactedUnit {
    pub prim: Option<CommittedEntrySet<PKShared, PVShared>>,
    pub scnds: HashMap<ScndIdxNum, CommittedEntrySet<SVPKShared, PVShared>>,
    pub dir: UnitDir,
}

impl CompactedUnit {
    pub fn new_empty(dir: UnitDir) -> Result<Self> {
        fs::create_dir_all(&*dir)?;
        Ok(Self {
            prim: None,
            scnds: HashMap::new(),
            dir,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.prim.is_none() && self.scnds.is_empty()
    }

    pub fn remove_dir(self) -> Result<()> {
        fs::remove_dir_all(&*self.dir)?;
        Ok(())
    }
}
