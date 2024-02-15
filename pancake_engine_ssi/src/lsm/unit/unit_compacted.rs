use crate::{db_state::ScndIdxNum, lsm::unit::UnitDir};
use anyhow::Result;
use pancake_engine_common::{fs_utils, SSTable};
use pancake_types::{
    serde::OptDatum,
    types::{PKShared, PVShared, SVPKShared},
};
use std::collections::HashMap;

pub struct CompactedUnit {
    pub prim: Option<SSTable<PKShared, OptDatum<PVShared>>>,
    pub scnds: HashMap<ScndIdxNum, SSTable<SVPKShared, OptDatum<PVShared>>>,
    pub dir: UnitDir,
}

impl CompactedUnit {
    pub fn new_empty(dir: UnitDir) -> Result<Self> {
        fs_utils::create_dir_all(dir.path())?;
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
        fs_utils::remove_dir_all(self.dir.path())?;
        Ok(())
    }
}
