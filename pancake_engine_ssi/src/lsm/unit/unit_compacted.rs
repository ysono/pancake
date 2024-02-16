use crate::{db_state::ScndIdxNum, lsm::unit::UnitDir};
use anyhow::{anyhow, Result};
use pancake_engine_common::{fs_utils, SSTable};
use pancake_types::{
    serde::OptDatum,
    types::{PKShared, PVShared, SVPKShared},
};
use std::any;
use std::collections::HashMap;

pub struct CompactedUnit {
    pub prim: Option<SSTable<PKShared, OptDatum<PVShared>>>,
    pub scnds: HashMap<ScndIdxNum, SSTable<SVPKShared, OptDatum<PVShared>>>,
    pub dir: UnitDir,
}

/// Note, this type does not offer any API to remove its dir.
/// Any logic that writes to a [`CompactedUnit`] should instantiate it lazily,
/// iff there are data to write.
impl CompactedUnit {
    pub fn new_empty(dir: UnitDir) -> Result<Self> {
        let dir_path = dir.path();
        if dir_path.exists() {
            return Err(anyhow!(
                "New {} cannot be created at an existing dir {:?}",
                any::type_name::<Self>(),
                dir_path
            ));
        }
        fs_utils::create_dir_all(dir_path)?;

        Ok(Self {
            prim: None,
            scnds: HashMap::new(),
            dir,
        })
    }
}
