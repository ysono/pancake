use crate::{db_state::ScndIdxNum, lsm_state::unit::UnitDir};
use anyhow::Result;
use pancake_engine_common::WritableMemLog;
use pancake_types::types::{PKShared, PVShared, SVPKShared};
use std::collections::{hash_map, HashMap};
use std::fs;

pub struct StagingUnit {
    pub prim: WritableMemLog<PKShared, PVShared>,
    pub scnds: HashMap<ScndIdxNum, WritableMemLog<SVPKShared, PVShared>>,
    pub dir: UnitDir,
}

impl StagingUnit {
    pub fn new(dir: UnitDir) -> Result<Self> {
        fs::create_dir_all(&*dir)?;
        let prim_path = dir.format_prim_path();
        let prim_memlog = WritableMemLog::load_or_new(prim_path)?;
        Ok(Self {
            prim: prim_memlog,
            scnds: HashMap::default(),
            dir,
        })
    }

    pub fn ensure_create_scnd<'a>(
        &'a mut self,
        si_num: ScndIdxNum,
    ) -> Result<&'a mut WritableMemLog<SVPKShared, PVShared>> {
        match self.scnds.entry(si_num) {
            hash_map::Entry::Occupied(entry) => Ok(entry.into_mut()),
            hash_map::Entry::Vacant(entry) => {
                let file_path = self.dir.format_scnd_path(si_num);
                let w_memlog = WritableMemLog::load_or_new(file_path)?;
                let w_memlog = entry.insert(w_memlog);
                Ok(w_memlog)
            }
        }
    }

    pub fn flush(&mut self) -> Result<()> {
        self.prim.flush()?;
        for (_, scnd) in self.scnds.iter_mut() {
            scnd.flush()?;
        }
        Ok(())
    }

    pub fn clear(&mut self) -> Result<()> {
        self.prim.clear()?;
        for (_, scnd) in self.scnds.iter_mut() {
            scnd.clear()?;
            // We could also remove the seconary WritableMemLog files.
        }
        Ok(())
    }

    pub fn remove_dir(self) -> Result<()> {
        fs::remove_dir_all(&*self.dir)?;
        Ok(())
    }
}