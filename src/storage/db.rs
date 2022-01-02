use crate::storage::lsm::LSMTree;
use crate::storage::scnd_idx::SecondaryIndex;
use crate::storage::types::{PKShared, PVShared, PrimaryKey, SubValue, SubValueSpec};
use crate::storage::utils;
use anyhow::{anyhow, Result};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const PRIM_LSM_DIRNAME: &'static str = "prim_lsm";
const ALL_SCND_IDXS_DIRNAME: &'static str = "scnd_idxs";

pub struct DB {
    prim_lsm: LSMTree<PKShared, PVShared>,
    all_scnd_idxs_dir: PathBuf,
    scnd_idxs: Vec<SecondaryIndex>,
}

impl DB {
    pub fn load_or_new<P: AsRef<Path>>(path: P) -> Result<DB> {
        let prim_lsm = LSMTree::load_or_new(path.as_ref().join(PRIM_LSM_DIRNAME))?;

        let all_scnd_idxs_dir = path.as_ref().join(ALL_SCND_IDXS_DIRNAME);
        fs::create_dir_all(&all_scnd_idxs_dir)?;

        let scnd_idxs = utils::read_dir(&all_scnd_idxs_dir)?
            .into_iter()
            .map(SecondaryIndex::load)
            .collect::<Result<Vec<_>>>()?;

        let db = DB {
            prim_lsm,
            all_scnd_idxs_dir,
            scnd_idxs,
        };
        Ok(db)
    }

    pub fn put(&mut self, pk: PKShared, pv: Option<PVShared>) -> Result<()> {
        let old_pv = self.get_pk_one(&pk)?;

        for scnd_idx in self.scnd_idxs.iter_mut() {
            scnd_idx.put(pk.clone(), old_pv.as_ref(), pv.as_ref())?;
        }

        self.prim_lsm.put(pk, pv)?;

        Ok(())
    }

    pub fn get_pk_one(&self, pk: &PrimaryKey) -> Result<Option<PVShared>> {
        self.prim_lsm.get(pk)
    }

    pub fn get_pk_range<'a>(
        &'a self,
        pk_lo: Option<&'a PrimaryKey>,
        pk_hi: Option<&'a PrimaryKey>,
    ) -> Result<impl 'a + Iterator<Item = Result<(PKShared, PVShared)>>> {
        self.prim_lsm.get_range(pk_lo, pk_hi)
    }

    pub fn get_sv_range<'a>(
        &'a self,
        spec: &'a SubValueSpec,
        sv_lo: Option<&'a SubValue>,
        sv_hi: Option<&'a SubValue>,
    ) -> Result<impl 'a + Iterator<Item = Result<(PKShared, PVShared)>>> {
        for scnd_idx in self.scnd_idxs.iter() {
            if scnd_idx.spec().as_ref() == spec {
                return scnd_idx.get_range(sv_lo, sv_hi);
            }
        }
        Err(anyhow!("Secondary index does not exist for {:?}", spec))
    }

    pub fn create_scnd_idx(&mut self, spec: Arc<SubValueSpec>) -> Result<()> {
        for scnd_idx in self.scnd_idxs.iter() {
            if scnd_idx.spec() == &spec {
                return Ok(());
            }
        }

        let scnd_idx = SecondaryIndex::new(&self.all_scnd_idxs_dir, spec, &self.prim_lsm)?;

        self.scnd_idxs.push(scnd_idx);

        Ok(())
    }

    pub fn delete_scnd_idx(&mut self, spec: &SubValueSpec) -> Result<()> {
        let mut del_idx = None::<usize>;
        for (i, scnd_idx) in self.scnd_idxs.iter().enumerate() {
            if scnd_idx.spec().as_ref() == spec {
                del_idx = Some(i);
                break;
            }
        }

        if let Some(del_idx) = del_idx {
            let scnd_idx = {
                // This is O(n). We could use a HashMap instead.
                self.scnd_idxs.remove(del_idx)
            };
            scnd_idx.remove_files()?;
        }

        Ok(())
    }
}
