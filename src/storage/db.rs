use crate::storage::lsm::LSMTree;
use crate::storage::scnd_idx::SecondaryIndex;
use crate::storage::types::{PrimaryKey, SubValue, SubValueSpec, Value};
use crate::storage::utils;
use anyhow::Result;
use std::fs;
use std::path::{Path, PathBuf};

const PRIM_LSM_DIRNAME: &'static str = "prim_lsm";
const ALL_SCND_IDXS_DIRNAME: &'static str = "scnd_idxs";

pub struct DB {
    prim_lsm: LSMTree<PrimaryKey, Value>,
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

    pub fn put(&mut self, pk: PrimaryKey, pv: Value) -> Result<()> {
        let old_pv = self.get_pk_one(&pk)?;

        for scnd_idx in self.scnd_idxs.iter_mut() {
            scnd_idx.put(&pk, old_pv.as_ref(), Some(&pv))?;
        }

        self.prim_lsm.put(pk, pv)?;

        Ok(())
    }

    pub fn delete(&mut self, pk: PrimaryKey) -> Result<()> {
        let old_pv = self.get_pk_one(&pk)?;

        for scnd_idx in self.scnd_idxs.iter_mut() {
            scnd_idx.put(&pk, old_pv.as_ref(), None)?;
        }

        self.prim_lsm.del(pk)?;

        Ok(())
    }

    pub fn get_pk_one(&self, pk: &PrimaryKey) -> Result<Option<Value>> {
        self.prim_lsm.get(pk)
    }

    pub fn get_pk_range(
        &self,
        pk_lo: Option<&PrimaryKey>,
        pk_hi: Option<&PrimaryKey>,
    ) -> Result<Vec<(PrimaryKey, Value)>> {
        // The `move` keyword here moves `k_lo: &PrimaryKey` out of the callback for `.map()`
        // into the following closure.
        let pk_lo_cmp = pk_lo.map(|pk_lo| move |sample_pk: &PrimaryKey| sample_pk.cmp(pk_lo));
        let pk_hi_cmp = pk_hi.map(|pk_hi| move |sample_pk: &PrimaryKey| sample_pk.cmp(pk_hi));

        self.prim_lsm
            .get_range(pk_lo_cmp.as_ref(), pk_hi_cmp.as_ref())
    }

    pub fn get_sv_range(
        &self,
        spec: &SubValueSpec,
        sv_lo: Option<&SubValue>,
        sv_hi: Option<&SubValue>,
    ) -> Result<Vec<(PrimaryKey, Value)>> {
        for scnd_idx in self.scnd_idxs.iter() {
            if scnd_idx.spec() == spec {
                let keys = scnd_idx.get_range(sv_lo, sv_hi)?;
                let mut kvs = vec![];
                for k in keys.into_iter() {
                    let v = self.get_pk_one(&k)?;
                    if let Some(v) = v {
                        kvs.push((k, v));
                    }
                }
                return Ok(kvs);
            }
        }
        Ok(vec![])
    }

    pub fn create_scnd_idx(&mut self, spec: SubValueSpec) -> Result<()> {
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
            if scnd_idx.spec() == spec {
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
