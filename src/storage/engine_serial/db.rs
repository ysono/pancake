use crate::ds_n_a::persisted_u64::PersistedU64;
use crate::storage::engine_serial::fs_utils::{self, UniqueId};
use crate::storage::engine_serial::lsm::{Entry, LSMTree};
use crate::storage::engine_serial::scnd_idx::SecondaryIndex;
use crate::storage::types::{PKShared, PVShared, PrimaryKey, SubValue, SubValueSpec};
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const PRIM_LSM_DIR_NAME: &str = "prim_lsm";
const ALL_SCND_IDXS_DIR_NAME: &str = "scnd_idxs";
const UNIQUE_ID_FILE_NAME: &str = "unique_id.u64";

pub struct DB {
    prim_lsm: LSMTree<PKShared, PVShared>,
    scnd_idxs: HashMap<Arc<SubValueSpec>, SecondaryIndex>,
    all_scnd_idxs_dir: PathBuf,
    unique_id: PersistedU64<UniqueId>,
}

impl DB {
    pub fn load_or_new<P: AsRef<Path>>(db_dir_path: P) -> Result<DB> {
        let prim_lsm_dir_path = db_dir_path.as_ref().join(PRIM_LSM_DIR_NAME);
        let all_scnd_idxs_dir = db_dir_path.as_ref().join(ALL_SCND_IDXS_DIR_NAME);
        let unique_id_path = db_dir_path.as_ref().join(UNIQUE_ID_FILE_NAME);
        fs::create_dir_all(&all_scnd_idxs_dir)?;

        let prim_lsm = LSMTree::load_or_new(prim_lsm_dir_path)?;

        let scnd_idxs = fs_utils::read_dir(&all_scnd_idxs_dir)?
            .map(|res_path| {
                res_path.and_then(|scnd_idx_dir_path| {
                    let scnd_idx = SecondaryIndex::load(scnd_idx_dir_path)?;
                    let spec = scnd_idx.spec().clone();
                    Ok((spec, scnd_idx))
                })
            })
            .collect::<Result<HashMap<_, _>>>()?;

        let unique_id = PersistedU64::load_or_new(unique_id_path)?;

        let db = DB {
            prim_lsm,
            scnd_idxs,
            all_scnd_idxs_dir,
            unique_id,
        };
        Ok(db)
    }

    pub fn put(&mut self, pk: PKShared, pv: Option<PVShared>) -> Result<()> {
        let opt_entry = self.prim_lsm.get_one(&pk);
        let opt_res_pair = opt_entry.as_ref().map(|entry| entry.try_borrow());
        let opt_pair = opt_res_pair.transpose()?;
        let old_pv: Option<&PVShared> = opt_pair.map(|pair| pair.1);

        for (_spec, scnd_idx) in self.scnd_idxs.iter_mut() {
            scnd_idx.put(pk.clone(), old_pv, pv.as_ref())?;
        }

        self.prim_lsm.put(pk, pv)?;

        Ok(())
    }

    pub fn get_pk_one<'a>(&'a self, pk: &'a PrimaryKey) -> Option<Entry<'a, PKShared, PVShared>> {
        self.prim_lsm.get_one(pk)
    }

    pub fn get_pk_range<'a>(
        &'a self,
        pk_lo: Option<&'a PrimaryKey>,
        pk_hi: Option<&'a PrimaryKey>,
    ) -> impl 'a + Iterator<Item = Entry<'a, PKShared, PVShared>> {
        self.prim_lsm.get_range(pk_lo, pk_hi)
    }

    pub fn get_sv_range<'a>(
        &'a self,
        spec: &'a SubValueSpec,
        sv_lo: Option<&'a SubValue>,
        sv_hi: Option<&'a SubValue>,
    ) -> Result<impl 'a + Iterator<Item = Entry<'a, PKShared, PVShared>>> {
        if let Some(scnd_idx) = self.scnd_idxs.get(spec) {
            let iter = scnd_idx.get_range(sv_lo, sv_hi);
            return Ok(iter);
        }
        Err(anyhow!("Secondary index does not exist for {:?}", spec))
    }

    pub fn create_scnd_idx(&mut self, spec: Arc<SubValueSpec>) -> Result<()> {
        if let Some(_) = self.scnd_idxs.get(&spec) {
            return Ok(());
        }

        let id = self.unique_id.get_and_inc()?;
        let scnd_idx_dir_path = self
            .all_scnd_idxs_dir
            .join(format!("scnd_idx-{}", id.to_shortform_string()));
        let scnd_idx = SecondaryIndex::new(scnd_idx_dir_path, Arc::clone(&spec), &self.prim_lsm)?;
        self.scnd_idxs.insert(spec, scnd_idx);

        Ok(())
    }

    pub fn delete_scnd_idx(&mut self, spec: &SubValueSpec) -> Result<()> {
        let scnd_idx = self.scnd_idxs.remove(spec);
        if let Some(scnd_idx) = scnd_idx {
            scnd_idx.remove_dir()?;
        }

        Ok(())
    }
}
