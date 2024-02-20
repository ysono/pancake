use crate::{lsm::LSMTree, scnd_idx::SecondaryIndex};
use anyhow::{anyhow, Context, Result};
use pancake_engine_common::{
    fs_utils::{self, AntiCollisionParentDir, NamePattern},
    Entry,
};
use pancake_types::types::{PKShared, PVShared, PrimaryKey, SubValue, SubValueSpec};
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

const PRIM_LSM_DIR_NAME: &str = "prim_lsm";
const ALL_SCND_IDXS_PARENT_DIR_NAME: &str = "scnd_idxs";

pub struct DB {
    _lock_dir: File,
    prim_lsm: LSMTree<PKShared, PVShared>,
    scnd_idxs: HashMap<Arc<SubValueSpec>, SecondaryIndex>,
    all_scnd_idxs_parent_dir: AntiCollisionParentDir,
}

impl DB {
    pub fn load_or_new<P: AsRef<Path>>(db_dir_path: P) -> Result<DB> {
        let db_dir_path = db_dir_path.as_ref();

        fs_utils::create_dir_all(db_dir_path)?;
        let lock_dir = fs_utils::lock_file(db_dir_path)?;

        let prim_lsm_dir_path = db_dir_path.join(PRIM_LSM_DIR_NAME);
        let all_scnd_idxs_parent_dir_path = db_dir_path.join(ALL_SCND_IDXS_PARENT_DIR_NAME);

        let prim_lsm = LSMTree::load_or_new(prim_lsm_dir_path)?;

        let mut scnd_idxs = HashMap::new();
        let all_scnd_idxs_parent_dir = AntiCollisionParentDir::load_or_new(
            all_scnd_idxs_parent_dir_path,
            NamePattern::new("", ""),
            |child_path, res_child_num| -> Result<()> {
                res_child_num.with_context(|| format!("The \"all secondary indexes\" dir contains an unexpected child path {child_path:?}"))?;

                let scnd_idx = SecondaryIndex::load(child_path)?;
                let spec = scnd_idx.spec().clone();
                scnd_idxs.insert(spec, scnd_idx);

                Ok(())
            },
        )?;

        Ok(DB {
            _lock_dir: lock_dir,
            prim_lsm,
            scnd_idxs,
            all_scnd_idxs_parent_dir,
        })
    }

    pub fn put(&mut self, pk: PKShared, pv: Option<PVShared>) -> Result<()> {
        let opt_entry = self.prim_lsm.get_one(&pk);
        let opt_res_pkpv = opt_entry.as_ref().map(|entry| entry.try_borrow());
        let opt_pkpv = opt_res_pkpv.transpose()?;
        let old_pv: Option<&PVShared> = opt_pkpv.map(|(_, pv)| pv);

        for (_spec, scnd_idx) in self.scnd_idxs.iter_mut() {
            scnd_idx.put(&pk, old_pv, pv.as_ref())?;
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
        Err(anyhow!("Secondary index does not exist for {spec:?}"))
    }

    pub fn create_scnd_idx(&mut self, spec: Arc<SubValueSpec>) -> Result<()> {
        if self.scnd_idxs.get(&spec).is_some() {
            return Ok(());
        }

        let dir_path = self.all_scnd_idxs_parent_dir.format_new_child_path();
        let scnd_idx = SecondaryIndex::new(dir_path, Arc::clone(&spec), &self.prim_lsm)?;
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
