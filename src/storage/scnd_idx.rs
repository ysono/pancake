use crate::storage::lsm::LSMTree;
use crate::storage::types::{PKShared, PVShared, SVPKShared, SubValue, SubValueSpec};
use crate::storage::utils;
use anyhow::Result;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// A secondary index is an abstraction of as sorted dictionary mapping
/// `(sub-portion of value , primary key)` : `value`.
///
/// Clients may query for `(primary key, value)` entries based on bounds
/// over `sub-portion of value` and optionally over `primary key`.
///
/// Each instance of [`SecondaryIndex`] is defined by a [`SubValueSpec`],
/// which specifies what kind of `sub-portion of value` this [`SecondaryIndex`]
/// is responsible for.
pub struct SecondaryIndex {
    dir_path: PathBuf,
    spec: Arc<SubValueSpec>,
    lsm: LSMTree<SVPKShared, PVShared>,
}

impl SecondaryIndex {
    fn spec_file_path<P: AsRef<Path>>(scnd_idx_dir_path: P) -> PathBuf {
        scnd_idx_dir_path.as_ref().join("spec.datum")
    }

    fn lsm_dir_path<P: AsRef<Path>>(scnd_idx_dir_path: P) -> PathBuf {
        scnd_idx_dir_path.as_ref().join("lsm")
    }

    pub fn load<P: AsRef<Path>>(scnd_idx_dir_path: P) -> Result<Self> {
        let spec_file_path = Self::spec_file_path(&scnd_idx_dir_path);
        let lsm_dir_path = Self::lsm_dir_path(&scnd_idx_dir_path);

        let mut spec_file = File::open(&spec_file_path)?;
        let spec = SubValueSpec::deser(&mut spec_file)?;
        let spec = Arc::new(spec);

        let lsm = LSMTree::load_or_new(&lsm_dir_path)?;

        Ok(Self {
            dir_path: scnd_idx_dir_path.as_ref().into(),
            spec,
            lsm,
        })
    }

    pub fn new<P: AsRef<Path>>(
        all_scnd_idxs_dir_path: P,
        spec: Arc<SubValueSpec>,
        prim_lsm: &LSMTree<PKShared, PVShared>,
    ) -> Result<Self> {
        let scnd_idx_dir_path = utils::new_timestamped_path(&all_scnd_idxs_dir_path, "");
        let spec_file_path = Self::spec_file_path(&scnd_idx_dir_path);
        let lsm_dir_path = Self::lsm_dir_path(&scnd_idx_dir_path);
        fs::create_dir_all(&lsm_dir_path)?;

        let mut spec_file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&spec_file_path)?;
        spec.ser(&mut spec_file)?;

        let mut scnd_lsm = LSMTree::load_or_new(&lsm_dir_path)?;
        for (pk, pv) in prim_lsm.get_whole_range()? {
            if let Some(sv) = spec.extract(&pv) {
                let svpk = SVPKShared { sv, pk };
                scnd_lsm.put(svpk, Some(pv))?;
            }
        }

        Ok(Self {
            dir_path: scnd_idx_dir_path,
            spec,
            lsm: scnd_lsm,
        })
    }

    pub fn remove_files(&self) -> Result<()> {
        fs::remove_dir_all(&self.dir_path)?;
        Ok(())
    }

    pub fn spec(&self) -> &Arc<SubValueSpec> {
        &self.spec
    }

    pub fn put(
        &mut self,
        pk: PKShared,
        old_pv: Option<&PVShared>,
        new_pv: Option<&PVShared>,
    ) -> Result<()> {
        let old_sv = old_pv.and_then(|old_pv| self.spec.extract(old_pv));
        let new_sv = new_pv.and_then(|new_pv| self.spec.extract(new_pv));

        if old_sv != new_sv {
            if let Some(old_sv) = old_sv {
                let svpk = SVPKShared {
                    sv: old_sv,
                    pk: pk.clone(),
                };
                self.lsm.put(svpk, None)?;
            }
            if let Some(new_sv) = new_sv {
                let svpk = SVPKShared { sv: new_sv, pk };
                self.lsm.put(svpk, new_pv.cloned())?;
            }
        }

        Ok(())
    }

    pub fn get_range(
        &self,
        sv_lo: Option<&SubValue>,
        sv_hi: Option<&SubValue>,
    ) -> Result<Vec<(PKShared, PVShared)>> {
        let kvs = self.lsm.get_range(sv_lo, sv_hi)?;
        let ret = kvs
            .into_iter()
            .map(|(svpk, pv)| (svpk.pk, pv))
            .collect::<Vec<_>>();
        Ok(ret)
    }
}
