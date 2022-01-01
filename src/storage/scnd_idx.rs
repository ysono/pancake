//! A secondary index is an abstraction that maps `{ sub-portion of value : [ primary key ] }`.
//!
//! Internally it uses a LSMTree index that maps `{ (sub-portion of value, primary key) : existence of key }`.

use crate::storage::lsm::LSMTree;
use crate::storage::serde::{self, ReadItem, Serializable};
use crate::storage::types::{Empty, PrimaryKey, SubValue, SubValueAndKey, SubValueSpec, Value};
use crate::storage::utils;
use anyhow::{anyhow, Result};
use std::cmp::Ordering;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};

/// Each instance of [`SecondaryIndex`] is defined by a [`SubValueSpec`].
///
/// A [`SubValueSpec`] specifies how to extract a [`SubValue`] out of a [`Value`].
///
/// Any [`Value`] from which a [`SubValue`] can be extracted is covered by this [`SecondaryIndex`].
///
/// Lookup within a [`SecondaryIndex`] is by [`SubValue`] and returns a list of [`PrimaryKey`].
pub struct SecondaryIndex {
    dir_path: PathBuf,
    spec: SubValueSpec,
    lsm: LSMTree<SubValueAndKey, Empty>,
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
        let spec = match serde::read_item::<SubValueSpec>(&mut spec_file)? {
            ReadItem::EOF => return Err(anyhow!("Unexpected EOF while reading a SubValueSpec")),
            ReadItem::Some { read_size: _, obj } => obj,
        };

        let lsm = LSMTree::load_or_new(&lsm_dir_path)?;

        Ok(Self {
            dir_path: scnd_idx_dir_path.as_ref().into(),
            spec,
            lsm,
        })
    }

    pub fn new<P: AsRef<Path>>(
        all_scnd_idxs_dir_path: P,
        spec: SubValueSpec,
        prim_lsm: &LSMTree<PrimaryKey, Value>,
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

        let mut scnd_lsm = LSMTree::<SubValueAndKey, Empty>::load_or_new(&lsm_dir_path)?;
        for (pk, pv) in prim_lsm.get_range(
            None::<&Box<dyn Fn(&PrimaryKey) -> Ordering>>,
            None::<&Box<dyn Fn(&PrimaryKey) -> Ordering>>,
        )? {
            if let Some(sv) = spec.extract(&pv) {
                let svpk = SubValueAndKey { sv, pk };
                scnd_lsm.put(svpk, Empty {})?;
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

    pub fn spec(&self) -> &SubValueSpec {
        &self.spec
    }

    pub fn put(
        &mut self,
        pk: &PrimaryKey,
        old_pv: Option<&Value>,
        new_pv: Option<&Value>,
    ) -> Result<()> {
        let old_sv = old_pv.map(|old_pv| self.spec.extract(old_pv)).flatten();
        let new_sv = new_pv.map(|new_pv| self.spec.extract(new_pv)).flatten();

        match (old_sv, new_sv) {
            (None, None) => (),
            (Some(old_sv), Some(new_sv)) if old_sv == new_sv => (),
            (old, new) => {
                if let Some(old_sv) = old {
                    let svpk = SubValueAndKey {
                        sv: old_sv,
                        pk: pk.clone(),
                    };
                    self.lsm.del(svpk)?;
                }
                if let Some(new_sv) = new {
                    let svpk = SubValueAndKey {
                        sv: new_sv,
                        pk: pk.clone(),
                    };
                    self.lsm.put(svpk, Empty {})?;
                }
            }
        }

        Ok(())
    }

    pub fn get_range(
        &self,
        sv_lo: Option<&SubValue>,
        sv_hi: Option<&SubValue>,
    ) -> Result<Vec<PrimaryKey>> {
        let svpk_lo_cmp = |sample_svpk: &SubValueAndKey| match sv_lo {
            None => return Ordering::Greater,
            Some(sv_lo) => match sample_svpk.sv.cmp(sv_lo) {
                Ordering::Equal => {
                    // sample_svpk may not be the smallest SubValueAndKey within our bounds,
                    // because there may be another SubValueAndKey with equal sub_value but lesser key.
                    return Ordering::Greater;
                }
                ord => return ord,
            },
        };

        let svpk_hi_cmp = |sample_svpk: &SubValueAndKey| match sv_hi {
            None => return Ordering::Less,
            Some(sv_hi) => match sample_svpk.sv.cmp(sv_hi) {
                Ordering::Equal => return Ordering::Less,
                ord => return ord,
            },
        };

        let kvs = self.lsm.get_range(Some(&svpk_lo_cmp), Some(&svpk_hi_cmp))?;
        let ret = kvs
            .into_iter()
            .map(|(svpk, _v)| svpk.pk)
            .collect::<Vec<_>>();
        Ok(ret)
    }
}
