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
    path: PathBuf,
    spec: SubValueSpec,
    idx: LSMTree<SubValueAndKey, Empty>,
}

impl SecondaryIndex {
    fn spec_path<P: AsRef<Path>>(secidx_path: P) -> PathBuf {
        secidx_path.as_ref().join("props").join("spec.data")
    }

    fn data_path<P: AsRef<Path>>(secidx_path: P) -> PathBuf {
        secidx_path.as_ref().join("data")
    }

    pub fn open<P: AsRef<Path>>(secidx_path: P) -> Result<Self> {
        let spec_path = Self::spec_path(&secidx_path);
        let mut spec_file = File::open(&spec_path)?;
        let spec = match serde::read_item::<SubValueSpec>(&mut spec_file)? {
            ReadItem::EOF => return Err(anyhow!("Unexpected EOF while reading a SubValueSpec")),
            ReadItem::Some { read_size: _, obj } => obj,
        };

        let data_path = Self::data_path(&secidx_path);
        let idx = LSMTree::open(&data_path)?;

        let secidx = Self {
            path: secidx_path.as_ref().into(),
            spec,
            idx,
        };
        Ok(secidx)
    }

    pub fn new<P: AsRef<Path>>(
        all_secidxs_path: P,
        spec: SubValueSpec,
        prim_idx: &LSMTree<PrimaryKey, Value>,
    ) -> Result<Self> {
        let secidx_path = utils::new_timestamped_path(&all_secidxs_path, "");

        let spec_path = Self::spec_path(&secidx_path);
        if let Some(parent_path) = spec_path.parent() {
            fs::create_dir_all(parent_path)?;
        }
        let mut spec_file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&spec_path)?;
        spec.ser(&mut spec_file)?;

        let data_path = Self::data_path(&secidx_path);
        fs::create_dir_all(&data_path)?;
        let mut sec_idx = LSMTree::<SubValueAndKey, Empty>::open(&data_path)?;
        for (key, val) in prim_idx.get_range(
            None::<&Box<dyn Fn(&PrimaryKey) -> Ordering>>,
            None::<&Box<dyn Fn(&PrimaryKey) -> Ordering>>,
        )? {
            if let Some(sub_value) = spec.extract(&val) {
                let sk = SubValueAndKey { sub_value, key };
                sec_idx.put(sk, Empty {})?;
            }
        }

        let obj = Self {
            path: secidx_path,
            spec,
            idx: sec_idx,
        };
        Ok(obj)
    }

    pub fn remove_files(&self) -> Result<()> {
        fs::remove_dir_all(&self.path)?;
        Ok(())
    }

    pub fn spec(&self) -> &SubValueSpec {
        &self.spec
    }

    pub fn put(
        &mut self,
        k: &PrimaryKey,
        old_v: Option<&Value>,
        new_v: Option<&Value>,
    ) -> Result<()> {
        let old_subval = old_v.map(|old_v| self.spec.extract(old_v)).flatten();
        let new_subval = new_v.map(|new_v| self.spec.extract(new_v)).flatten();

        match (old_subval, new_subval) {
            (None, None) => (),
            (Some(old_subval), Some(new_subval)) if old_subval == new_subval => (),
            (old, new) => {
                if let Some(old_subval) = old {
                    let sk = SubValueAndKey {
                        sub_value: old_subval,
                        key: k.clone(),
                    };
                    self.idx.del(sk)?;
                }
                if let Some(new_subval) = new {
                    let sk = SubValueAndKey {
                        sub_value: new_subval,
                        key: k.clone(),
                    };
                    self.idx.put(sk, Empty {})?;
                }
            }
        }

        Ok(())
    }

    pub fn get_range(
        &self,
        subval_lo: Option<&SubValue>,
        subval_hi: Option<&SubValue>,
    ) -> Result<Vec<PrimaryKey>> {
        let sk_lo_cmp = |sample_sk: &SubValueAndKey| match subval_lo {
            None => return Ordering::Greater,
            Some(subval_lo) => match sample_sk.sub_value.cmp(subval_lo) {
                Ordering::Equal => {
                    // sample_sk may not be the smallest SubValueAndKey within our bounds,
                    // because there may be another SubValueAndKey with equal sub_value but lesser key.
                    return Ordering::Greater;
                }
                ord => return ord,
            },
        };

        let sk_hi_cmp = |sample_sk: &SubValueAndKey| match subval_hi {
            None => return Ordering::Less,
            Some(subval_hi) => match sample_sk.sub_value.cmp(subval_hi) {
                Ordering::Equal => return Ordering::Less,
                ord => return ord,
            },
        };

        let kvs = self.idx.get_range(Some(&sk_lo_cmp), Some(&sk_hi_cmp))?;
        let ret = kvs.into_iter().map(|(sk, _v)| sk.key).collect::<Vec<_>>();
        Ok(ret)
    }
}
