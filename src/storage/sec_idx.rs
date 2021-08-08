use crate::storage::lsm::LSMTree;
use crate::storage::serde::{self, ReadItem, Serializable};
use crate::storage::types::{
    Bool, Datum, OptDatum, PrimaryKey, SubValue, SubValueAndKey, SubValueSpec, Value,
};
use crate::storage::utils;
use anyhow::{anyhow, Result};
use std::cmp::Ordering;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};

pub enum PutArg {
    New { new_val: Datum },
    Del { old_val: Datum },
}

pub struct SecondaryIndex {
    path: PathBuf,
    spec: SubValueSpec,
    idx: LSMTree<SubValueAndKey, Bool>,
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
        prim_idx: &LSMTree<PrimaryKey, OptDatum<Value>>,
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
        let mut sec_idx = LSMTree::<SubValueAndKey, Bool>::open(&data_path)?;
        for res_kv in prim_idx.get_range(
            None::<&Box<dyn Fn(&PrimaryKey) -> Ordering>>,
            None::<&Box<dyn Fn(&PrimaryKey) -> Ordering>>,
        )? {
            let (key, opt_val) = res_kv?;
            if let OptDatum::Some(val) = opt_val {
                if let Some(sub_value) = spec.extract(&val) {
                    let sk = SubValueAndKey { sub_value, key };
                    sec_idx.put(sk, Bool(true))?;
                }
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
                    self.idx.put(sk, Bool(false))?;
                }
                if let Some(new_subval) = new {
                    let sk = SubValueAndKey {
                        sub_value: new_subval,
                        key: k.clone(),
                    };
                    self.idx.put(sk, Bool(true))?;
                }
            }
        }

        Ok(())
    }

    pub fn get_range(
        &self,
        subval: &SubValue,
        pk_lo: Option<&PrimaryKey>,
        pk_hi: Option<&PrimaryKey>,
    ) -> Result<Vec<PrimaryKey>> {
        let k_lo_cmp = |sample_sk: &SubValueAndKey| {
            match sample_sk.sub_value.cmp(subval) {
                Ordering::Equal => {
                    if let Some(pk_lo) = pk_lo {
                        sample_sk.key.cmp(pk_lo)
                    } else {
                        // When subval is equal, if primary key bound is not given,
                        // then treat any sample primary key as "greater than" the desired low boundary.
                        Ordering::Greater
                    }
                }
                ord => ord,
            }
        };

        let k_hi_cmp = |sample_sk: &SubValueAndKey| {
            match sample_sk.sub_value.cmp(subval) {
                Ordering::Equal => {
                    if let Some(pk_hi) = pk_hi {
                        sample_sk.key.cmp(pk_hi)
                    } else {
                        // When subval is equal, if primary key bound is not given,
                        // then treat any sample primary key as "less than" the desired high boundary.
                        Ordering::Less
                    }
                }
                ord => ord,
            }
        };

        let out = self
            .idx
            .get_range(Some(&k_lo_cmp), Some(&k_hi_cmp))?
            .filter_map(|res_kv| match res_kv {
                Err(e) => Some(Err(e)),
                Ok((sk, is_alive)) => match is_alive {
                    Bool(false) => None,
                    Bool(true) => Some(Ok(sk.key)),
                },
            })
            .collect::<Result<Vec<_>>>();
        out
    }
}
