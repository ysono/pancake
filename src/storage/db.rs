use crate::storage::lsm::LSMTree;
use crate::storage::sec_idx::SecondaryIndex;
use crate::storage::types::{PrimaryKey, SubValue, SubValueSpec, Value};
use crate::storage::utils;
use anyhow::Result;
use std::fs;
use std::path::{Path, PathBuf};

const PRIMARY_INDEX: &'static str = "primary_index";
const SECONDARY_INDEXES: &'static str = "secondary_indexes";

pub struct DB {
    primary_index: LSMTree<PrimaryKey, Value>,
    all_secidxs_dir: PathBuf,
    secondary_indexes: Vec<SecondaryIndex>,
}

impl DB {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<DB> {
        let primary_index = LSMTree::open(path.as_ref().join(PRIMARY_INDEX))?;

        let all_secidxs_dir = path.as_ref().join(SECONDARY_INDEXES);
        fs::create_dir_all(&all_secidxs_dir)?;

        let secondary_indexes = utils::read_dir(&all_secidxs_dir)?
            .into_iter()
            .map(SecondaryIndex::open)
            .collect::<Result<Vec<_>>>()?;

        let db = DB {
            primary_index,
            all_secidxs_dir,
            secondary_indexes,
        };
        Ok(db)
    }

    pub fn put(&mut self, k: PrimaryKey, v: Value) -> Result<()> {
        let old_v = self.get(&k)?;

        for secidx in self.secondary_indexes.iter_mut() {
            secidx.put(&k, old_v.as_ref(), Some(&v))?;
        }

        self.primary_index.put(k, v)?;

        Ok(())
    }

    pub fn delete(&mut self, k: PrimaryKey) -> Result<()> {
        let old_v = self.get(&k)?;

        for secidx in self.secondary_indexes.iter_mut() {
            secidx.put(&k, old_v.as_ref(), None)?;
        }

        self.primary_index.del(k)?;

        Ok(())
    }

    pub fn get(&self, k: &PrimaryKey) -> Result<Option<Value>> {
        self.primary_index.get(k)
    }

    pub fn get_range(
        &self,
        k_lo: Option<&PrimaryKey>,
        k_hi: Option<&PrimaryKey>,
    ) -> Result<Vec<(PrimaryKey, Value)>> {
        // The `move` keyword here moves `k_lo: &PrimaryKey` out of the callback for `.map()`
        // into the following closure.
        let k_lo_cmp = k_lo.map(|k_lo| move |sample_k: &PrimaryKey| sample_k.cmp(k_lo));
        let k_hi_cmp = k_hi.map(|k_hi| move |sample_k: &PrimaryKey| sample_k.cmp(k_hi));

        self.primary_index
            .get_range(k_lo_cmp.as_ref(), k_hi_cmp.as_ref())
    }

    pub fn get_by_sub_value(
        &self,
        spec: &SubValueSpec,
        subval_lo: Option<&SubValue>,
        subval_hi: Option<&SubValue>,
    ) -> Result<Vec<(PrimaryKey, Value)>> {
        for secidx in self.secondary_indexes.iter() {
            if secidx.spec() == spec {
                let keys = secidx.get_range(subval_lo, subval_hi)?;
                let mut kvs = vec![];
                for k in keys.into_iter() {
                    let v = self.get(&k)?;
                    if let Some(v) = v {
                        kvs.push((k, v));
                    }
                }
                return Ok(kvs);
            }
        }
        Ok(vec![])
    }

    pub fn create_sec_idx(&mut self, spec: SubValueSpec) -> Result<()> {
        for secidx in self.secondary_indexes.iter() {
            if secidx.spec() == &spec {
                return Ok(());
            }
        }

        let secidx = SecondaryIndex::new(&self.all_secidxs_dir, spec, &self.primary_index)?;

        self.secondary_indexes.push(secidx);

        Ok(())
    }

    pub fn delete_sec_idx(&mut self, spec: &SubValueSpec) -> Result<()> {
        let mut del_idx = None::<usize>;
        for (i, secidx) in self.secondary_indexes.iter().enumerate() {
            if secidx.spec() == spec {
                del_idx = Some(i);
                break;
            }
        }

        if let Some(del_idx) = del_idx {
            let secidx = {
                // This is O(n). We could use a HashMap instead.
                self.secondary_indexes.remove(del_idx)
            };
            secidx.remove_files()?;
        }

        Ok(())
    }
}
