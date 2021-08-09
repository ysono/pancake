use crate::storage::lsm::LSMTree;
use crate::storage::types::{OptDatum, PrimaryKey, Value};
use anyhow::Result;
use std::path::Path;

const PRIMARY_INDEX: &'static str = "primary_index";

pub struct DB {
    primary_index: LSMTree<PrimaryKey, OptDatum<Value>>,
}

impl DB {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<DB> {
        let primary_index = LSMTree::open(path.as_ref().join(PRIMARY_INDEX))?;

        let db = DB { primary_index };

        Ok(db)
    }

    pub fn put(&mut self, k: PrimaryKey, v: Value) -> Result<()> {
        self.primary_index.put(k, OptDatum::Some(v))
    }

    pub fn delete(&mut self, k: PrimaryKey) -> Result<()> {
        self.primary_index.put(k, OptDatum::Tombstone)
    }

    pub fn get(&self, k: &PrimaryKey) -> Result<Option<Value>> {
        match self.primary_index.get(k)? {
            Some(OptDatum::Some(dat)) => Ok(Some(dat)),
            _ => Ok(None),
        }
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

        let ret = self
            .primary_index
            .get_range(k_lo_cmp.as_ref(), k_hi_cmp.as_ref())?
            .filter_map(|res_kv| match res_kv {
                Err(e) => Some(Err(e)),
                Ok((k, v)) => match v {
                    OptDatum::Tombstone => None,
                    OptDatum::Some(v) => Some(Ok((k, v))),
                },
            })
            .collect::<Result<Vec<_>>>();
        ret
    }
}
