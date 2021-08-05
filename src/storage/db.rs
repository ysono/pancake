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

    pub fn get(&self, k: PrimaryKey) -> Result<Option<Value>> {
        match self.primary_index.get(k)? {
            Some(OptDatum::Some(dat)) => Ok(Some(dat)),
            _ => Ok(None),
        }
    }
}
