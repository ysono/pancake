use crate::storage::api::{Key, Value};
use crate::storage::lsm::LSMTree;
use anyhow::Result;
use std::path::Path;

const PRIMARY_INDEX: &'static str = "primary_index";

pub struct DB {
    primary_index: LSMTree,
}

impl DB {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<DB> {
        let primary_index = LSMTree::open(path.as_ref().join(PRIMARY_INDEX))?;

        let db = DB { primary_index };

        Ok(db)
    }

    pub fn put(&mut self, k: Key, v: Value) -> Result<()> {
        self.primary_index.put(k, v)
    }

    pub fn get(&self, k: Key) -> Result<Value> {
        self.primary_index.get(k)
    }
}
