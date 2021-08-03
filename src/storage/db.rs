use crate::storage::api::{Datum, OptDatum};
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

    pub fn put(&mut self, k: Datum, v: Datum) -> Result<()> {
        self.primary_index.put(k, OptDatum::Some(v))
    }

    pub fn get(&self, k: Datum) -> Result<Option<Datum>> {
        self.primary_index.get(k)
    }

    pub fn delete(&mut self, k: Datum) -> Result<()> {
        self.primary_index.put(k, OptDatum::Tombstone)
    }
}
