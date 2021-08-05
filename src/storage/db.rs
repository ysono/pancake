use crate::storage::lsm::LSMTree;
use crate::storage::types::{Datum, OptDatum};
use anyhow::Result;
use std::path::Path;

const PRIMARY_INDEX: &'static str = "primary_index";

pub struct DB {
    primary_index: LSMTree<Datum, OptDatum<Datum>>,
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

    pub fn delete(&mut self, k: Datum) -> Result<()> {
        self.primary_index.put(k, OptDatum::Tombstone)
    }

    pub fn get(&self, k: Datum) -> Result<Option<Datum>> {
        match self.primary_index.get(k)? {
            Some(OptDatum::Some(dat)) => Ok(Some(dat)),
            _ => Ok(None),
        }
    }
}
