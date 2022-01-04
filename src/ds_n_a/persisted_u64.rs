use anyhow::Result;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::ops::DerefMut;
use std::path::PathBuf;

pub struct PersistedU64<Id> {
    file_path: PathBuf,
    curr_val: Id,
}

impl<Id> PersistedU64<Id>
where
    Id: From<u64> + DerefMut<Target = u64> + Copy,
{
    pub fn load_or_new(file_path: PathBuf) -> Result<Self> {
        let curr_val = if file_path.exists() {
            let mut s = String::new();
            File::open(&file_path)?.read_to_string(&mut s)?;
            let u = s.parse::<u64>()?;
            Id::from(u)
        } else {
            Id::from(0)
        };

        Ok(Self {
            file_path,
            curr_val,
        })
    }

    // Intentionally requiring mutual borrow of self, for free compiler checks of
    // mutual exclusion.
    pub fn get_and_inc(&mut self) -> Result<Id> {
        let ret = self.curr_val;

        *self.curr_val += 1;
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&self.file_path)?;
        write!(&mut file, "{}", *self.curr_val)?;

        Ok(ret)
    }
}
