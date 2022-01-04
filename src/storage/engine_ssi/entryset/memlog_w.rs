use crate::storage::engine_ssi::entryset::{EntrySetDir, Memtable};
use crate::storage::serde::{DatumWriter, OptDatum, Ser};
use anyhow::Result;
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};

/// A MemLog is a sorted dictionary (called Memtable), backed up by a log file.
pub struct WritableMemLog<K, V> {
    pub memtable: Memtable<K, V>,
    pub log_writer: DatumWriter<File>,
    pub entryset_dir: EntrySetDir,
}

impl<K, V> WritableMemLog<K, V>
where
    K: Ser + Ord,
    OptDatum<V>: Ser,
{
    pub fn new(entryset_dir: EntrySetDir) -> Result<Self> {
        fs::create_dir(&*entryset_dir)?;
        let log_path = entryset_dir.memlog_file_path();

        let log_file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&log_path)?;
        let log_writer = DatumWriter::from(BufWriter::new(log_file));

        Ok(Self {
            memtable: Memtable::from(BTreeMap::default()),
            log_writer,

            entryset_dir,
        })
    }

    pub fn clear(&mut self) -> Result<()> {
        self.memtable.clear();
        self.log_writer.flush()?;
        let log_path = self.entryset_dir.memlog_file_path();
        let log_file = OpenOptions::new().write(true).open(&log_path)?;
        log_file.set_len(0)?;
        self.log_writer = DatumWriter::from(BufWriter::new(log_file));
        Ok(())
    }

    pub fn put(&mut self, k: K, v: OptDatum<V>) -> Result<()> {
        k.ser(&mut self.log_writer)?;
        v.ser(&mut self.log_writer)?;

        self.memtable.insert(k, v);

        Ok(())
    }

    pub fn remove_entryset_dir(self) -> Result<()> {
        fs::remove_dir_all(&*self.entryset_dir)?;
        Ok(())
    }
}
