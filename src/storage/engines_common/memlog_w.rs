use crate::storage::engines_common::ReadonlyMemLog;
use crate::storage::serde::{DatumWriter, OptDatum, Ser, Serializable};
use anyhow::Result;
use shorthand::ShortHand;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;

/// A MemLog is a sorted dictionary (called Memtable), backed up by a write-ahead log file.
#[derive(ShortHand)]
#[shorthand(disable(get))]
pub struct WritableMemLog<K, V> {
    #[shorthand(enable(get))]
    r_memlog: ReadonlyMemLog<K, V>,
    log_writer: DatumWriter<File>,
}

impl<K, V> WritableMemLog<K, V>
where
    K: Serializable + Ord,
    OptDatum<V>: Serializable,
{
    pub fn load_or_new<P: AsRef<Path>>(log_path: P) -> Result<Self> {
        let r_memlog = ReadonlyMemLog::load(&log_path)?;

        let log_file = OpenOptions::new()
            .create(true)
            .append(true) // *Not* write(true)
            .open(&log_path)?;
        let log_writer = DatumWriter::from(BufWriter::new(log_file));

        Ok(Self {
            r_memlog,
            log_writer,
        })
    }

    pub fn put(&mut self, k: K, v: OptDatum<V>) -> Result<()> {
        k.ser(&mut self.log_writer)?;
        v.ser(&mut self.log_writer)?;
        self.log_writer.flush()?;

        self.r_memlog.memtable.insert(k, v);

        Ok(())
    }

    pub fn clear(&mut self) -> Result<()> {
        self.r_memlog.memtable.clear();
        self.log_writer.flush()?;
        let log_file = OpenOptions::new()
            .write(true)
            .open(&self.r_memlog.log_path)?;
        log_file.set_len(0)?;
        self.log_writer = DatumWriter::from(BufWriter::new(log_file));
        Ok(())
    }
}

impl<K, V> Into<ReadonlyMemLog<K, V>> for WritableMemLog<K, V> {
    fn into(self) -> ReadonlyMemLog<K, V> {
        self.r_memlog
    }
}
