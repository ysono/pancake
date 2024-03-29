use crate::{fs_utils, ReadonlyMemLog};
use anyhow::Result;
use pancake_types::types::Serializable;
use shorthand::ShortHand;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::mem;
use std::path::Path;

/// A MemLog is a sorted dictionary (called Memtable), backed up by a write-ahead log file.
#[derive(ShortHand)]
#[shorthand(disable(get))]
pub struct WritableMemLog<K, V> {
    #[shorthand(enable(get))]
    r_memlog: ReadonlyMemLog<K, V>,
    log_writer: BufWriter<File>,
}

impl<K, V> WritableMemLog<K, V>
where
    K: Serializable + Ord,
    V: Serializable,
{
    pub fn load_or_new<P: AsRef<Path>>(log_path: P) -> Result<Self> {
        let r_memlog = ReadonlyMemLog::load(&log_path)?;

        let log_file = fs_utils::open_file(
            &log_path,
            OpenOptions::new().create(true).append(true), /* Append. *Not* write. */
        )?;
        let log_writer = BufWriter::new(log_file);

        Ok(Self {
            r_memlog,
            log_writer,
        })
    }

    /// The caller is responsible for [`Self::flush()`]ing subsequently.
    pub fn put(&mut self, k: K, v: V) -> Result<()> {
        k.ser(&mut self.log_writer)?;
        v.ser(&mut self.log_writer)?;

        self.r_memlog.memtable.insert(k, v);

        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.log_writer.flush()?;
        Ok(())
    }

    pub fn clear(&mut self) -> Result<()> {
        self.r_memlog.memtable.clear();

        let log_file =
            fs_utils::open_file(&self.r_memlog.log_path, OpenOptions::new().write(true))?;
        log_file.set_len(0)?;
        let new_writer = BufWriter::new(log_file);

        let old_writer = mem::replace(&mut self.log_writer, new_writer);

        let (_old_file, _old_res_buf) = old_writer.into_parts(); // Drop these without flushing.

        Ok(())
    }
}

impl<K, V> From<WritableMemLog<K, V>> for ReadonlyMemLog<K, V> {
    fn from(w_memlog: WritableMemLog<K, V>) -> ReadonlyMemLog<K, V> {
        w_memlog.r_memlog
    }
}
