use crate::storage::engine_ssi::entryset::{CommittedEntrySetInfo, ReadonlyMemLog, SSTable};
use crate::storage::engines_common::Entry;
use crate::storage::serde::{Deser, OptDatum};
use anyhow::{anyhow, Result};
use std::borrow::Borrow;
use std::iter;

pub enum CommittedEntrySet<K, V> {
    RMemLog(ReadonlyMemLog<K, V>),
    SSTable(SSTable<K, V>),
}

impl<K, V> CommittedEntrySet<K, V>
where
    K: Deser + Ord,
    OptDatum<V>: Deser,
{
    pub fn load(info: CommittedEntrySetInfo) -> Result<Self> {
        let ml_path = info.entryset_dir.memlog_file_path();
        if ml_path.exists() {
            let ml = ReadonlyMemLog::load(info)?;
            return Ok(Self::RMemLog(ml));
        }

        let sst_path = info.entryset_dir.sstable_file_path();
        if sst_path.exists() {
            let sst = SSTable::load(info)?;
            return Ok(Self::SSTable(sst));
        }

        return Err(anyhow!(
            "Entryset dir {:?} does not contain any key-value data file.",
            *info.entryset_dir
        ));
    }

    pub fn get_one<'a, Q>(&'a self, k: &'a Q) -> Option<Entry<'a, K, OptDatum<V>>>
    where
        K: Borrow<Q> + PartialOrd<Q>,
        Q: Ord,
    {
        match self {
            Self::RMemLog(r_memlog) => r_memlog.memtable.get_key_value(k).map(Entry::Ref),
            Self::SSTable(sstable) => sstable.get_one(k).map(Entry::Own),
        }
    }

    pub fn get_range<'a, Q>(
        &'a self,
        k_lo: Option<&'a Q>,
        k_hi: Option<&'a Q>,
    ) -> impl Iterator<Item = Entry<'a, K, OptDatum<V>>>
    where
        K: PartialOrd<Q>,
    {
        let mut rml_iter = None;
        let mut sst_iter = None;
        match self {
            Self::RMemLog(r_memlog) => {
                let iter = r_memlog.memtable.get_range(k_lo, k_hi).map(Entry::Ref);
                rml_iter = Some(iter);
            }
            Self::SSTable(sstable) => {
                let iter = sstable.get_range(k_lo, k_hi).map(Entry::Own);
                sst_iter = Some(iter);
            }
        }

        let ret_iter_fn = move || -> Option<Entry<K, OptDatum<V>>> {
            if let Some(rml_iter) = rml_iter.as_mut() {
                rml_iter.next()
            } else if let Some(sst_iter) = sst_iter.as_mut() {
                sst_iter.next()
            } else {
                None
            }
        };
        iter::from_fn(ret_iter_fn)
    }

    pub fn get_whole_range<'a>(&'a self) -> impl Iterator<Item = Entry<'a, K, OptDatum<V>>> {
        self.get_range(None, None)
    }

    pub fn info(&self) -> &CommittedEntrySetInfo {
        match self {
            Self::RMemLog(memlog) => &memlog.entryset_info,
            Self::SSTable(sstable) => &sstable.entryset_info,
        }
    }
}
