use crate::storage::lsm::sstable::SSTable;
use crate::storage::serde::{self, KeyValueIterator, OptDatum, Serializable};
use crate::storage::utils;
use anyhow::Result;
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::hash::Hash;
use std::mem;
use std::path::{Path, PathBuf};

static COMMIT_LOG_FILE_NAME: &'static str = "commit_log.kv";
static SSTABLES_DIR_NAME: &'static str = "sstables";
static MEMTABLE_FLUSH_SIZE_THRESH: usize = 7;
static SSTABLE_COMPACT_COUNT_THRESH: usize = 4;

pub struct LSMTree<K, V>
where
    V: Serializable + Clone,
{
    path: PathBuf,
    memtable: BTreeMap<K, OptDatum<V>>,
    commit_log: File,
    sstables: Vec<SSTable<K, OptDatum<V>>>,
}

impl<K, V> LSMTree<K, V>
where
    K: Serializable + Ord + Hash + Clone,
    V: Serializable + Clone,
{
    pub fn load_or_new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let cl_path = path.as_ref().join(COMMIT_LOG_FILE_NAME);
        let sstables_dir_path = path.as_ref().join(SSTABLES_DIR_NAME);
        std::fs::create_dir_all(&sstables_dir_path)?;

        let mut memtable = Default::default();
        if cl_path.exists() {
            Self::read_commit_log(&cl_path, &mut memtable)?;
        }

        let commit_log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&cl_path)?;

        let sstables = utils::read_dir_sorted(sstables_dir_path)?
            .into_iter()
            .map(SSTable::read_from_file)
            .collect::<Result<Vec<_>>>()?;

        let ret = Self {
            path: path.as_ref().into(),
            memtable,
            commit_log,
            sstables,
        };
        Ok(ret)
    }

    fn read_commit_log(path: &PathBuf, memtable: &mut BTreeMap<K, OptDatum<V>>) -> Result<()> {
        let file = File::open(path)?;
        let iter = KeyValueIterator::<K, OptDatum<V>>::from(file);
        for file_data in iter {
            let (key, val) = file_data?;
            memtable.insert(key, val);
        }
        Ok(())
    }

    fn flush_memtable(&mut self) -> Result<()> {
        let new_sst = SSTable::write_from_mem(
            &self.memtable,
            utils::new_timestamped_path(self.path.join(SSTABLES_DIR_NAME), "kv"),
        )?;
        self.sstables.push(new_sst);

        self.memtable.clear();
        self.commit_log.set_len(0)?;

        Ok(())
    }

    fn compact_sstables(&mut self) -> Result<()> {
        let new_table_path = utils::new_timestamped_path(self.path.join(SSTABLES_DIR_NAME), "kv");
        let new_table = SSTable::compact(new_table_path, &self.sstables)?;
        let new_tables = vec![new_table];

        // In async version, we will have to assume that new sstables may have been created while we were compacting, so we won't be able to just swap.
        let old_tables = mem::replace(&mut self.sstables, new_tables);
        for table in old_tables {
            table.remove_file()?;
        }

        Ok(())
    }

    fn check_start_job(&mut self) -> Result<()> {
        if self.memtable.len() >= MEMTABLE_FLUSH_SIZE_THRESH {
            self.flush_memtable()?;
        }
        if self.sstables.len() >= SSTABLE_COMPACT_COUNT_THRESH {
            self.compact_sstables()?;
        }
        Ok(())
    }

    pub fn put(&mut self, k: K, v: Option<V>) -> Result<()> {
        let v = OptDatum::from(v);

        serde::serialize_kv(&k, &v, &mut self.commit_log)?;

        self.memtable.insert(k, v);

        self.check_start_job()?;

        Ok(())
    }

    pub fn get<Q>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q> + PartialOrd<Q>,
        Q: Ord,
    {
        let do_get = || {
            if let Some(v) = self.memtable.get(k) {
                return Ok(Some(v.clone()));
            }
            // TODO bloom filter here
            for ss in self.sstables.iter().rev() {
                let v = ss.get(k)?;
                if v.is_some() {
                    return Ok(v);
                }
            }
            Ok(None)
        };
        let res: Result<Option<OptDatum<V>>> = do_get();
        res.map(|opt_optdat| opt_optdat.and_then(|optdat| optdat.into()))
    }

    pub fn get_range<'a, Q>(
        &'a self,
        k_lo: Option<&'a Q>,
        k_hi: Option<&'a Q>,
    ) -> Result<impl 'a + Iterator<Item = Result<(K, V)>>>
    where
        K: PartialOrd<Q>,
    {
        let ssts_iter = SSTable::merge_range(&self.sstables, k_lo, k_hi)?;

        let mut mt_iter = self.memtable.iter();

        if let Some(k_lo) = k_lo {
            // Find the max key less than the desired key. Not equal to it, b/c
            // `.nth()` takes the item at the provided position.
            if let Some(iter_pos) = self.memtable.iter().rposition(|(sample_k, _v)| {
                sample_k
                    .partial_cmp(k_lo)
                    .unwrap_or(Ordering::Greater)
                    .is_lt()
            }) {
                mt_iter.nth(iter_pos);
            }
        }

        let mt_iter = mt_iter.take_while(move |(sample_k, _v)| {
            if let Some(k_hi) = k_hi {
                sample_k
                    .partial_cmp(&k_hi)
                    .unwrap_or(Ordering::Less)
                    .is_le()
            } else {
                true
            }
        });

        let mut ssts_iter = ssts_iter.peekable();
        let mut mt_iter = mt_iter.peekable();

        /*
        Here we're doing k-merge between (the iterator of all sstables) and (the iterator of all memtables).
        We have to do this manually due to type difference.
        An sstable iterator yields Item = Result<(K, V)>.
        A memtable iterator yields Item = (&K, &V).
        */
        let out_iter_fn = move || -> Option<Result<(K, OptDatum<V>)>> {
            let next_is_sst = match (ssts_iter.peek(), mt_iter.peek()) {
                (None, None) => return None,
                (None, Some(_)) => false,
                (Some(_), None) => true,
                (Some(Err(_)), _) => true,
                (Some(Ok((sst_k, _sst_v))), Some((mt_k, _mt_v))) => {
                    if &sst_k < mt_k {
                        true
                    } else if &sst_k > mt_k {
                        false
                    } else {
                        ssts_iter.next();
                        false
                    }
                }
            };

            if next_is_sst {
                return ssts_iter.next();
            } else {
                let (k, v) = mt_iter.next().unwrap();
                let kv = (k.clone(), v.clone());
                return Some(Ok(kv));
            }
        };
        let out_iter = std::iter::from_fn(out_iter_fn).filter_map(|res_kv| match res_kv {
            Err(e) => Some(Err(e)),
            Ok((_k, OptDatum::Tombstone)) => None,
            Ok((k, OptDatum::Some(v))) => Some(Ok((k, v))),
        });
        Ok(out_iter)
    }

    pub fn get_whole_range<'a>(&'a self) -> Result<impl 'a + Iterator<Item = Result<(K, V)>>> {
        self.get_range(None, None)
    }
}
