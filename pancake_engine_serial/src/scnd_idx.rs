use crate::lsm::LSMTree;
use anyhow::Result;
use pancake_engine_common::{fs_utils, Entry};
use pancake_types::types::{PKShared, PVShared, SVPKShared, SubValue, SubValueSpec};
use std::fs::OpenOptions;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const SPEC_FILE_NAME: &str = "sv_spec.txt";
const LSM_DIR_NAME: &str = "lsm";

/// A secondary index is an abstraction of a sorted dictionary mapping
/// `(sub-portion of value , primary key) : value`.
///
/// Clients may query for `(primary key, value)` entries based on bounds
/// over `sub-portion of value`.
///
/// Each instance of [`SecondaryIndex`] is defined by a [`SubValueSpec`],
/// which specifies the `sub-portion of value` that this [`SecondaryIndex`]
/// indexes on.
pub struct SecondaryIndex {
    dir_path: PathBuf,
    spec: Arc<SubValueSpec>,
    lsm: LSMTree<SVPKShared, PVShared>,
}

impl SecondaryIndex {
    fn spec_file_path<P: AsRef<Path>>(scnd_idx_dir_path: P) -> PathBuf {
        scnd_idx_dir_path.as_ref().join(SPEC_FILE_NAME)
    }
    fn lsm_dir_path<P: AsRef<Path>>(scnd_idx_dir_path: P) -> PathBuf {
        scnd_idx_dir_path.as_ref().join(LSM_DIR_NAME)
    }

    pub fn load<P: AsRef<Path>>(scnd_idx_dir_path: P) -> Result<Self> {
        let spec_file_path = Self::spec_file_path(&scnd_idx_dir_path);
        let lsm_dir_path = Self::lsm_dir_path(&scnd_idx_dir_path);

        let spec_file = fs_utils::open_file(spec_file_path, OpenOptions::new().read(true))?;
        let mut spec_reader = BufReader::new(spec_file);
        let spec = SubValueSpec::deser(&mut spec_reader)?;
        let spec = Arc::new(spec);

        let lsm = LSMTree::load_or_new(lsm_dir_path)?;

        Ok(Self {
            dir_path: scnd_idx_dir_path.as_ref().into(),
            spec,
            lsm,
        })
    }

    pub fn new<P: AsRef<Path>>(
        scnd_idx_dir_path: P,
        spec: Arc<SubValueSpec>,
        prim_lsm: &LSMTree<PKShared, PVShared>,
    ) -> Result<Self> {
        let spec_file_path = Self::spec_file_path(&scnd_idx_dir_path);
        let lsm_dir_path = Self::lsm_dir_path(&scnd_idx_dir_path);
        fs_utils::create_dir_all(&lsm_dir_path)?;

        let spec_file = fs_utils::open_file(
            spec_file_path,
            OpenOptions::new().create_new(true).write(true),
        )?;
        let mut spec_writer = BufWriter::new(spec_file);
        spec.ser(&mut spec_writer)?;
        spec_writer.flush()?;

        let mut scnd_lsm = LSMTree::load_or_new(&lsm_dir_path)?;
        for entry in prim_lsm.get_whole_range() {
            let (_pk, pv) = entry.try_borrow()?;
            if let Some(sv) = spec.extract(pv) {
                let (pk, pv) = entry.into_owned_kv()?;
                let svpk = SVPKShared { sv, pk };
                scnd_lsm.put(svpk, Some(pv))?;
            }
        }

        Ok(Self {
            dir_path: scnd_idx_dir_path.as_ref().into(),
            spec,
            lsm: scnd_lsm,
        })
    }

    pub fn remove_dir(&self) -> Result<()> {
        fs_utils::remove_dir_all(&self.dir_path)?;
        Ok(())
    }

    pub fn spec(&self) -> &Arc<SubValueSpec> {
        &self.spec
    }

    pub fn put(
        &mut self,
        pk: &PKShared,
        old_pv: Option<&PVShared>,
        new_pv: Option<&PVShared>,
    ) -> Result<()> {
        let old_sv = old_pv.and_then(|old_pv| self.spec.extract(old_pv));
        let new_sv = new_pv.and_then(|new_pv| self.spec.extract(new_pv));

        // Assign old_sv to be Some iff we need to tombstone the old entry.
        // Assign new_sv to be Some iff we need to put the new entry.
        let (old_sv, new_sv) = match (old_sv, new_sv) {
            (Some(old_sv), Some(new_sv)) => {
                if old_sv != new_sv {
                    (Some(old_sv), Some(new_sv))
                } else if old_pv != new_pv {
                    (None, Some(new_sv))
                } else {
                    (None, None)
                }
            }
            pair => pair,
        };

        if let Some(old_sv) = old_sv {
            let svpk = SVPKShared {
                sv: old_sv,
                pk: pk.clone(),
            };
            self.lsm.put(svpk, None)?;
        }
        if let Some(new_sv) = new_sv {
            let svpk = SVPKShared {
                sv: new_sv,
                pk: pk.clone(),
            };
            self.lsm.put(svpk, new_pv.cloned())?;
        }

        Ok(())
    }

    pub fn get_range<'a>(
        &'a self,
        sv_lo: Option<&'a SubValue>,
        sv_hi: Option<&'a SubValue>,
    ) -> impl 'a + Iterator<Item = Entry<'a, PKShared, PVShared>> {
        self.lsm
            .get_range(sv_lo, sv_hi)
            .map(|entry| entry.convert::<PKShared, PVShared>())
    }
}
