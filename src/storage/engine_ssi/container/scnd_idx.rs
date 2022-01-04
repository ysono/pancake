use crate::storage::engine_ssi::container::LSMTree;
use crate::storage::types::{PVShared, SVPKShared, SubValueSpec};
use anyhow::Result;
use shorthand::ShortHand;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

fn spec_file_path<P: AsRef<Path>>(scnd_idx_dir: P) -> PathBuf {
    scnd_idx_dir.as_ref().join("spec.datum")
}

fn lsm_dir_path<P: AsRef<Path>>(scnd_idx_dir: P) -> PathBuf {
    scnd_idx_dir.as_ref().join("lsm")
}

#[derive(ShortHand)]
pub struct SecondaryIndex {
    scnd_idx_dir: PathBuf,
    spec: Arc<SubValueSpec>,
    lsm: LSMTree<SVPKShared, PVShared>,
    is_built: AtomicBool,
}

impl SecondaryIndex {
    pub fn new<P: AsRef<Path>>(scnd_idx_dir: P, spec: Arc<SubValueSpec>) -> Result<Self> {
        let spec_path = spec_file_path(&scnd_idx_dir);
        let lsm_dir = lsm_dir_path(&scnd_idx_dir);
        fs::create_dir_all(&lsm_dir)?;

        let spec = Arc::<SubValueSpec>::from(spec);
        let spec_file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&spec_path)?;
        let mut spec_writer = BufWriter::new(spec_file);
        spec.ser(&mut spec_writer)?;

        let lsm = LSMTree::load_or_new(lsm_dir)?;

        let is_built = AtomicBool::new(false);

        Ok(Self {
            scnd_idx_dir: scnd_idx_dir.as_ref().into(),
            spec,
            lsm,
            is_built,
        })
    }

    pub fn load<P: AsRef<Path>>(scnd_idx_dir: P) -> Result<Self> {
        let spec_path = spec_file_path(&scnd_idx_dir);
        let lsm_dir = lsm_dir_path(&scnd_idx_dir);

        let spec_file = File::open(spec_path)?;
        let mut spec_file_reader = BufReader::new(spec_file);
        let spec = SubValueSpec::deser(&mut spec_file_reader)?;

        let lsm = LSMTree::load_or_new(lsm_dir)?;

        let is_built = AtomicBool::new(true);
        // Note, there is no mechanism for detecting an incomplete build.

        Ok(Self {
            scnd_idx_dir: scnd_idx_dir.as_ref().into(),
            spec: Arc::new(spec),
            lsm,
            is_built,
        })
    }
}
