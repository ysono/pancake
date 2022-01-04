use crate::storage::engine_ssi::container::DB;
use crate::storage::types::SubValueSpec;
use anyhow::{anyhow, Result};
use std::fs;
use std::sync::atomic::Ordering;

pub enum DeleteScndIdxResult {
    NoOp(String),
    Success,
}

pub async fn delete_scnd_idx(db: &DB, spec: &SubValueSpec) -> Result<DeleteScndIdxResult> {
    {
        let guard = db.scnd_idxs().read().await;
        match guard.get(spec) {
            None => {
                return Ok(DeleteScndIdxResult::NoOp(format!(
                    "Secondary index for spec {:?} already does not exist.",
                    spec
                )));
            }
            Some(scnd_idx) => {
                if scnd_idx.is_built().load(Ordering::SeqCst) == false {
                    return Ok(DeleteScndIdxResult::NoOp(format!(
                        "Secondary index is being built. Wait till it's done."
                    )));
                }
            }
        }
    }

    let scnd_idx;
    {
        let mut guard = db.scnd_idxs().write().await;
        match guard.get(spec) {
            None => {
                return Ok(DeleteScndIdxResult::NoOp(format!(
                    "Secondary index for spec {:?} already does not exist.",
                    spec
                )));
            }
            Some(scnd_idx) => {
                if scnd_idx.is_built().load(Ordering::SeqCst) == false {
                    return Ok(DeleteScndIdxResult::NoOp(format!(
                        "Secondary index is being built. Wait till it's done."
                    )));
                }
            }
        }
        scnd_idx = guard.remove(spec).unwrap();
    }

    scnd_idx.lsm().delete_dangling_slices()?;
    let is_cleanup_done = scnd_idx.lsm().is_cleanup_done();

    fs::remove_dir_all(scnd_idx.scnd_idx_dir())?;

    if !is_cleanup_done {
        return Err(anyhow!([
            "Memory leak! Not all dangling slices could be cleaned.",
            "Did some thread fail to unhold a list ver?",
        ]
        .join(" ")));
    }

    Ok(DeleteScndIdxResult::Success)
}
