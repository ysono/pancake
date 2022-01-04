use super::Txn;
use crate::ds_n_a::cmp::TryPartialOrd;
use crate::ds_n_a::interval_set::IntervalSet;
use crate::storage::engine_ssi::container::LSMTree;
use crate::storage::engines_common::Entry;
use crate::storage::serde::{OptDatum, Serializable};
use anyhow::Result;
use std::collections::HashMap;
use std::mem;

#[derive(PartialEq, Eq, Debug)]
pub enum CommitResult {
    Conflict,
    Success,
}

impl<'txn> Txn<'txn> {
    pub async fn try_commit(&mut self) -> Result<CommitResult> {
        if self.written_prim.is_none() {
            return Ok(CommitResult::Success);
        }

        self.update_snapshot_gap().await;

        loop {
            loop {
                if self.gap_ver_lo == self.snap_ver_ceil {
                    break;
                }

                let has_conflict = self.has_conflict().await?;

                self.update_snapshot_gap().await;

                if has_conflict {
                    return Ok(CommitResult::Conflict);
                }
            }

            {
                let _guard = self.db.commit_mutex().lock().await;

                self.update_snapshot_gap().await;

                if self.gap_ver_lo != self.snap_ver_ceil {
                    continue;
                }

                self.do_commit().await?;

                return Ok(CommitResult::Success);
            }
        }
    }

    async fn update_snapshot_gap(&mut self) {
        self.gap_ver_lo = self.snap_ver_ceil;
        self.snap_ver_ceil = self
            .db
            .commit_ver_state()
            .hold_leading_and_unhold(self.gap_ver_lo, || self.db.send_job_cv())
            .await;
    }

    async fn has_conflict(&mut self) -> Result<bool> {
        self.dep_itvs_prim.merge();
        let has_conflict = self
            .has_conflict_in_one_lsm(&self.dep_itvs_prim, self.db.prim_lsm())
            .await?;
        if has_conflict == true {
            return Ok(true);
        }

        for (_spec, itvs) in self.dep_itvs_scnds.iter_mut() {
            itvs.merge();
        }
        for (spec, itvs) in self.dep_itvs_scnds.iter() {
            let lsm = self.scnd_idxs_guard.get(spec).unwrap().lsm();
            let has_conflict = self.has_conflict_in_one_lsm(itvs, lsm).await?;
            if has_conflict == true {
                return Ok(true);
            }
        }

        Ok(false)
    }

    async fn has_conflict_in_one_lsm<K, V, Pt>(
        &'txn self,
        itvs: &'txn IntervalSet<Pt>,
        lsm: &'txn LSMTree<K, V>,
    ) -> Result<bool>
    where
        K: Serializable + Ord + Clone,
        OptDatum<V>: Serializable,
        Entry<'txn, K, OptDatum<V>>: TryPartialOrd<Pt>,
    {
        if itvs.is_empty() {
            return Ok(false);
        }

        let has_conflict: Result<bool> = lsm
            .iter_entrysets(
                Some(self.snap_ver_ceil),
                Some(self.gap_ver_lo),
                |entrysets| -> Result<bool> {
                    for entryset in entrysets {
                        let entries = entryset.get_whole_range();
                        let has_conf = itvs.overlaps_with(entries)?;
                        if has_conf {
                            return Ok(true);
                        }
                    }
                    Ok(false)
                },
                || self.db.send_job_cv(),
            )
            .await;
        has_conflict
    }

    async fn do_commit(&mut self) -> Result<()> {
        let written_scnds = mem::replace(&mut self.written_scnds, HashMap::new());
        for (spec, w_memlog) in written_scnds.into_iter() {
            if w_memlog.memtable.is_empty() {
                // Insert back, to be cleaned up later.
                self.written_scnds.insert(spec, w_memlog);
            } else {
                let scnd_idx = self.scnd_idxs_guard.get(&spec).unwrap();

                scnd_idx.lsm().commit(w_memlog, self.snap_ver_ceil).await?;
            }
        }

        let written_prim = self.written_prim.take().unwrap();
        self.db
            .prim_lsm()
            .commit(written_prim, self.snap_ver_ceil)
            .await?;

        self.db.commit_ver_state().get_and_inc_leading().await;
        self.db.send_job_cv();

        Ok(())
    }

    pub async fn clear(&mut self) -> Result<()> {
        self.dep_itvs_prim.clear();
        for (_spec, itvset) in self.dep_itvs_scnds.iter_mut() {
            itvset.clear();
        }

        if let Some(written) = self.written_prim.as_mut() {
            written.clear()?;
        }
        for (_spec, written) in self.written_scnds.iter_mut() {
            written.clear()?;
        }

        self.update_snapshot_gap().await;

        Ok(())
    }
}
