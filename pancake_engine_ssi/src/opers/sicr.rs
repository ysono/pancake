use crate::{
    db::DB,
    db_state::{ScndIdxNewDefnResult, ScndIdxNum, ScndIdxState},
    ds_n_a::{atomic_linked_list::ListNode, send_ptr::NonNullSendPtr},
    lsm::{
        entryset::CommittedEntrySet,
        unit::{CommitVer, CommittedUnit, StagingUnit},
    },
};
use anyhow::{anyhow, Result};
use derive_more::Display;
use pancake_engine_common::{fs_utils, SSTable};
use pancake_types::{
    serde::OptDatum,
    types::{PVShared, SVPKShared, SubValueSpec},
};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::MutexGuard;

mod creation;
mod paths;

pub use paths::ScndIdxCreationsDir;
use paths::*;

impl DB {
    pub async fn create_scnd_idx(
        &self,
        sv_spec: &Arc<SubValueSpec>,
    ) -> Result<(), ScndIdxCreationJobErr> {
        let mut job = ScndIdxCreationJob::new(self, sv_spec).await?;
        job.run().await?;
        job.remove_intermediary_files()?;

        Ok(())
    }
}

struct ScndIdxCreationJob<'job> {
    db: &'job DB,

    _si_cr_guard: MutexGuard<'job, ()>,

    sv_spec: Arc<SubValueSpec>,

    si_num: ScndIdxNum,
    pre_output_commit_ver: CommitVer,
    output_commit_ver: CommitVer,
    output_node: NonNullSendPtr<ListNode<CommittedUnit>>,

    job_dir: ScndIdxCreationJobDir,
    prim_entryset_file_paths: Vec<PathBuf>,
}

impl<'job> ScndIdxCreationJob<'job> {
    async fn new(db: &'job DB, sv_spec: &Arc<SubValueSpec>) -> Result<Self, ScndIdxCreationJobErr> {
        let si_cr_guard = match db.si_cr_mutex().try_lock() {
            Err(_) => return Err(ScndIdxCreationJobErr::Busy),
            Ok(guard) => guard,
        };

        {
            let db_state = db.db_state().read().await;

            if db_state.is_terminating == true {
                return Err(anyhow!("DB is terminating").into());
            }

            match db_state.get_scnd_idx_defn(sv_spec) {
                Some(si_state) => return Err(si_state.into()),
                None => {}
            }
        }

        let output_unit_dir_path = db.lsm_dir().format_new_unit_dir_path();
        let output_unit = StagingUnit::new_empty(output_unit_dir_path)?;

        let (si_num, pre_output_commit_ver, output_commit_ver, snap, snap_list_ver);
        {
            let mut db_state = db.db_state().write().await;

            if db_state.is_terminating == true {
                return Err(anyhow!("DB is terminating").into());
            }

            match db_state.define_new_scnd_idx(sv_spec) {
                Err(e) => return Err(e.into()),
                Ok(ScndIdxNewDefnResult::Existent(si_state)) => return Err(si_state.into()),
                Ok(ScndIdxNewDefnResult::DidDefineNew(si_num_)) => si_num = si_num_,
            }

            {
                let mut lsm_state = db.lsm_state().lock().await;

                pre_output_commit_ver = lsm_state.hold_curr_commit_ver();

                lsm_state.bump_commit_ver(output_unit)?;

                output_commit_ver = lsm_state.hold_curr_commit_ver();

                snap = lsm_state.list().snap();

                snap_list_ver = lsm_state.hold_curr_list_ver();
            }
        }

        let job_dir = db.si_cr_dir().create_new_job_dir()?;
        let mut prim_entryset_file_paths = vec![];
        for unit in snap.iter() {
            if unit.prim.is_some() {
                let prim_file_path = unit.dir.format_prim_file_path();
                let stg_file_path = job_dir.format_new_kv_file_path();
                fs_utils::hard_link_file(prim_file_path, &stg_file_path)?;
                prim_entryset_file_paths.push(stg_file_path);
            }
        }

        let output_node = snap.head_ptr().unwrap();

        let updated_mhlv;
        {
            let mut lsm_state = db.lsm_state().lock().await;

            updated_mhlv = lsm_state.unhold_list_ver(snap_list_ver)?;
        }
        if let Some(mhlv) = updated_mhlv {
            db.notify_min_held_list_ver(mhlv);
        }

        Ok(Self {
            db,

            _si_cr_guard: si_cr_guard,

            sv_spec: Arc::clone(sv_spec),

            si_num,
            pre_output_commit_ver,
            output_commit_ver,
            output_node,

            job_dir,
            prim_entryset_file_paths,
        })
    }

    async fn run(&mut self) -> Result<(), ScndIdxCreationJobErr> {
        let merged_file_path = self.create_unit()?;

        self.modify_lsm_state(merged_file_path).await?;

        Ok(())
    }

    async fn modify_lsm_state(&self, merged_file_path: Option<PathBuf>) -> Result<()> {
        {
            let mut db_state = self.db.db_state().write().await;

            /* We're modifying output_node, which has already been in the LL, in-place.
            We must modify it while no other threads are traversing over the node. */
            if let Some(orig_path) = merged_file_path {
                let out_node_ref = unsafe { &mut *(self.output_node.as_ptr()) };

                let out_path = out_node_ref.elem.dir.format_scnd_file_path(self.si_num);

                fs_utils::rename_file(orig_path, &out_path)?;

                /* Note, we wrote as <SVPK, PV>, but are now reading as <SVPK, OptDatum<PV>>. This is valid. */
                let out_sstable = SSTable::<SVPKShared, OptDatum<PVShared>>::load(out_path)?;

                let out_entryset = CommittedEntrySet::SSTable(out_sstable);

                out_node_ref.elem.scnds.insert(self.si_num, out_entryset);
            }

            db_state.set_scnd_idx_as_readable(&self.sv_spec)?;
        }

        let fc_able_commit_vers;
        {
            let mut lsm_state = self.db.lsm_state().lock().await;

            fc_able_commit_vers = lsm_state.unhold_commit_vers([
                Some(self.pre_output_commit_ver),
                Some(self.output_commit_ver),
            ])?;
        }

        for cmt_ver in fc_able_commit_vers {
            if let Some(cmt_ver) = cmt_ver {
                let send_res = self.db.fc_able_commit_vers_tx().send(cmt_ver).await;
                if send_res.is_err() {
                    eprintln!("SICr could not notify to F+C that post-completion unholding of CommitVers caused one or more of these CommitVers to become non-held.");
                }
            }
        }

        Ok(())
    }

    fn remove_intermediary_files(self) -> Result<()> {
        self.job_dir.remove_dir()?;
        Ok(())
    }
}

#[derive(Debug, Display)]
pub enum ScndIdxCreationJobErr {
    Busy,
    Existent { is_readable: bool },
    InternalError(anyhow::Error),
}

impl From<ScndIdxState> for ScndIdxCreationJobErr {
    fn from(si_state: ScndIdxState) -> Self {
        Self::Existent {
            is_readable: si_state.is_readable,
        }
    }
}
impl<E: Into<anyhow::Error>> From<E> for ScndIdxCreationJobErr {
    fn from(e: E) -> Self {
        Self::InternalError(e.into())
    }
}
