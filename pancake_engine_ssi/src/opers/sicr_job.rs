mod creation;

use crate::ds_n_a::atomic_linked_list::ListNode;
use crate::ds_n_a::send_ptr::SendPtr;
use crate::{
    db_state::ScndIdxNum,
    lsm::{unit::CommitVer, LsmElem},
    DB,
};
use anyhow::Result;
use derive_more::Constructor;
use pancake_types::types::SubValueSpec;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::sync::{mpsc, watch};

pub struct ScndIdxCreationRequest {
    pub sv_spec: Arc<SubValueSpec>,
    pub scnd_idx_num: ScndIdxNum,
    pub response_to_client: oneshot::Sender<Result<()>>,
}

pub struct ScndIdxCreationWork {
    pub snap_head_excl: SendPtr<ListNode<LsmElem>>,
    pub output_commit_ver: CommitVer,
    pub req: ScndIdxCreationRequest,
}

#[derive(Constructor)]
pub struct ScndIdxCreationJob {
    db: Arc<DB>,

    working_dir: PathBuf,

    /* rx */
    scnd_idx_creation_work: mpsc::Receiver<ScndIdxCreationWork>,
    is_terminating: watch::Receiver<()>,
}

impl ScndIdxCreationJob {
    pub async fn run(mut self) -> Result<()> {
        loop {
            tokio::select! {
                opt_msg = (self.scnd_idx_creation_work.recv()) => {
                    if let Some(msg) = opt_msg {
                        self.create(msg).await;
                    }
                }
                res = (self.is_terminating.changed()) => {
                    res.ok();
                    break
                }
            }
        }

        println!("SICr is exiting.");

        Ok(())
    }
}
