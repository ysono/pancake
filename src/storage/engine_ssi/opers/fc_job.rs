mod compaction;
mod fc;
mod gc;
mod scnd;
use gc::NodeListVerInterval;

use crate::ds_n_a::atomic_linked_list::ListNode;
use crate::ds_n_a::send_ptr::SendPtr;
use crate::storage::engine_ssi::{
    lsm_state::{GcAbleInterval, LsmElem},
    opers::sicr_job::{ScndIdxCreationRequest, ScndIdxCreationWork},
    DB,
};
use anyhow::Result;
use derive_more::Constructor;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};

#[derive(Constructor)]
pub struct FlushingAndCompactionJob {
    db: Arc<DB>,

    dangling_nodes: HashMap<NodeListVerInterval, Vec<SendPtr<ListNode<LsmElem>>>>,

    /* rx */
    gc_avail: mpsc::Receiver<GcAbleInterval>,
    replace_avail: watch::Receiver<()>,
    scnd_idx_creation_request: mpsc::Receiver<ScndIdxCreationRequest>,
    is_terminating: watch::Receiver<()>,

    /* tx */
    scnd_idx_creation_work: mpsc::Sender<ScndIdxCreationWork>,
}

impl FlushingAndCompactionJob {
    pub async fn run(mut self) -> Result<()> {
        loop {
            tokio::select! {
                opt_msg = (self.gc_avail.recv()) => {
                    self.gc_dangling_nodes(opt_msg.unwrap())?;
                }
                res = (self.replace_avail.changed()) => {
                    res.ok();
                    self.flush_and_compact().await?;
                }
                opt_msg = (self.scnd_idx_creation_request.recv()) => {
                    if let Some(msg) = opt_msg {
                        self.prep_for_scnd_idx_creation(msg).await?;
                    }
                }
                res = (self.is_terminating.changed()) => {
                    res.ok();
                    break
                }
            }
        }

        self.gc_avail.close();
        self.scnd_idx_creation_request.close();

        self.poll_held_list_vers_then_gc().await?;

        println!("F+C is exiting.");

        Ok(())
    }
}
