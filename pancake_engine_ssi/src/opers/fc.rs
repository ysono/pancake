use crate::{
    lsm::ListVer,
    opers::{
        fc::gc::DanglingNodeSetsDeque,
        sicr_job::{ScndIdxCreationRequest, ScndIdxCreationWork},
    },
    DB,
};
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};

mod fc_compaction;
mod fc_traversal;
mod gc;
mod scnd;

pub struct FlushingAndCompactionWorker {
    pub(crate) db: Arc<DB>,

    pub(crate) dangling_nodes: DanglingNodeSetsDeque,

    /* rx */
    pub(crate) fc_avail_rx: watch::Receiver<()>,
    pub(crate) scnd_idx_creation_request_rx: mpsc::Receiver<ScndIdxCreationRequest>,
    pub(crate) min_held_list_ver_rx: watch::Receiver<ListVer>,
    pub(crate) is_terminating_rx: watch::Receiver<()>,

    /* tx */
    pub(crate) scnd_idx_work_tx: mpsc::Sender<ScndIdxCreationWork>,
}

impl FlushingAndCompactionWorker {
    pub async fn run(mut self) -> Result<()> {
        loop {
            tokio::select! {
                res = (self.fc_avail_rx.changed()) => {
                    res.ok();
                    self.flush_and_compact().await?;
                }
                opt_msg = (self.scnd_idx_creation_request_rx.recv()) => {
                    if let Some(msg) = opt_msg {
                        self.prep_for_scnd_idx_creation(msg).await?;
                    }
                }
                res = (self.min_held_list_ver_rx.changed()) => {
                    res.ok();
                    let min_held_list_ver = self.min_held_list_ver_rx.borrow().clone();
                    self.dangling_nodes.gc_old_nodes(min_held_list_ver)?;
                }
                res = (self.is_terminating_rx.changed()) => {
                    res.ok();
                    break
                }
            }
            // For each of these channels, the sender is a property of DB, hence can never be dropped.
        }

        println!("F+C received termination signal.");

        self.scnd_idx_creation_request_rx.close();

        self.poll_held_list_vers_then_gc().await?;

        println!("F+C is exiting.");

        Ok(())
    }
}
