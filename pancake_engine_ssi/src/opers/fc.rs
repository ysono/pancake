use crate::{
    lsm::{unit::CommitVer, ListVer},
    opers::fc::gc::DanglingNodeSetsDeque,
    DB,
};
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};

mod fc_compaction;
mod fc_segm;
mod gc;

pub struct FlushingAndCompactionWorker {
    pub(crate) db: Arc<DB>,

    pub(crate) dangling_nodes: DanglingNodeSetsDeque,

    pub(crate) fc_able_commit_vers_rx: mpsc::Receiver<CommitVer>,
    pub(crate) min_held_list_ver_rx: watch::Receiver<ListVer>,
    pub(crate) is_terminating_rx: watch::Receiver<()>,
}

impl FlushingAndCompactionWorker {
    pub async fn run(mut self) -> Result<()> {
        loop {
            tokio::select! {
                opt_msg = (self.fc_able_commit_vers_rx.recv()) => {
                    if let Some(probe_commit_ver) = opt_msg {
                        self.flush_and_compact(probe_commit_ver).await?;
                    }
                }
                res = (self.min_held_list_ver_rx.changed()) => {
                    res.ok();
                    let min_held_list_ver = *(self.min_held_list_ver_rx.borrow());
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

        self.poll_held_list_vers_then_gc().await?;

        println!("F+C is exiting.");

        Ok(())
    }
}
