use crate::{
    ds_n_a::{atomic_linked_list::ListNode, send_ptr::NonNullSendPtr},
    lsm::{ListVer, LsmElem},
    opers::fc::gc::DanglingNodeSetsDeque,
    DB,
};
use anyhow::{anyhow, Result};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch};

mod fc_compaction;
mod fc_traversal;
mod gc;

pub struct FlushingAndCompactionWorker {
    pub(crate) db: Arc<DB>,

    pub(crate) dangling_nodes: DanglingNodeSetsDeque,

    pub(crate) fc_avail_rx: watch::Receiver<()>,
    pub(crate) fc_request_rx: mpsc::Receiver<FlushAndCompactRequest>,
    pub(crate) min_held_list_ver_rx: watch::Receiver<ListVer>,
    pub(crate) is_terminating_rx: watch::Receiver<()>,
}

impl FlushingAndCompactionWorker {
    pub async fn run(mut self) -> Result<()> {
        loop {
            tokio::select! {
                res = (self.fc_avail_rx.changed()) => {
                    res.ok();
                    self.flush_and_compact(None).await?;
                }
                opt_msg = (self.fc_request_rx.recv()) => {
                    if let Some(msg) = opt_msg {
                        let FlushAndCompactRequest{snap_head, response_tx} = msg;
                        self.flush_and_compact(Some(snap_head)).await?;
                        response_tx.send(()).map_err(|()| anyhow!("Could not notify flushing+compaction job completion to its requester."))?;
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

pub struct FlushAndCompactRequest {
    pub snap_head: NonNullSendPtr<ListNode<LsmElem>>,

    pub response_tx: oneshot::Sender<()>,
}
