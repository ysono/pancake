mod compaction;
mod fc;
mod gc;
mod scnd;
use gc::DanglingNodeSet;

use crate::{
    lsm_state::ListVer,
    opers::sicr_job::{ScndIdxCreationRequest, ScndIdxCreationWork},
    DB,
};
use anyhow::Result;
use derive_more::Constructor;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};

#[derive(Constructor)]
pub struct FlushingAndCompactionJob {
    db: Arc<DB>,

    dangling_nodes: VecDeque<DanglingNodeSet>,

    /* rx */
    min_held_list_ver: watch::Receiver<ListVer>,
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
                res = (self.min_held_list_ver.changed()) => {
                    res.ok();
                    let min_held_list_ver = self.min_held_list_ver.borrow().clone();
                    self.gc_dangling_nodes(min_held_list_ver);
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
            // For each of these channels, the sender is a property of DB, hence can never be dropped.
        }

        println!("F+C received termination signal.");

        self.scnd_idx_creation_request.close();

        self.poll_held_list_vers_then_gc().await;

        println!("F+C is exiting.");

        Ok(())
    }
}
