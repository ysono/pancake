use crate::ds_n_a::atomic_linked_list::ListNode;
use crate::ds_n_a::send_ptr::NonNullSendPtr;
use crate::{
    lsm::{ListVer, LsmElem, LsmElemType},
    opers::fc::FlushingAndCompactionWorker,
};
use anyhow::Result;
use std::collections::VecDeque;
use std::time::Duration;

#[derive(Default)]
pub struct DanglingNodeSetsDeque {
    deque: VecDeque<DanglingNodeSet>,
}

impl DanglingNodeSetsDeque {
    pub fn push_back(&mut self, set: DanglingNodeSet) {
        self.deque.push_back(set);
    }

    pub fn gc_old_nodes(&mut self, min_held_list_ver: ListVer) -> Result<()> {
        let is_set_gcable =
            |set: &DanglingNodeSet| set.max_incl_traversable_list_ver < min_held_list_ver;
        self.gc(is_set_gcable)
    }

    pub fn gc_all_nodes(&mut self) -> Result<()> {
        let is_set_gcable = |_: &DanglingNodeSet| true;
        self.gc(is_set_gcable)
    }

    fn gc<F>(&mut self, is_set_gcable: F) -> Result<()>
    where
        F: Fn(&DanglingNodeSet) -> bool,
    {
        while let Some(set) = self.deque.front() {
            if is_set_gcable(set) {
                let set = self.deque.pop_front().unwrap();
                for nodes in set.nodes {
                    for node_ptr in nodes.into_iter() {
                        let node_own = unsafe { Box::from_raw(node_ptr.as_ptr().cast_mut()) };
                        match node_own.elem.elem_type {
                            LsmElemType::CommittedUnit(unit) => {
                                unit.remove_dir()?;
                            }
                            LsmElemType::Dummy { .. } => {}
                        }
                    }
                }
            } else {
                break;
            }
        }

        Ok(())
    }
}

pub struct DanglingNodeSet {
    pub max_incl_traversable_list_ver: ListVer,
    pub nodes: Vec<Vec<NonNullSendPtr<ListNode<LsmElem>>>>,
}

impl FlushingAndCompactionWorker {
    pub(super) async fn poll_held_list_vers_then_gc(&mut self) -> Result<()> {
        loop {
            println!("F+C is polling for all ListVers to be unheld.");
            {
                let lsm_state = self.db.lsm_state().lock().await;

                if lsm_state.is_held_list_vers_empty() {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        self.dangling_nodes.gc_all_nodes()?;

        Ok(())
    }
}
