use crate::ds_n_a::atomic_linked_list::ListNode;
use crate::ds_n_a::send_ptr::SendPtr;
use crate::storage::engine_ssi::{
    lsm_state::{ListVer, LsmElem},
    opers::fc_job::FlushingAndCompactionJob,
};
use std::time::Duration;

pub struct DanglingNodeSet {
    pub max_incl_traversable_list_ver: ListVer,
    pub nodes: Vec<SendPtr<ListNode<LsmElem>>>,
}

impl FlushingAndCompactionJob {
    fn do_gc(&mut self, is_gc_able: impl Fn(ListVer) -> bool) {
        while let Some(DanglingNodeSet {
            max_incl_traversable_list_ver,
            ..
        }) = self.dangling_nodes.front()
        {
            if is_gc_able(max_incl_traversable_list_ver.clone()) {
                let DanglingNodeSet { nodes, .. } = self.dangling_nodes.pop_front().unwrap();
                for node_ptr in nodes.into_iter() {
                    let node_own = unsafe { Box::from_raw(node_ptr.as_ptr_mut()) };
                    match node_own.elem {
                        LsmElem::Unit(unit) => match unit.remove_dir() {
                            Err(e) => {
                                eprintln!("Unit dir could not be removed: {}", e.to_string());
                            }
                            Ok(()) => {}
                        },
                        LsmElem::Dummy { .. } => {}
                    }
                }
            } else {
                break;
            }
        }
    }

    pub(super) fn gc_dangling_nodes(&mut self, min_held_list_ver: ListVer) {
        self.do_gc(|max_incl_traversable_list_ver| {
            max_incl_traversable_list_ver < min_held_list_ver
        });
    }

    pub(super) async fn poll_held_list_vers_then_gc(&mut self) {
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

        self.do_gc(|_| true);
    }
}
