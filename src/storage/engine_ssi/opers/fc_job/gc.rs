use crate::storage::engine_ssi::{
    lsm_state::{GcAbleInterval, ListVer, LsmElemContent},
    opers::fc_job::FlushingAndCompactionJob,
};
use anyhow::Result;
use tokio::time::Duration;

#[derive(PartialEq, Eq, Hash, Clone)]
pub struct NodeListVerInterval {
    pub lo_incl: ListVer,
    pub hi_incl: ListVer,
}
impl NodeListVerInterval {
    fn is_subsumed_by(&self, gc_able_itv: &GcAbleInterval) -> bool {
        let is_lo_covered = || match gc_able_itv.lo_excl {
            None => true,
            Some(gc_lo_excl) => gc_lo_excl < self.lo_incl,
        };
        let is_hi_covered = || self.hi_incl < gc_able_itv.hi_excl;
        is_lo_covered() && is_hi_covered()
    }
}

impl FlushingAndCompactionJob {
    fn do_gc<F>(&mut self, is_node_itv_droppable: F) -> Result<()>
    where
        F: Fn(&NodeListVerInterval) -> bool,
    {
        self.dangling_nodes.retain(|node_itv, nodes| {
            let is_droppable = is_node_itv_droppable(node_itv);

            if is_droppable {
                while let Some(node_ptr) = nodes.pop() {
                    let node_own = unsafe { Box::from_raw(node_ptr.as_ptr_mut()) };
                    match node_own.elem.content {
                        LsmElemContent::Unit(unit) => match unit.remove_dir() {
                            Err(e) => {
                                eprintln!("Unit dir could not be removed: {}", e.to_string());
                            }
                            Ok(()) => {}
                        },
                        LsmElemContent::Dummy { .. } => {}
                    }
                }
            }

            !is_droppable
        });
        Ok(())
    }

    pub(super) fn gc_dangling_nodes(&mut self, gc_able_itv: GcAbleInterval) -> Result<()> {
        self.do_gc(|node_itv| node_itv.is_subsumed_by(&gc_able_itv))?;

        Ok(())
    }

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

        self.do_gc(|_node_itv| true)?;

        Ok(())
    }
}
