use crate::ds_n_a::atomic_linked_list::{AtomicLinkedList, ListNode};
use crate::storage::engine_ssi::lsm_state::unit::{CommitVer, CommittedUnit};
use derive_more::{Constructor, Deref, DerefMut, From};
use std::collections::BTreeMap;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicUsize};

#[derive(From, Deref, DerefMut, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ListVer(u64);

pub const LIST_VER_PLACEHOLDER: ListVer = ListVer(0);

pub enum LsmElemContent {
    Unit(CommittedUnit),
    Dummy {
        hold_count: AtomicUsize,
        is_fence: AtomicBool,
    },
}

pub struct LsmElem {
    pub content: LsmElemContent,
    pub traversable_list_ver_lo_incl: ListVer,
}

#[derive(Constructor)]
pub struct LsmState {
    pub list: AtomicLinkedList<LsmElem>,

    pub next_commit_ver: CommitVer,

    pub curr_list_ver: ListVer,
    held_list_vers: BTreeMap<ListVer, usize>,
}

impl LsmState {
    pub fn hold_curr_list_ver(&mut self) -> ListVer {
        self.held_list_vers
            .entry(self.curr_list_ver)
            .or_insert_with(|| 1);
        self.curr_list_ver
    }

    pub fn unhold_list_ver(&mut self, arg_ver: ListVer) -> Option<GcAbleInterval> {
        match self.held_list_vers.get_mut(&arg_ver) {
            None => {
                return None;
            }
            Some(count) => {
                if *count != 1 {
                    *count -= 1;
                    return None;
                } else {
                    self.held_list_vers.remove(&arg_ver);

                    if arg_ver == self.curr_list_ver {
                        return None;
                    } else {
                        let mut lo_excl = None;
                        let mut hi_excl = None;

                        for (list_ver, _) in self.held_list_vers.iter() {
                            if list_ver < &arg_ver {
                                lo_excl = Some(list_ver.clone());
                            } else {
                                hi_excl = Some(list_ver.clone());
                                break;
                            }
                        }

                        let hi_excl = hi_excl.unwrap_or(self.curr_list_ver);

                        return Some(GcAbleInterval { lo_excl, hi_excl });
                    }
                }
            }
        }
    }

    /// Returns
    /// - the curr_list_ver.
    /// - a newly GC'able ListVer interval, if any.
    pub fn hold_and_unhold_list_ver<'a>(
        &mut self,
        prior: ListVer,
    ) -> (ListVer, Option<GcAbleInterval>) {
        let mut gc_itv = None;
        if prior != self.curr_list_ver {
            gc_itv = self.unhold_list_ver(prior);
            self.hold_curr_list_ver();
        }
        return (self.curr_list_ver, gc_itv);
    }

    pub fn is_held_list_vers_empty(&self) -> bool {
        self.held_list_vers.is_empty()
    }

    /// Non-atomically does the following:
    /// 1. Get the head.
    /// 1. Callback reads the head and determines whether to push a new node.
    /// 1. If there is a new node to push, push it.
    /// Returns the resulting head.
    pub fn update_or_push<Cb>(&self, update_or_provide_head: Cb) -> *const ListNode<LsmElem>
    where
        Cb: FnOnce(Option<&LsmElemContent>) -> Option<Box<ListNode<LsmElem>>>,
    {
        let maybe_head_ref = self.list.head();
        let maybe_head_content = maybe_head_ref.map(|head_ref| &head_ref.elem.content);
        if let Some(new_node) = update_or_provide_head(maybe_head_content) {
            let new_head_ptr = self.list.push_node(new_node);
            return new_head_ptr;
        } else {
            match maybe_head_ref {
                None => return ptr::null(),
                Some(head_ref) => return head_ref as *const _,
            }
        }
    }
}

pub struct GcAbleInterval {
    pub lo_excl: Option<ListVer>,
    pub hi_excl: ListVer,
}
