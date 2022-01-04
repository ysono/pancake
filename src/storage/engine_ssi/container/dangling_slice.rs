use crate::ds_n_a::atomic_linked_list::{ListElem, ListNode};
use crate::ds_n_a::send_ptr::SendPtr;
use crate::storage::engine_ssi::container::ListVer;
use crate::storage::engine_ssi::entryset::CommittedEntrySet;
use anyhow::Result;
use std::sync::atomic::Ordering;

pub struct DanglingSlice<K, V> {
    pub list_ver: ListVer,
    pub newest_incl: SendPtr<ListNode<CommittedEntrySet<K, V>>>,
    pub oldest_incl: SendPtr<ListNode<CommittedEntrySet<K, V>>>,
}

impl<K, V> DanglingSlice<K, V> {
    /// [`DanglingSlice`] is NOT automatically dropped!
    /// This is because the timing at which memory and dir can be deleted depends on the context.
    /// User must ensure to call this manual drop method upon:
    /// - Deletion of a secondary index
    /// - Termination of the program
    pub fn drop_dirs_and_memory(self) -> Result<()> {
        let last_ptr = self.oldest_incl.raw();
        let mut curr_ptr = self.newest_incl.raw();
        loop {
            let curr_own = unsafe { Box::from_raw(curr_ptr as *mut ListNode<_>) };

            if let ListElem::Elem(elem) = curr_own.elem {
                match elem {
                    CommittedEntrySet::RMemLog(memlog) => memlog.remove_entryset_dir()?,
                    CommittedEntrySet::SSTable(sstable) => sstable.remove_entryset_dir()?,
                }
            }

            if curr_ptr == last_ptr {
                break;
            } else {
                curr_ptr = curr_own.older.load(Ordering::SeqCst);
            }
        }

        Ok(())
    }
}
