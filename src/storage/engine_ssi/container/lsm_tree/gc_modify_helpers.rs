use crate::ds_n_a::atomic_linked_list::{ListElem, ListNode};
use crate::ds_n_a::send_ptr::SendPtr;
use crate::storage::engine_ssi::container::{DanglingSlice, LSMTree};
use crate::storage::engine_ssi::entryset::{CommittedEntrySet, SSTable};
use std::sync::atomic::{AtomicPtr, Ordering};

impl<'a, K, V> LSMTree<K, V> {
    /// Cut non-terminus dummy nodes immediately older than K.
    /// K is constant. J is pointed to the new node that is immediately older than K.
    pub(super) async fn cut_nonterminus_dummies(
        &self,
        k_ptr: SendPtr<ListNode<CommittedEntrySet<K, V>>>,
        j_ptr: &mut SendPtr<ListNode<CommittedEntrySet<K, V>>>,
    ) {
        let mut h_ptr = k_ptr;
        let mut g_ptr = *j_ptr;

        loop {
            let g_ref = unsafe { g_ptr.as_ref() };
            match &g_ref.elem {
                ListElem::Dummy { is_terminus } => {
                    let is_terminus = is_terminus.load(Ordering::SeqCst);
                    if is_terminus == false {
                        h_ptr = g_ptr;
                        g_ptr = SendPtr::from(g_ref.older.load(Ordering::SeqCst));
                    } else {
                        break;
                    }
                }
                _ => break,
            }
        }

        if g_ptr != *j_ptr {
            let k_ref = unsafe { k_ptr.as_ref() };
            k_ref.older.store(g_ptr.raw() as *mut _, Ordering::SeqCst);

            self.save_dangling_slice(*j_ptr, h_ptr).await;

            *j_ptr = g_ptr;
        }
    }

    /// Outcome: K --> R --> G.
    /// K is pointed to R. J is pointed to G.
    pub(super) async fn replace_slice(
        &self,
        k_ptr: &mut SendPtr<ListNode<CommittedEntrySet<K, V>>>,
        j_ptr: &mut SendPtr<ListNode<CommittedEntrySet<K, V>>>,
        h_ptr: SendPtr<ListNode<CommittedEntrySet<K, V>>>,
        g_ptr: SendPtr<ListNode<CommittedEntrySet<K, V>>>,
        sstable: SSTable<K, V>,
    ) {
        let r_own = Box::new(ListNode {
            elem: ListElem::Elem(CommittedEntrySet::SSTable(sstable)),
            older: AtomicPtr::new(g_ptr.raw() as *mut _),
        });
        let r_ptr = SendPtr::from(Box::into_raw(r_own));

        let k_ref = unsafe { k_ptr.as_ref() };
        k_ref.older.store(r_ptr.raw() as *mut _, Ordering::SeqCst);

        self.save_dangling_slice(*j_ptr, h_ptr).await;

        *k_ptr = r_ptr;
        *j_ptr = g_ptr;
    }

    async fn save_dangling_slice(
        &self,
        j_ptr: SendPtr<ListNode<CommittedEntrySet<K, V>>>,
        h_ptr: SendPtr<ListNode<CommittedEntrySet<K, V>>>,
    ) {
        let penult_list_ver = self.list_ver_state.get_and_inc_leading().await;

        let dangl_slice = DanglingSlice {
            list_ver: penult_list_ver,
            newest_incl: j_ptr,
            oldest_incl: h_ptr,
        };
        self.dangling_slices.push(dangl_slice);
    }
}
