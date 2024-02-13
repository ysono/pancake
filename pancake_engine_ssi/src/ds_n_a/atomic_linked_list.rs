use crate::ds_n_a::send_ptr::SendPtr;
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicPtr, Ordering};

mod test;

pub struct ListNode<T> {
    pub elem: T,
    pub next: AtomicPtr<ListNode<T>>,
}

pub struct AtomicLinkedList<T> {
    head_ptr: AtomicPtr<ListNode<T>>,
}

impl<T> AtomicLinkedList<T> {
    pub fn from_elems(mut iter: impl Iterator<Item = T>) -> Self {
        let mut head_ptr: *mut ListNode<T> = ptr::null_mut();
        let mut tail_ptr = head_ptr;

        if let Some(elem) = iter.next() {
            let head_own = Box::new(ListNode {
                elem,
                next: AtomicPtr::default(),
            });
            head_ptr = Box::into_raw(head_own);
            tail_ptr = head_ptr;
        }
        for elem in iter {
            let curr_own = Box::new(ListNode {
                elem,
                next: AtomicPtr::default(),
            });
            let curr_ptr = Box::into_raw(curr_own);

            let tail_ref = unsafe { &*tail_ptr };
            tail_ref.next.store(curr_ptr, Ordering::SeqCst);

            tail_ptr = curr_ptr;
        }

        Self {
            head_ptr: AtomicPtr::new(head_ptr),
        }
    }

    pub fn head(&self) -> Option<&ListNode<T>> {
        let head_ptr = self.head_ptr.load(Ordering::SeqCst);
        if head_ptr.is_null() {
            None
        } else {
            Some(unsafe { &*head_ptr })
        }
    }

    pub fn push_elem(&self, elem: T) -> *const ListNode<T> {
        let y_own = Box::new(ListNode {
            elem,
            next: AtomicPtr::default(),
        });
        self.push_node(y_own)
    }

    pub fn push_node(&self, y_own: Box<ListNode<T>>) -> *const ListNode<T> {
        let mut x_ptr = y_own.next.load(Ordering::SeqCst);
        let y_ptr = Box::into_raw(y_own);
        loop {
            let cae_res = self.head_ptr.compare_exchange_weak(
                x_ptr,
                y_ptr,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
            match cae_res {
                Err(r_ptr) => {
                    x_ptr = r_ptr;
                    let y_ref = unsafe { &*y_ptr };
                    y_ref.next.store(x_ptr, Ordering::SeqCst);
                }
                Ok(_) => break,
            }
        }
        y_ptr
    }

    #[allow(dead_code)]
    pub fn iter(&self) -> AtomicLinkedListIterator<T> {
        AtomicLinkedListIterator {
            next_ptr: &self.head_ptr,
        }
    }
}

impl<T> Drop for AtomicLinkedList<T> {
    fn drop(&mut self) {
        let mut curr_ptr = self.head_ptr.load(Ordering::SeqCst);
        while !curr_ptr.is_null() {
            let curr_own = unsafe { Box::from_raw(curr_ptr) };
            curr_ptr = curr_own.next.load(Ordering::SeqCst);
        }
    }
}

pub struct AtomicLinkedListIterator<'a, T> {
    next_ptr: &'a AtomicPtr<ListNode<T>>,
}
impl<'a, T> Iterator for AtomicLinkedListIterator<'a, T> {
    type Item = &'a T;
    fn next(&mut self) -> Option<Self::Item> {
        let curr_ptr = self.next_ptr.load(Ordering::SeqCst);
        if curr_ptr.is_null() {
            return None;
        }
        let curr_ref = unsafe { &*curr_ptr };
        self.next_ptr = &curr_ref.next;
        Some(&curr_ref.elem)
    }
}

/// In both head and tail, SendPtr is assumed to contain a non-nullptr.
pub struct AtomicLinkedListSnapshot<T> {
    pub head_excl_ptr: SendPtr<ListNode<T>>,
    pub tail_excl_ptr: Option<SendPtr<ListNode<T>>>,
}
impl<T> AtomicLinkedListSnapshot<T> {
    pub fn iter(&self) -> AtomicLinkedListSnapshotIterator<T> {
        let prev_ptr =
            if self.tail_excl_ptr.is_some() && self.tail_excl_ptr.unwrap() == self.head_excl_ptr {
                ptr::null()
            } else {
                self.head_excl_ptr.as_ptr()
            };

        AtomicLinkedListSnapshotIterator {
            prev_ptr,
            tail_excl_ptr: self
                .tail_excl_ptr
                .map(|send_ptr| unsafe { NonNull::new_unchecked(send_ptr.as_ptr_mut()) }),
        }
    }
}

pub struct AtomicLinkedListSnapshotIterator<T> {
    prev_ptr: *const ListNode<T>,
    tail_excl_ptr: Option<NonNull<ListNode<T>>>,
}
impl<T: 'static> Iterator for AtomicLinkedListSnapshotIterator<T> {
    type Item = &'static T;
    fn next(&mut self) -> Option<Self::Item> {
        if self.prev_ptr.is_null() {
            return None;
        } else {
            let prev_ref = unsafe { &*self.prev_ptr };
            let curr_ptr = prev_ref.next.load(Ordering::SeqCst);
            if curr_ptr.is_null()
                || (self.tail_excl_ptr.is_some()
                    && self.tail_excl_ptr.unwrap().as_ptr() == curr_ptr)
            {
                self.prev_ptr = ptr::null();
                return None;
            } else {
                self.prev_ptr = curr_ptr;
                let curr_ref = unsafe { &*curr_ptr };
                return Some(&curr_ref.elem);
            }
        }
    }
}
