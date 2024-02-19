use crate::ds_n_a::send_ptr::NonNullSendPtr;
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
    pub fn from_elems(mut elems: impl Iterator<Item = T>) -> Self {
        let mut head_ptr: *mut ListNode<T> = ptr::null_mut();
        let mut tail_ptr = head_ptr;

        if let Some(elem) = elems.next() {
            let head_own = Box::new(ListNode {
                elem,
                next: AtomicPtr::default(),
            });
            head_ptr = Box::into_raw(head_own);
            tail_ptr = head_ptr;
        }
        for elem in elems {
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

    pub fn head_node_ptr(&self) -> Option<NonNull<ListNode<T>>> {
        let head_ptr = self.head_ptr.load(Ordering::SeqCst);
        NonNull::new(head_ptr)
    }

    pub fn push_head_elem(&self, elem: T) -> NonNull<ListNode<T>> {
        let y_own = Box::new(ListNode {
            elem,
            next: AtomicPtr::default(),
        });
        self.push_head_node(y_own)
    }

    pub fn push_head_node(&self, y_own: Box<ListNode<T>>) -> NonNull<ListNode<T>> {
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
        unsafe { NonNull::new_unchecked(y_ptr) }
    }

    #[allow(dead_code)]
    pub fn iter<'a>(&'a self) -> ListIterator<'a, T> {
        ListIterator {
            next_ptr_ref: &self.head_ptr,
            tail_excl_ptr: ptr::null(),
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

pub struct ListIterator<'a, T> {
    next_ptr_ref: &'a AtomicPtr<ListNode<T>>,
    tail_excl_ptr: *const ListNode<T>,
}
impl<'a, T> Iterator for ListIterator<'a, T> {
    type Item = &'a T;
    fn next(&mut self) -> Option<Self::Item> {
        let curr_ptr = self.next_ptr_ref.load(Ordering::SeqCst).cast_const();
        if curr_ptr.is_null() || (curr_ptr == self.tail_excl_ptr) {
            return None;
        }
        let curr_ref = unsafe { &*curr_ptr };
        self.next_ptr_ref = &curr_ref.next;
        Some(&curr_ref.elem)
    }
}

/// A non-empty snapshot of an [`AtomicLinkedList`].
///
/// The relationship between any instance of this type and any instance of [`AtomicLinkedList`] is strictly implicit.
pub struct ListSnapshot<T> {
    head_ptr: NonNullSendPtr<ListNode<T>>,
    tail_ptr: Option<NonNullSendPtr<ListNode<T>>>,
}
impl<T> ListSnapshot<T> {
    pub fn new<PH, PT>(head_ptr: PH, tail_ptr: PT) -> Self
    where
        NonNullSendPtr<ListNode<T>>: From<PH>,
        NonNullSendPtr<ListNode<T>>: From<PT>,
    {
        Self {
            head_ptr: NonNullSendPtr::from(head_ptr),
            tail_ptr: Some(NonNullSendPtr::from(tail_ptr)),
        }
    }

    /// If [`Self::new()`] took [`Option`] as the arg for the tail_ptr,
    /// and if the caller specifies `None`, then the caller must specify the type signature (`None::<Foo>`),
    /// which is annoying, hence this separate constructor.
    pub fn new_tailless<PH>(head_ptr: PH) -> Self
    where
        NonNullSendPtr<ListNode<T>>: From<PH>,
    {
        Self {
            head_ptr: NonNullSendPtr::from(head_ptr),
            tail_ptr: None,
        }
    }

    pub fn head_ptr(&self) -> NonNullSendPtr<ListNode<T>> {
        self.head_ptr
    }
    pub fn tail_ptr(&self) -> Option<NonNullSendPtr<ListNode<T>>> {
        self.tail_ptr
    }

    pub fn iter_excluding_head_and_tail<'a, 'b>(&'a self) -> ListIterator<'b, T> {
        let head_ref = unsafe { self.head_ptr.as_ref() };
        let tail_excl_ptr = match self.tail_ptr {
            None => ptr::null(),
            Some(tail_ptr) => tail_ptr.as_ptr(),
        };
        ListIterator {
            next_ptr_ref: &head_ref.next,
            tail_excl_ptr,
        }
    }
}
