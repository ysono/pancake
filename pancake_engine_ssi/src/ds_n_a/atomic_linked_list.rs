use crate::ds_n_a::send_ptr::{NonNullSendPtr, SendPtr};
use std::marker::PhantomData;
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicPtr, Ordering};

mod test;

pub struct ListNode<T> {
    pub elem: T,
    pub next: AtomicPtr<ListNode<T>>,
}

impl<T> ListNode<T> {
    pub fn new(elem: T) -> Box<Self> {
        Box::new(Self {
            elem,
            next: AtomicPtr::default(),
        })
    }
}

pub struct AtomicLinkedList<T> {
    /// This field could be an `AtomicPtr<ListNode<T>>`, but in all our use cases,
    /// the head is modified without contention, so we make it typed non-atomic.
    head_ptr: Option<NonNullSendPtr<ListNode<T>>>,
}

impl<T> AtomicLinkedList<T> {
    pub fn from_elems(mut elems: impl Iterator<Item = T>) -> Self {
        let mut head_ptr: *mut ListNode<T> = ptr::null_mut();
        let mut tail_ptr = head_ptr;

        if let Some(elem) = elems.next() {
            let head_own = ListNode::new(elem);
            head_ptr = Box::into_raw(head_own);
            tail_ptr = head_ptr;
        }
        for elem in elems {
            let curr_own = ListNode::new(elem);
            let curr_ptr = Box::into_raw(curr_own);

            let tail_ref = unsafe { &*tail_ptr };
            tail_ref.next.store(curr_ptr, Ordering::SeqCst);

            tail_ptr = curr_ptr;
        }

        let head_ptr = NonNull::new(head_ptr).map(NonNullSendPtr::from);
        Self { head_ptr }
    }

    pub fn head_node_ptr(&self) -> Option<NonNullSendPtr<ListNode<T>>> {
        self.head_ptr
    }

    pub fn set_head_node_ptr_noncontested(
        &mut self,
        new_head_ptr: Option<NonNullSendPtr<ListNode<T>>>,
    ) {
        self.head_ptr = new_head_ptr;
    }

    pub fn push_head_node_noncontested(
        &mut self,
        new_head_own: Box<ListNode<T>>,
    ) -> NonNullSendPtr<ListNode<T>> {
        let head_ptr = NonNullSendPtr::as_ptr(self.head_ptr);
        new_head_own
            .next
            .store(head_ptr.cast_mut(), Ordering::SeqCst);

        let new_head_ptr = Box::into_raw(new_head_own);
        let new_head_ptr = NonNullSendPtr::from(unsafe { NonNull::new_unchecked(new_head_ptr) });
        self.head_ptr = Some(new_head_ptr);
        new_head_ptr
    }

    pub fn snap(&self) -> ListSnapshot<T> {
        ListSnapshot {
            head_ptr: self.head_ptr,
            tail_ptr: None,
        }
    }
}

impl<T> Drop for AtomicLinkedList<T> {
    fn drop(&mut self) {
        let mut curr_ptr = NonNullSendPtr::as_ptr(self.head_ptr).cast_mut();
        while !curr_ptr.is_null() {
            let curr_own = unsafe { Box::from_raw(curr_ptr) };
            curr_ptr = curr_own.next.load(Ordering::SeqCst);
        }
    }
}

/// The relationship between any instance of [`ListSnapshot`] and any instance of [`AtomicLinkedList`] is strictly implicit.
pub struct ListSnapshot<T> {
    head_ptr: Option<NonNullSendPtr<ListNode<T>>>,
    tail_ptr: Option<NonNullSendPtr<ListNode<T>>>,
}
impl<T> ListSnapshot<T> {
    pub fn new_unchecked(
        head_ptr: Option<NonNullSendPtr<ListNode<T>>>,
        tail_ptr: Option<NonNullSendPtr<ListNode<T>>>,
    ) -> Self {
        Self { head_ptr, tail_ptr }
    }

    pub fn head_ptr(&self) -> Option<NonNullSendPtr<ListNode<T>>> {
        self.head_ptr
    }

    /// Including head. Excluding tail.
    pub fn iter<'s, 'iter>(&'s self) -> ListIterator<'iter, T> {
        ListIterator::new(self.head_ptr, self.tail_ptr)
    }
}

pub struct ListIterator<'iter, T> {
    next_ptr: SendPtr<ListNode<T>>,
    tail_excl_ptr: SendPtr<ListNode<T>>,
    _phant: PhantomData<&'iter ()>,
}
impl<'iter, T> ListIterator<'iter, T> {
    fn new(
        head_incl_ptr: Option<NonNullSendPtr<ListNode<T>>>,
        tail_excl_ptr: Option<NonNullSendPtr<ListNode<T>>>,
    ) -> Self {
        Self {
            next_ptr: NonNullSendPtr::as_sendptr(head_incl_ptr),
            tail_excl_ptr: NonNullSendPtr::as_sendptr(tail_excl_ptr),
            _phant: PhantomData,
        }
    }

    pub fn next_node(&mut self) -> Option<&'iter ListNode<T>> {
        let curr_ptr = self.next_ptr.as_ptr();
        if curr_ptr.is_null() || (curr_ptr == self.tail_excl_ptr.as_ptr()) {
            return None;
        } else {
            let curr_ref = unsafe { &*curr_ptr };

            let next_ptr = curr_ref.next.load(Ordering::SeqCst).cast_const();
            self.next_ptr = SendPtr::from(next_ptr);

            return Some(curr_ref);
        }
    }
}
impl<'iter, T> Iterator for ListIterator<'iter, T>
where
    T: 'iter,
{
    type Item = &'iter T;
    fn next(&mut self) -> Option<Self::Item> {
        self.next_node().map(|node| &node.elem)
    }
}
