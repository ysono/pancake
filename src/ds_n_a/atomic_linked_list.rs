use std::fmt::Debug;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};

#[derive(Debug)]
pub enum ListElem<T> {
    Elem(T),
    Dummy { is_terminus: AtomicBool },
}

impl<T> ListElem<T> {
    pub fn new_dummy(is_terminus: bool) -> Self {
        Self::Dummy {
            is_terminus: AtomicBool::new(is_terminus),
        }
    }
}

pub struct ListNode<T> {
    pub(in crate) elem: ListElem<T>,
    pub(in crate) older: AtomicPtr<ListNode<T>>,
}

impl<T> ListNode<T> {
    pub fn new_dummy(is_terminus: bool) -> Self {
        Self {
            elem: ListElem::new_dummy(is_terminus),
            older: AtomicPtr::default(),
        }
    }
}

pub struct AtomicLinkedList<T> {
    dummy_newest: ListNode<T>,
    dummy_oldest: ListNode<T>,
    _pin: PhantomPinned,
}

impl<T> AtomicLinkedList<T> {
    pub fn new<I: Iterator<Item = T>>(iter: I) -> Pin<Box<Self>> {
        let mut moi = Box::pin(Self {
            dummy_newest: ListNode::new_dummy(true),
            dummy_oldest: ListNode::new_dummy(true),
            _pin: PhantomPinned,
        });

        let mut_ref_moi = unsafe { moi.as_mut().get_unchecked_mut() };

        let mut curr_node = &mut mut_ref_moi.dummy_newest;
        for t in iter {
            let next_node_own = Box::new(ListNode {
                elem: ListElem::Elem(t),
                older: AtomicPtr::default(),
            });
            let next_node_ptr = Box::into_raw(next_node_own);
            curr_node.older = AtomicPtr::new(next_node_ptr);
            curr_node = unsafe { &mut *next_node_ptr };
        }
        curr_node.older = AtomicPtr::new(&mut mut_ref_moi.dummy_oldest);

        moi
    }

    pub fn push_newest(&self, elem: ListElem<T>) -> &ListNode<T> {
        let mut y_own = Box::new(ListNode {
            elem,
            older: AtomicPtr::default(),
        });

        let mut x_ptr = self.dummy_newest.older.load(Ordering::Acquire);
        y_own.older = AtomicPtr::new(x_ptr);
        let y_ptr = Box::into_raw(y_own);
        loop {
            let cae_res = self.dummy_newest.older.compare_exchange_weak(
                x_ptr,
                y_ptr,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
            match cae_res {
                Err(r_ptr) => {
                    x_ptr = r_ptr;
                    let y_ref = unsafe { &mut *y_ptr };
                    y_ref.older = AtomicPtr::new(x_ptr);
                }
                Ok(_) => break,
            }
        }
        unsafe { &*y_ptr }
    }

    pub fn iter(&self) -> AtomicLinkedListIterator<T> {
        AtomicLinkedListIterator {
            lst: &self,
            nxt: unsafe { &*self.dummy_newest.older.load(Ordering::SeqCst) },
        }
    }

    pub fn dummy_oldest(&self) -> &ListNode<T> {
        &self.dummy_oldest
    }
}

impl<T> Drop for AtomicLinkedList<T> {
    fn drop(&mut self) {
        let last_ptr = &self.dummy_oldest as *const _ as *mut _;
        let mut curr_ptr = self.dummy_newest.older.load(Ordering::SeqCst);
        while curr_ptr != last_ptr {
            let curr_own = unsafe { Box::from_raw(curr_ptr) };
            curr_ptr = curr_own.older.load(Ordering::SeqCst);
        }
    }
}

pub struct AtomicLinkedListIterator<'a, T> {
    lst: &'a AtomicLinkedList<T>,
    nxt: &'a ListNode<T>,
}

impl<'a, T> Iterator for AtomicLinkedListIterator<'a, T> {
    type Item = &'a ListElem<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.nxt as *const _ == &self.lst.dummy_oldest as *const _ {
            None
        } else {
            let node = self.nxt;
            self.nxt = unsafe { &*self.nxt.older.load(Ordering::SeqCst) };
            Some(&node.elem)
        }
    }
}

#[cfg(test)]
mod test;
