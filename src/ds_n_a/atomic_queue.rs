use std::fmt::Debug;
use std::pin::Pin;
use std::sync::atomic::{AtomicPtr, Ordering};

#[derive(Debug)]
enum QueueElem<T> {
    Elem(T),
    Dummy,
}

pub struct QueueNode<T> {
    elem: QueueElem<T>,
    next: AtomicPtr<QueueNode<T>>,
}

/// NOT thread-safe! At most one thread/task may modify and/or read this queue.
pub struct AtomicQueue<T> {
    dummy_front: QueueNode<T>,
    /// `last` is never nullptr.
    last: AtomicPtr<QueueNode<T>>,
}

impl<T> AtomicQueue<T> {
    pub fn new() -> Pin<Box<Self>> {
        let dummy_front = QueueNode {
            elem: QueueElem::Dummy,
            next: AtomicPtr::default(),
        };
        let mut moi = Box::pin(Self {
            dummy_front,
            last: AtomicPtr::default(),
        });

        let mut_ref_moi = unsafe { moi.as_mut().get_unchecked_mut() };
        mut_ref_moi.last = AtomicPtr::new(&mut mut_ref_moi.dummy_front);

        moi
    }

    pub fn push(&self, val: T) {
        let new_last_own = Box::new(QueueNode {
            elem: QueueElem::Elem(val),
            next: AtomicPtr::default(),
        });
        let new_last_ptr = Box::into_raw(new_last_own);

        let penult_ptr = self.last.load(Ordering::SeqCst);
        let penult_ref = unsafe { &*penult_ptr };
        penult_ref.next.store(new_last_ptr, Ordering::SeqCst);

        self.last.store(new_last_ptr, Ordering::SeqCst);
    }

    pub fn peek(&self) -> Option<&T> {
        let first_ptr = self.dummy_front.next.load(Ordering::SeqCst);
        if first_ptr.is_null() {
            return None;
        }
        let first_ref = unsafe { &*first_ptr };
        match &first_ref.elem {
            QueueElem::Elem(val) => return Some(val),
            _ => panic!("Unexpected elem type."),
        }
    }

    pub fn pop(&self) -> Option<T> {
        let first_ptr = self.dummy_front.next.load(Ordering::SeqCst);
        if first_ptr.is_null() {
            return None;
        }
        let first_own = unsafe { Box::from_raw(first_ptr) };
        let second_ptr = first_own.next.load(Ordering::SeqCst);
        self.dummy_front.next.store(second_ptr, Ordering::SeqCst);
        if second_ptr.is_null() {
            self.last
                .store(&self.dummy_front as *const _ as *mut _, Ordering::SeqCst);
        }

        match first_own.elem {
            QueueElem::Elem(val) => return Some(val),
            _ => panic!("Unexpected elem type."),
        }
    }
}

impl<T> Drop for AtomicQueue<T> {
    fn drop(&mut self) {
        let mut curr_ptr = self.dummy_front.next.load(Ordering::SeqCst);
        while !curr_ptr.is_null() {
            let curr_own = unsafe { Box::from_raw(curr_ptr) };
            curr_ptr = curr_own.next.load(Ordering::SeqCst);
        }
    }
}

#[cfg(test)]
mod test;
