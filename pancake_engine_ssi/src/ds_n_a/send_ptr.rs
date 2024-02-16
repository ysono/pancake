use derive_more::{Deref, From};
use std::ptr::NonNull;

/// A newtype of `*const T` that's `unsafe`ly marked as [`Send`] and [`Sync`].
/// Use at your own discretion.
#[derive(From)]
pub struct SendPtr<T>(*const T);
impl<T> SendPtr<T> {
    pub fn as_ptr(&self) -> *const T {
        self.0
    }
    pub unsafe fn as_ref<'a, 'b>(&'a self) -> &'b T {
        &*(self.0)
    }
}
impl<T> Clone for SendPtr<T> {
    fn clone(&self) -> Self {
        Self(self.0)
    }
}
impl<T> Copy for SendPtr<T> {}
unsafe impl<T> Send for SendPtr<T> {}
unsafe impl<T> Sync for SendPtr<T> {}

/// A newtype of [`NonNull`] that's `unsafe`ly marked as [`Send`] and [`Sync`].
/// Use at your own discretion.
#[derive(Deref)]
pub struct NonNullSendPtr<T>(SendPtr<T>);
impl<T> From<NonNull<T>> for NonNullSendPtr<T> {
    fn from(ptr: NonNull<T>) -> Self {
        Self(SendPtr::from(ptr.as_ptr().cast_const()))
    }
}
impl<T> Clone for NonNullSendPtr<T> {
    fn clone(&self) -> Self {
        Self(self.0)
    }
}
impl<T> Copy for NonNullSendPtr<T> {}
unsafe impl<T> Send for NonNullSendPtr<T> {}
unsafe impl<T> Sync for NonNullSendPtr<T> {}
