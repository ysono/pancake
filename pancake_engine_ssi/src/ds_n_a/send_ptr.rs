use derive_more::{Deref, From};
use std::ptr::{self, NonNull};

/// A newtype of `*const T` that's `unsafe`ly marked as [`Send`] and [`Sync`].
/// Use at your own discretion.
#[derive(From)]
pub struct SendPtr<T>(*const T);
impl<T> SendPtr<T> {
    pub fn as_ptr(&self) -> *const T {
        self.0
    }
}
impl<T> Clone for SendPtr<T> {
    #[allow(clippy::non_canonical_clone_impl)]
    fn clone(&self) -> Self {
        Self(self.0)
    }
}
impl<T> Copy for SendPtr<T> {}
unsafe impl<T> Send for SendPtr<T> {}
unsafe impl<T> Sync for SendPtr<T> {}

/// A newtype of [`NonNull`] that's `unsafe`ly marked as [`Send`] and [`Sync`].
/// Use at your own discretion.
#[derive(From, Deref)]
pub struct NonNullSendPtr<T>(NonNull<T>);
impl<T> NonNullSendPtr<T> {
    pub fn as_ptr(opt_self: Option<Self>) -> *const T {
        match opt_self {
            None => ptr::null(),
            Some(slf) => slf.0.as_ptr(),
        }
    }
    pub fn as_sendptr(opt_self: Option<Self>) -> SendPtr<T> {
        SendPtr::from(Self::as_ptr(opt_self))
    }
}
impl<T> Clone for NonNullSendPtr<T> {
    #[allow(clippy::non_canonical_clone_impl)]
    fn clone(&self) -> Self {
        Self(self.0)
    }
}
impl<T> Copy for NonNullSendPtr<T> {}
unsafe impl<T> Send for NonNullSendPtr<T> {}
unsafe impl<T> Sync for NonNullSendPtr<T> {}
