#[derive(Eq)]
pub struct SendPtr<T>(*const T);
impl<T> SendPtr<T> {
    pub fn from(ptr: *const T) -> Self {
        Self(ptr)
    }
    pub fn as_ptr(&self) -> *const T {
        self.0
    }
    pub fn as_ptr_mut(&self) -> *mut T {
        self.0 as *mut T
    }
    pub unsafe fn as_ref<'a, 'b>(&'a self) -> &'b T {
        &*(self.0)
    }
}

unsafe impl<T> Send for SendPtr<T> {}
unsafe impl<T> Sync for SendPtr<T> {}

impl<T> PartialEq for SendPtr<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<T> Clone for SendPtr<T> {
    fn clone(&self) -> Self {
        Self(self.0)
    }
}
impl<T> Copy for SendPtr<T> {}
