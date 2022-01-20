use std::borrow::Borrow;
use std::time::Duration;

pub async fn sleep(millis: u64) {
    tokio::time::sleep(Duration::from_millis(millis)).await;
}

/// Use case:
/// Launching async task(s) that capture a reference,
///     then joining such task(s) in a local scope,
///     s.t. we know that the reference is safe, although rust compiler doesn't.
///
/// Specific use case:
/// Arguments to "get_by" apis require `&'txn PrimaryKey` or `&'txn SubValue`.
/// For these, we must reference a variable that is created before a txn begins running.
pub unsafe fn coerce_ref_to_static<T, U>(t: &T) -> &'static U
where
    T: Borrow<U>,
{
    let t_ptr = t.borrow() as *const U;
    &*t_ptr
}
