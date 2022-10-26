use anyhow::Result;
use std::borrow::Borrow;
use std::time::Duration;
use tokio::task::{JoinError, JoinHandle};

pub fn sleep_sync(millis: u64) {
    std::thread::sleep(Duration::from_millis(millis))
}
pub async fn sleep_async(millis: u64) {
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

/// First join all tasks. Then evaluate `Result<_>`s.
pub async fn join_tasks<T>(tasks: Vec<JoinHandle<Result<T>>>) -> Result<Vec<T>> {
    let mut join_results = vec![];
    for task in tasks.into_iter() {
        let join_res: Result<Result<T>, JoinError> = task.await;
        join_results.push(join_res);
    }

    let mut ret_items = vec![];
    for join_res in join_results.into_iter() {
        let ret_item = join_res??;
        ret_items.push(ret_item);
    }
    Ok(ret_items)
}
