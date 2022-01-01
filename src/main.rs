use pancake::storage::db::DB;
use std::env;
use std::sync::{Arc, RwLock};

#[tokio::main]
async fn main() {
    let path = env::temp_dir().join("pancake");
    let db = DB::open(path).unwrap();
    let db: Arc<RwLock<DB>> = Arc::new(RwLock::new(db));

    pancake::frontend::http::main(db).await;
}
