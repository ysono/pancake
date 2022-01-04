use pancake::storage::engine_serial::db::DB;
use std::env;
use std::sync::{Arc, RwLock};

#[tokio::main]
async fn main() {
    let db_dir = env::temp_dir().join("pancake");
    let db = DB::load_or_new(db_dir).unwrap();
    let db: Arc<RwLock<DB>> = Arc::new(RwLock::new(db));

    pancake::frontend::http::main(db).await;
}
