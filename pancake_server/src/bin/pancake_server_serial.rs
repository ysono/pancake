use anyhow::Result;
use pancake_engine_common::fs_utils::{self, EngineType};
use pancake_engine_serial::DB;
use pancake_server::{
    common::server,
    engine_serial::{
        route_handlers::{self, AppState},
        wasm::WasmEngine,
    },
};
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let engine_type = EngineType::SERIAL;

    let root_dir = env::var(server::ENV_VAR_ROOT_DIR).map_or_else(
        |_| fs_utils::default_db_root_dir(engine_type),
        PathBuf::from,
    );

    let db = DB::load_or_new(root_dir)?;
    let db = Arc::new(RwLock::new(db));

    let wasm_engine = WasmEngine::new(Arc::clone(&db))?;

    let state = AppState::new(db, wasm_engine);
    let state = Arc::new(state);

    let router = route_handlers::create_router(state);

    let bind_addr = env::var(server::ENV_VAR_BIND_ADDR)
        .unwrap_or_else(|_| server::default_bind_addr(engine_type).to_string());
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    println!("Listening on {}", listener.local_addr()?);
    axum::serve(listener, router).await?;

    Ok(())
}
