use anyhow::Result;
use pancake_engine_common::fs_utils::{self, EngineType};
use pancake_engine_ssi::DB;
use pancake_server::{
    common::server,
    engine_ssi::{
        route_handlers::{self, AppState},
        wasm::WasmEngine,
    },
};
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::oneshot;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let mut signal = signal(SignalKind::interrupt())?;

    let engine_type = EngineType::SSI;

    let (db, fc_fut);
    {
        let root_dir = env::var(server::ENV_VAR_ROOT_DIR).map_or_else(
            |_| fs_utils::default_db_root_dir(engine_type),
            PathBuf::from,
        );

        let fc_worker;
        (db, fc_worker) = DB::load_or_new(root_dir)?;

        fc_fut = fc_worker.run();
    }

    let wasm_engine = WasmEngine::new(Arc::clone(&db))?;

    let (frontend_terminate_tx, frontend_terminate_rx) = oneshot::channel::<()>();

    let frontend_fut;
    {
        let state = AppState::new(Arc::clone(&db), wasm_engine);
        let state = Arc::new(state);

        let router = route_handlers::create_router(state);

        let bind_addr = env::var(server::ENV_VAR_BIND_ADDR)
            .unwrap_or_else(|_| server::default_bind_addr(engine_type).to_string());
        let listener = tokio::net::TcpListener::bind(bind_addr).await?;
        println!("Listening on {}", listener.local_addr()?);

        frontend_fut = async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    frontend_terminate_rx.await.ok();
                })
                .await
        };
    }

    let fc_task = tokio::spawn(fc_fut);
    let frontend_task = tokio::spawn(frontend_fut);
    println!("Launched all tasks.");

    signal.recv().await;
    println!("Received process signal.");

    frontend_terminate_tx.send(()).ok();
    db.terminate().await;
    println!("Notified termination to each task.");

    let frontend_join_res = frontend_task.await;
    let fc_join_res = fc_task.await;
    fc_join_res??;
    frontend_join_res??;

    Ok(())
}
