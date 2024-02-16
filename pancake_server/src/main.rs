use anyhow::Result;
use pancake_engine_serial::DB as SerialDb;
use pancake_engine_ssi::DB as SsiDb;
use pancake_server::{
    http, wasm::engine_serial::WasmEngine as SerialWasmEng,
    wasm::engine_ssi::WasmEngine as SsiWasmEng,
};
use std::env;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::oneshot;

const ENV_VAR_PARENT_DIR: &str = "PANCAKE_PARENT_DIR";

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let mut signal = signal(SignalKind::interrupt())?;

    let parent_dir = env::var(ENV_VAR_PARENT_DIR)
        .map_or_else(|_| env::temp_dir().join("pancake"), |s| PathBuf::from(s));

    let serial_db_dir = parent_dir.join("serial");
    let ssi_db_dir = parent_dir.join("ssi");

    let serial_db = SerialDb::load_or_new(serial_db_dir)?;
    let serial_db = Arc::new(RwLock::new(serial_db));

    let serial_wasm_engine = SerialWasmEng::new(Arc::clone(&serial_db))?;

    let (ssi_db, ssi_fc_worker) = SsiDb::load_or_new(ssi_db_dir)?;
    let ssi_fc_task = tokio::spawn(ssi_fc_worker.run());

    let ssi_wasm_engine = SsiWasmEng::new(Arc::clone(&ssi_db))?;

    let (frontend_terminate_tx, frontend_terminate_rx) = oneshot::channel::<()>();
    let frontend_fut = http::main(
        serial_db,
        serial_wasm_engine,
        Arc::clone(&ssi_db),
        ssi_wasm_engine,
        frontend_terminate_rx,
    );
    let frontend_task = tokio::spawn(frontend_fut);

    signal.recv().await;

    frontend_terminate_tx.send(()).ok();
    ssi_db.terminate().await;

    frontend_task.await?;
    let ssi_fc_res = ssi_fc_task.await?;
    ssi_fc_res?;

    Ok(())
}
