use anyhow::Result;
use pancake::frontend::http;
use pancake::storage::engine_serial::db::DB as SerialDb;
use pancake::storage::engine_ssi::DB as SsiDb;
use std::env;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::oneshot;

const ENV_VAR_PARENT_DIR: &str = "PANCAKE_PARENT_DIR";

#[tokio::main]
async fn main() -> Result<()> {
    let mut signal = signal(SignalKind::interrupt())?;

    let parent_dir = env::var(ENV_VAR_PARENT_DIR)
        .map_or_else(|_| env::temp_dir().join("pancake"), |s| PathBuf::from(s));

    let serial_db_dir = parent_dir.join("serial");
    let ssi_db_dir = parent_dir.join("ssi");

    let serial_db = SerialDb::load_or_new(serial_db_dir)?;
    let serial_db = Arc::new(RwLock::new(serial_db));

    let (ssi_db, ssi_gc_job_fut) = SsiDb::load_db_and_gc_job(ssi_db_dir)?;
    let ssi_gc_task = tokio::spawn(ssi_gc_job_fut);

    let (frontend_terminate_tx, frontend_terminate_rx) = oneshot::channel::<()>();
    let frontend_fut = http::main(serial_db, Arc::clone(&ssi_db), frontend_terminate_rx);
    let frontend_task = tokio::spawn(frontend_fut);

    signal.recv().await;

    frontend_terminate_tx.send(()).ok();
    ssi_db.is_terminating().store(true, Ordering::SeqCst);

    frontend_task.await?;
    ssi_db.send_job_cv();
    ssi_gc_task.await??;

    Ok(())
}
