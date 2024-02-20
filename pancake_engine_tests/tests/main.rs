use anyhow::Result;
use pancake_engine_common::fs_utils::{self, EngineType};
use pancake_engine_serial::DB as SerialDb;
use pancake_engine_ssi::DB as SsiDb;
use std::fs;

mod storage;
use storage::concurrent_txns::test_concurrent_txns;
use storage::helpers::one_stmt::{OneStmtSerialDbAdaptor, OneStmtSsiDbAdaptor};
use storage::individual_stmts::test_stmts_serially;

#[tokio::test()]
async fn integration_test_serial() -> Result<()> {
    let db_root_dir = fs_utils::default_db_root_dir(EngineType::SERIAL);
    if db_root_dir.exists() {
        fs::remove_dir_all(&db_root_dir)?;
    }

    let mut db = SerialDb::load_or_new(&db_root_dir)?;
    let mut db_adap = OneStmtSerialDbAdaptor { db: &mut db };

    test_stmts_serially(&mut db_adap).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_ssi() -> Result<()> {
    let db_root_dir = fs_utils::default_db_root_dir(EngineType::SSI);
    if db_root_dir.exists() {
        fs::remove_dir_all(&db_root_dir)?;
    }

    let (db, fc_worker) = SsiDb::load_or_new(db_root_dir)?;
    let fc_task = tokio::spawn(fc_worker.run());
    let mut db_adap = OneStmtSsiDbAdaptor { db: &db };

    test_stmts_serially(&mut db_adap).await?;

    test_concurrent_txns(&db).await?;

    db.terminate().await;

    fc_task.await??;

    Ok(())
}
