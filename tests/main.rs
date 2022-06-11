use anyhow::Result;
use pancake::storage::engine_serial::db::DB as SerialDb;
use pancake::storage::engine_ssi::DB as SsiDb;
use std::env;
use std::fs;
use std::path::PathBuf;

mod storage;
use storage::concurrent_txns::test_concurrent_txns;
use storage::helpers::one_stmt::{OneStmtSerialDbAdaptor, OneStmtSsiDbAdaptor};
use storage::individual_stmts::test_stmts_serially;

const ENV_VAR_PARENT_DIR: &str = "PANCAKE_PARENT_DIR";

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_main() -> Result<()> {
    let parent_dir = env::var(ENV_VAR_PARENT_DIR)
        .map_or_else(|_| env::temp_dir().join("pancake"), |s| PathBuf::from(s));
    let serial_db_dir = parent_dir.join("serial");
    let ssi_db_dir = parent_dir.join("ssi");
    if parent_dir.exists() {
        /* Don't remove the dir itself, so that symbolic links remain valid.
        This is for tester's convenience only.*/
        for sub in fs::read_dir(&parent_dir)? {
            let sub = sub?.path();
            let meta = fs::metadata(&sub)?;
            if meta.is_file() {
                fs::remove_file(sub)?;
            } else {
                fs::remove_dir_all(sub)?;
            }
        }
    }

    let mut serial_db = SerialDb::load_or_new(serial_db_dir)?;
    let mut serial_db_adap = OneStmtSerialDbAdaptor { db: &mut serial_db };

    let (ssi_db, ssi_fc_job, ssi_sicr_job) = SsiDb::load_or_new(ssi_db_dir)?;
    let ssi_fc_task = tokio::spawn(ssi_fc_job.run());
    let ssi_sicr_task = tokio::spawn(ssi_sicr_job.run());
    let mut ssi_db_adap = OneStmtSsiDbAdaptor { db: &ssi_db };

    test_stmts_serially(&mut serial_db_adap).await?;
    test_stmts_serially(&mut ssi_db_adap).await?;

    test_concurrent_txns(&ssi_db).await?;

    ssi_db.terminate().await;
    ssi_fc_task.await??;
    ssi_sicr_task.await??;

    Ok(())
}
