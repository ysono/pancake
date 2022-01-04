use anyhow::Result;
use pancake::storage::engine_serial::db::DB as SerialDb;
use pancake::storage::engine_ssi::DB as SsiDb;
use std::env;
use std::fs;
use std::sync::atomic::Ordering;

mod storage;
use storage::concurrent_txns::test_concurrent_txns;
use storage::helpers::one_stmt::{OneStmtSerialDbAdaptor, OneStmtSsiDbAdaptor};
use storage::individual_stmts::test_stmts_serially;

#[tokio::test]
async fn test_main() -> Result<()> {
    let parent_dir = env::temp_dir().join("pancake");
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

    let (ssi_db, ssi_gc_job_fut) = SsiDb::load_db_and_gc_job(ssi_db_dir)?;
    let ssi_gc_task = tokio::spawn(ssi_gc_job_fut);
    let mut ssi_db_adap = OneStmtSsiDbAdaptor { db: &ssi_db };

    test_stmts_serially(&mut serial_db_adap).await?;
    test_stmts_serially(&mut ssi_db_adap).await?;

    test_concurrent_txns(&ssi_db).await?;

    ssi_db.is_terminating().store(true, Ordering::SeqCst);
    ssi_db.send_job_cv();
    ssi_gc_task.await??;

    Ok(())
}
