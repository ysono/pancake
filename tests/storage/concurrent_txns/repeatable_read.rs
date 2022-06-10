use super::super::helpers::{
    etc::{sleep_async, sleep_sync},
    gen,
    one_stmt::OneStmtSsiDbAdaptor,
};
use anyhow::{anyhow, Result};
use pancake::storage::engine_ssi::{ClientCommitDecision, Txn, DB};
use pancake::storage::types::Value;
use std::sync::Arc;
use tokio::task::JoinHandle;

pub async fn repeatable_read(db: &'static DB) -> Result<()> {
    let db_adap = Arc::new(OneStmtSsiDbAdaptor { db });

    let w_txns_ct = 50;
    let r_txns_ct = 5;
    let stagger_ms = 10;
    let mut w_tasks = vec![];
    let mut r_tasks = vec![];

    let pk = Arc::new(gen::gen_str_pk("the_repeatable_key"));
    let gen_pv_str = |txn_i: u64| format!("from txn {}", txn_i);

    /*
    Launch all writing txns now.
    Each writing txn starts after a staggered delay, then overwrites the same PK.
    */
    for txn_i in 0..w_txns_ct {
        let db_adap = Arc::clone(&db_adap);
        let pk = Arc::clone(&pk);
        let task_fut = async move {
            sleep_async(stagger_ms * txn_i).await;

            let pv = Arc::new(gen::gen_str_pv(gen_pv_str(txn_i)));
            db_adap.nonmut_put(pk.clone(), Some(pv)).await?;

            Ok(())
        };
        let task: JoinHandle<Result<()>> = tokio::spawn(task_fut);
        w_tasks.push(task);
    }

    /*
    Launch reading txns by staggered delays.
    Each reading txn reads the same PK repeatedly, and asserts that received values are all equal.
    */
    for txn_i in 0..r_txns_ct {
        let pk = Arc::clone(&pk);
        let task_fut = async move {
            sleep_async(stagger_ms * txn_i).await;

            let txn_fut = Txn::run(db, |txn| {
                let first_opt_pv = txn.get_pk_one(&pk)?.map(|(_, pv)| pv);

                for _ in 0..w_txns_ct {
                    sleep_sync(stagger_ms);

                    let curr_opt_pv = txn.get_pk_one(&pk)?.map(|(_, pv)| pv);
                    if first_opt_pv != curr_opt_pv {
                        return Err(anyhow!(
                            "Non-repeatable read! {:?} {:?}",
                            first_opt_pv,
                            curr_opt_pv
                        ));
                    }
                }

                Ok(ClientCommitDecision::Commit(first_opt_pv))
            });
            txn_fut.await
        };
        let task: JoinHandle<Result<Option<Arc<Value>>>> = tokio::spawn(task_fut);
        r_tasks.push(task);
    }

    for task in w_tasks.into_iter() {
        let res: Result<Result<()>, _> = task.await;
        res??;
    }

    let mut read_opt_pvs = vec![];
    for task in r_tasks.into_iter() {
        let res: Result<Result<Option<Arc<Value>>>, _> = task.await;
        let opt_pv = res??;
        read_opt_pvs.push(opt_pv);
    }
    /* A sanitizing validation of the test setup.
    Assert that different reading txns saw different snapshots.
    This assertion technically succeeds non-deterministically. If failing, try again. */
    assert!(read_opt_pvs
        .iter()
        .skip(1)
        .any(|opt_pv| opt_pv != &read_opt_pvs[0]));

    Ok(())
}
