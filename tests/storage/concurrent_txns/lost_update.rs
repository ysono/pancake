use super::super::helpers::{
    gen,
    one_stmt::{OneStmtDbAdaptor, OneStmtSsiDbAdaptor},
};
use anyhow::Result;
use pancake::storage::engine_ssi::{ClientCommitDecision, Txn, DB};
use pancake::storage::serde::Datum;
use pancake::storage::types::Value;
use std::sync::Arc;
use tokio::task::JoinHandle;

pub async fn no_lost_update(db: &'static DB) -> Result<()> {
    let w_txns_ct = 50;
    let mut tasks = vec![];

    let pk = Arc::new(gen::gen_str_pk("the_counter_key"));
    let init_val: i64 = 300;

    /* Each txn increments the counter by exactly 1. */
    for _ in 0..w_txns_ct {
        let pk = Arc::clone(&pk);
        let task_fut = async move {
            let txn_fut = Txn::run(db, |txn| {
                let prior_pkpv = txn.get_pk_one(&pk)?;
                let next_val = match prior_pkpv.as_ref() {
                    Some((_, pv)) => match pv.as_ref() {
                        Value(Datum::I64(prior_val)) => prior_val + 1,
                        _ => init_val,
                    },
                    _ => init_val,
                };
                let next_pv = Arc::new(Value(Datum::I64(next_val)));
                txn.put(&pk, &Some(next_pv))?;
                Ok(ClientCommitDecision::Commit(()))
            });
            txn_fut.await
        };
        let task: JoinHandle<Result<()>> = tokio::spawn(task_fut);
        tasks.push(task);
    }

    for task in tasks.into_iter() {
        let res: Result<Result<()>, _> = task.await;
        res??;
    }

    /* Check the ending condition. */
    let db_adap = OneStmtSsiDbAdaptor { db };
    let pv = db_adap.get_pk_one(&pk).await?.map(|(_pk, pv)| pv);
    let exp_int = init_val + w_txns_ct - 1;
    assert_eq!(Some(Arc::new(Value(Datum::I64(exp_int)))), pv);

    Ok(())
}
