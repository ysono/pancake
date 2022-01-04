use super::super::helpers::{
    etc::{coerce_ref_to_static, sleep},
    gen,
    one_stmt::{OneStmtDbAdaptor, OneStmtSsiDbAdaptor},
};
use anyhow::Result;
use pancake::storage::engine_ssi::oper::txn::{CloseResult, CommitResult, Txn};
use pancake::storage::engine_ssi::DB;
use pancake::storage::serde::Datum;
use pancake::storage::types::Value;
use std::sync::Arc;
use tokio::task::JoinHandle;

pub async fn no_lost_update(db: &'static DB) -> Result<()> {
    static PK_STR: &'static str = "the_counter";

    static INIT_VAL: i64 = 300;

    let w_txns_ct = 50;

    let mut tasks = vec![];

    /* Each txn increments the counter by exactly 1. */
    let pk = Arc::new(gen::gen_str_pk(PK_STR));
    let pk_ref = unsafe { coerce_ref_to_static(&pk) };
    for _ in 0..w_txns_ct {
        let pk = Arc::clone(&pk);
        let txn_fut = Txn::run(db, move |mut txn| {
            Box::pin(async move {
                sleep(1).await;

                let res: Result<()> = async {
                    loop {
                        let next_int: i64 = match txn.get_pk_one(pk_ref).await? {
                            Some((_pk, pv)) => {
                                if let Value(Datum::I64(prev)) = pv.as_ref() {
                                    prev + 1
                                } else {
                                    INIT_VAL
                                }
                            }
                            _ => INIT_VAL,
                        };

                        let next_pv = Arc::new(Value(Datum::I64(next_int)));
                        txn.put(Arc::clone(&pk), Some(next_pv)).await?;

                        sleep(1).await;

                        match txn.try_commit().await? {
                            CommitResult::Conflict => txn.clear().await?,
                            CommitResult::Success => break,
                        }
                    }
                    Ok(())
                }
                .await;

                txn.close(res).await
            })
        });
        let task: JoinHandle<CloseResult<()>> = tokio::spawn(txn_fut);
        tasks.push(task);
    }

    for task in tasks.into_iter() {
        let res: CloseResult<()> = task.await?;
        let res: Result<()> = res.into();
        res?;
    }

    /* Check the ending condition. */
    let db_adap = OneStmtSsiDbAdaptor { db };
    let pv = db_adap.get_pk_one(&pk).await?.map(|(_pk, pv)| pv);
    let exp_int = INIT_VAL + w_txns_ct - 1;
    assert_eq!(Some(Arc::new(Value(Datum::I64(exp_int)))), pv);

    Ok(())
}
