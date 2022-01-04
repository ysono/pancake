use super::super::helpers::{
    etc::{coerce_ref_to_static, sleep},
    gen,
    one_stmt::OneStmtSsiDbAdaptor,
};
use anyhow::{anyhow, Result};
use pancake::storage::engine_ssi::oper::txn::{CloseResult, Txn};
use pancake::storage::engine_ssi::DB;
use pancake::storage::types::PVShared;
use std::sync::Arc;
use tokio::task::JoinHandle;

pub async fn repeatable_read(db: &'static DB) -> Result<()> {
    let db_adap = Arc::new(OneStmtSsiDbAdaptor { db });

    static PK_STR: &str = "the_key";

    let w_txns_ct = 50;
    let r_txns_ct = 5;
    let stagger_ms = 10;

    /*
    Launch all writing txns now.
    Each writing txn starts after a staggered delay, then overwrites the same PK.
    */
    let mut w_tasks = vec![];
    for w_txn_i in 0..w_txns_ct {
        let db_adap = Arc::clone(&db_adap);
        let txn_fut = async move {
            sleep(w_txn_i * stagger_ms).await;

            let pk = Arc::new(gen::gen_str_pk(PK_STR));
            let pv = Arc::new(gen::gen_str_pv(format!("{}", w_txn_i)));
            db_adap.nonmut_put(pk, Some(pv)).await?;

            Ok(())
        };
        let task: JoinHandle<Result<()>> = tokio::spawn(txn_fut);
        w_tasks.push(task);
    }

    /*
    Launch reading txns by staggered delays.
    Each reading txn reads the same PK repeatedly,
        and asserts that received values are all equal.
    */
    let pk = gen::gen_str_pk(PK_STR);
    let pk_ref = unsafe { coerce_ref_to_static(&pk) };
    let mut r_tasks = vec![];
    for _ in 0..r_txns_ct {
        let txn_fut = Txn::run(db, move |mut txn| {
            Box::pin(async move {
                let res: Result<()> = async {
                    let mut prev_pv: Option<Option<PVShared>> = None;

                    for _ in 0..w_txns_ct {
                        let curr_pv: Option<PVShared> =
                            txn.get_pk_one(pk_ref).await?.map(|(_pk, pv)| pv);
                        match &prev_pv {
                            None => prev_pv = Some(curr_pv),
                            Some(prev_pv) if prev_pv != &curr_pv => {
                                /* Don't `assert!()` here. Don't panic. */
                                return Err(anyhow!(
                                    "Non-repeatable read! {:?} {:?}",
                                    prev_pv,
                                    curr_pv
                                ));
                            }
                            _ => (),
                        }

                        sleep(stagger_ms).await;
                    }

                    Ok(())
                }
                .await;

                txn.close(res).await
            })
        });
        let task: JoinHandle<CloseResult<()>> = tokio::spawn(txn_fut);
        r_tasks.push(task);

        sleep(stagger_ms).await;
    }

    for task in w_tasks.into_iter() {
        let res: Result<()> = task.await?;
        res?;
    }
    for task in r_tasks.into_iter() {
        let res: CloseResult<()> = task.await?;
        let res: Result<()> = res.into();
        res?;
    }

    Ok(())
}
