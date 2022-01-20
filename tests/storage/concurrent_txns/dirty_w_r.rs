use super::super::helpers::{
    etc::{coerce_ref_to_static, sleep},
    gen,
    one_stmt::{OneStmtDbAdaptor, OneStmtSsiDbAdaptor},
};
use anyhow::{anyhow, Result};
use pancake::storage::engine_ssi::oper::txn::{CloseResult, CommitResult, Txn};
use pancake::storage::engine_ssi::DB;
use pancake::storage::types::PVShared;
use std::sync::Arc;
use tokio::task::JoinHandle;

static OWNERS_PK_STR: &'static str = "owners::merch_x";
static INVOICES_PK_STR: &'static str = "invoices::merch_x";

pub async fn no_dirty_write(db: &'static DB) -> Result<()> {
    let w_txns_ct = 20;
    let mut tasks = vec![];

    /*
    Each writing txn puts two PKs, assigning both the same PV that is unique to the txn.
    */
    for uniq_i in 0..w_txns_ct {
        let txn_fut = Txn::run(db, move |mut txn| {
            Box::pin(async move {
                sleep(1).await;

                let res: Result<()> = async {
                    let owners_pk = Arc::new(gen::gen_str_pk(OWNERS_PK_STR));
                    let owners_pv = Arc::new(gen::gen_str_pv(format!("cust_{}", uniq_i)));
                    txn.put(owners_pk, Some(owners_pv)).await?;

                    let invoices_pk = Arc::new(gen::gen_str_pk(INVOICES_PK_STR));
                    let invoices_pv = Arc::new(gen::gen_str_pv(format!("cust_{}", uniq_i)));
                    txn.put(invoices_pk, Some(invoices_pv)).await?;

                    loop {
                        sleep(1).await;

                        match txn.try_commit().await? {
                            CommitResult::Conflict => (),
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

    /*
    Each writing txn puts two PKs, then aborts.
    */
    static CUST_ABORTED: &str = "cust_aborted";
    for _ in 0..w_txns_ct {
        let txn_fut = Txn::run(db, move |mut txn| {
            Box::pin(async move {
                sleep(1).await;

                let res: Result<()> = async {
                    let pv = Arc::new(gen::gen_str_pv(CUST_ABORTED));

                    let owners_pk = Arc::new(gen::gen_str_pk(OWNERS_PK_STR));
                    txn.put(owners_pk, Some(Arc::clone(&pv))).await?;

                    let invoices_pk = Arc::new(gen::gen_str_pk(INVOICES_PK_STR));
                    txn.put(invoices_pk, Some(pv)).await?;

                    /* Do not commit here. Do nothing. */

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

    /* Assert that the two PKs map to the same PV. */
    let db_adap = OneStmtSsiDbAdaptor { db: &db };
    let owner: Option<PVShared> = db_adap
        .get_pk_one(&gen::gen_str_pk(OWNERS_PK_STR))
        .await?
        .map(|(_pk, pv)| pv);
    let invoice: Option<PVShared> = db_adap
        .get_pk_one(&gen::gen_str_pk(INVOICES_PK_STR))
        .await?
        .map(|(_pk, pv)| pv);
    assert_eq!(owner, invoice);

    /* Assert that aborting txns did not leave any durable change. */
    assert_ne!(Arc::new(gen::gen_str_pv(CUST_ABORTED)), owner.unwrap());

    Ok(())
}

pub async fn no_dirty_read(db: &'static DB) -> Result<()> {
    let w_txns_ct = 50;
    let r_txns_ct = 50;
    let stagger_ms = 10;
    let mut tasks = vec![];

    /*
    Launch all writing txns now.
    Each writing txn starts after a staggered delay,
        then puts two PKs, assigning them the same PV that is unique to the txn.
    */
    for uniq_i in 0..w_txns_ct {
        let txn_fut = Txn::run(db, move |mut txn| {
            Box::pin(async move {
                sleep(uniq_i * stagger_ms).await;

                let owners_pk = Arc::new(gen::gen_str_pk(OWNERS_PK_STR));
                let invoices_pk = Arc::new(gen::gen_str_pk(INVOICES_PK_STR));

                let res: Result<()> = async {
                    let owners_pk = Arc::clone(&owners_pk);
                    let owners_pv = Arc::new(gen::gen_str_pv(format!("cust_{}", uniq_i)));
                    txn.put(owners_pk, Some(owners_pv)).await?;

                    let invoices_pk = Arc::clone(&invoices_pk);
                    let invoices_pv = Arc::new(gen::gen_str_pv(format!("cust_{}", uniq_i)));
                    txn.put(invoices_pk, Some(invoices_pv)).await?;

                    loop {
                        match txn.try_commit().await? {
                            CommitResult::Conflict => (),
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

    /*
    Launch reading txns by staggered delays.
    Each reading txn reads the two PKs,
        with a delay s.t. there is a `put` event in between,
        and verifies that the two PVs are equal.
    */
    let owners_pk = gen::gen_str_pk(OWNERS_PK_STR);
    let invoices_pk = gen::gen_str_pk(INVOICES_PK_STR);
    let owners_pk_ref = unsafe { coerce_ref_to_static(&owners_pk) };
    let invoices_pk_ref = unsafe { coerce_ref_to_static(&invoices_pk) };
    for _ in 0..r_txns_ct {
        let txn_fut = Txn::run(db, move |mut txn| {
            Box::pin(async move {
                let res: Result<()> = async {
                    let owners_pv: Option<PVShared> =
                        txn.get_pk_one(owners_pk_ref).await?.map(|(_pk, pv)| pv);

                    /* Mult by 2 to ensure at least one `put` happens concurrently. */
                    sleep(stagger_ms * 2).await;

                    let invoices_pv: Option<PVShared> =
                        txn.get_pk_one(invoices_pk_ref).await?.map(|(_pk, pv)| pv);

                    /* Keep this print, to visually make sure that the test delay params
                    are tuned s.t. each reading txn snapshots differently. */
                    println!("{:?} {:?}", owners_pv, invoices_pv);
                    /* Don't `assert!()` here. Don't panic. */
                    if owners_pv != invoices_pv {
                        return Err(anyhow!("{:?} {:?}", owners_pv, invoices_pv));
                    }

                    Ok(())
                }
                .await;

                txn.close(res).await
            })
        });
        let task: JoinHandle<CloseResult<()>> = tokio::spawn(txn_fut);
        tasks.push(task);

        sleep(stagger_ms).await;
    }

    for task in tasks.into_iter() {
        let res: CloseResult<()> = task.await?;
        let res: Result<()> = res.into();
        res?;
    }

    Ok(())
}
