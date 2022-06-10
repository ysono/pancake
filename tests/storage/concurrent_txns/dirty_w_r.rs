use super::super::helpers::{
    etc::{sleep_async, sleep_sync},
    gen,
    one_stmt::{OneStmtDbAdaptor, OneStmtSsiDbAdaptor},
};
use anyhow::{anyhow, Result};
use pancake::storage::engine_ssi::{ClientCommitDecision, Txn, DB};
use pancake::storage::serde::Datum;
use pancake::storage::types::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::task::JoinHandle;

pub async fn no_dirty_write(db: &'static DB) -> Result<()> {
    let w_txns_ct = 20;
    let w_abort_txns_ct = 20;
    let objs_ct = 15;
    let mut tasks = vec![];

    let gen_pk_str = |obj_i: u64| format!("dirty_write_test:item{}", obj_i);
    let gen_pv_str = |txn_i: u64| format!("from txn {}", txn_i);
    let parse_pv_str = |s: &str| s["from txn ".len()..].parse::<u64>();

    /* Each writing txn puts multiple objs. */
    for txn_i in 0..w_txns_ct {
        let task_fut = async move {
            sleep_async(1).await;

            let mut entries = BTreeMap::new();
            let pv = Arc::new(gen::gen_str_pv(gen_pv_str(txn_i)));
            for obj_i in 0..objs_ct {
                let pk = Arc::new(gen::gen_str_pk(gen_pk_str(obj_i)));
                entries.insert(pk, pv.clone());
            }

            let txn_fut = Txn::run(db, |txn| {
                for (pk, pv) in entries.iter() {
                    txn.put(&pk, &Some(pv.clone()))?;
                }
                Ok(ClientCommitDecision::Commit(()))
            });
            txn_fut.await
        };
        let task: JoinHandle<Result<()>> = tokio::spawn(task_fut);
        tasks.push(task);
    }

    /* Each writing txn puts two PKs, then aborts. */
    for txn_i in w_txns_ct..(w_txns_ct + w_abort_txns_ct) {
        let task_fut = async move {
            sleep_async(1).await;

            let mut entries = BTreeMap::new();
            let pv = Arc::new(gen::gen_str_pv(gen_pv_str(txn_i)));
            for obj_i in 0..objs_ct {
                let pk = Arc::new(gen::gen_str_pk(gen_pk_str(obj_i)));
                entries.insert(pk, pv.clone());
            }

            let txn_fut = Txn::run(db, |txn| {
                for (pk, pv) in entries.iter() {
                    txn.put(&pk, &Some(pv.clone()))?;
                }
                Ok(ClientCommitDecision::Abort(()))
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

    /* Collect entries. */
    let db_adap = OneStmtSsiDbAdaptor { db: &db };
    let mut gotten_entries = BTreeMap::new();
    for obj_i in 0..objs_ct {
        let pk = gen::gen_str_pk(gen_pk_str(obj_i));
        let (_pk, opt_pv) = db_adap.get_pk_one(&pk).await?.unwrap();
        gotten_entries.insert(pk, opt_pv);
    }
    /* Assert that all PVs are equal. */
    assert_eq!(gotten_entries.len(), objs_ct as usize);
    let pv_0 = gotten_entries.get(&gen::gen_str_pk(gen_pk_str(0))).unwrap();
    for obj_i in 1..objs_ct {
        let pv_i = gotten_entries
            .get(&gen::gen_str_pk(gen_pk_str(obj_i)))
            .unwrap();
        assert_eq!(pv_i, pv_0);
    }
    /* Assert that the PV is from a non-aborted txn. */
    if let Value(Datum::Str(s)) = pv_0.as_ref() {
        let txn_i = parse_pv_str(s)?;
        assert!(txn_i < w_txns_ct);
    } else {
        panic!("We put a Str-typed PV but got non-Str-typed.")
    }

    Ok(())
}

pub async fn no_dirty_read(db: &'static DB) -> Result<()> {
    let w_txns_ct = 20;
    let w_abort_txns_ct = 20;
    let r_txns_ct = 20;
    let objs_ct = 15;
    let stagger_ms = 10;
    let mut w_tasks = vec![];
    let mut r_tasks = vec![];

    let gen_pk_str = |obj_i: u64| format!("dirty_read_test:item{}", obj_i);
    let gen_pv_str = |txn_i: u64| format!("from txn {}", txn_i);
    let parse_pv_str = |s: &str| s["from txn ".len()..].parse::<u64>();

    /*
    Launch writing txns by staggered delays.
    Each writing txn puts multiple objs.
    */
    for txn_i in 0..w_txns_ct {
        let task_fut = async move {
            sleep_async(stagger_ms * txn_i).await;

            let mut entries = BTreeMap::new();
            let pv = Arc::new(gen::gen_str_pv(gen_pv_str(txn_i)));
            for obj_i in 0..objs_ct {
                let pk = Arc::new(gen::gen_str_pk(gen_pk_str(obj_i)));
                entries.insert(pk, pv.clone());
            }

            let txn_fut = Txn::run(db, |txn| {
                for (pk, pv) in entries.iter() {
                    txn.put(&pk, &Some(pv.clone()))?;
                }
                Ok(ClientCommitDecision::Commit(()))
            });
            txn_fut.await
        };
        let task: JoinHandle<Result<()>> = tokio::spawn(task_fut);
        w_tasks.push(task);
    }

    /*
    Launch aborting txns by staggered delays.
    Each writing txn puts multiple objs, then aborst.
    */
    for txn_i in w_txns_ct..(w_txns_ct + w_abort_txns_ct) {
        let task_fut = async move {
            sleep_async(stagger_ms * txn_i).await;

            let mut entries = BTreeMap::new();
            let pv = Arc::new(gen::gen_str_pv(gen_pv_str(txn_i)));
            for obj_i in 0..objs_ct {
                let pk = Arc::new(gen::gen_str_pk(gen_pk_str(obj_i)));
                entries.insert(pk, pv.clone());
            }

            let txn_fut = Txn::run(db, |txn| {
                for (pk, pv) in entries.iter() {
                    txn.put(&pk, &Some(pv.clone()))?;
                }
                Ok(ClientCommitDecision::Abort(()))
            });
            txn_fut.await
        };
        let task: JoinHandle<Result<()>> = tokio::spawn(task_fut);
        w_tasks.push(task);
    }

    /*
    Launch reading txns by staggered delays.
    Each reading txn reads all objs, and verifies that all PVs are equal.
    */
    for txn_i in 0..r_txns_ct {
        let task_fut = async move {
            sleep_async(stagger_ms * txn_i).await;

            let pks = (0..objs_ct)
                .map(|obj_i| Arc::new(gen::gen_str_pk(gen_pk_str(obj_i))))
                .collect::<Vec<_>>();

            let txn_fut = Txn::run(db, |txn| {
                let mut res_opt_pvs = pks.iter().map(|pk| txn.get_pk_one(pk));

                let first_opt_pkpv = res_opt_pvs.next().unwrap()?;
                let first_opt_pv = first_opt_pkpv.map(|(_, pv)| pv);

                for res_opt_pv in res_opt_pvs {
                    let curr_opt_pv = res_opt_pv?.map(|(_, pv)| pv);
                    /* Assert that reading is from a snapshot. (We're testing beyond dirty read.) */
                    if first_opt_pv != curr_opt_pv {
                        return Err(anyhow!(
                            "Read unequal Opt<PV>s: {:?} {:?}",
                            first_opt_pv,
                            curr_opt_pv
                        ));
                    }

                    /* Sleep to ensure that the whole reading txn is concurrent with at least one writing txn. */
                    sleep_sync(stagger_ms * 2 / objs_ct);
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
    /* Assert that all PVs are from non-aborted txns. */
    for opt_pv in read_opt_pvs.iter() {
        if let Some(pv) = opt_pv {
            if let Value(Datum::Str(s)) = pv.as_ref() {
                let txn_i = parse_pv_str(s)?;
                assert!(txn_i < w_txns_ct);
            } else {
                panic!("We put a Str-typed PV but got non-Str-typed.");
            }
        }
    }

    Ok(())
}
