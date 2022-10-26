use super::super::helpers::{
    etc::{join_tasks, sleep_async, sleep_sync},
    gen,
    one_stmt::{OneStmtDbAdaptor, OneStmtSsiDbAdaptor},
};
use anyhow::{anyhow, Result};
use pancake_engine_ssi::{ClientCommitDecision, Txn, DB};
use pancake_types::serde::Datum;
use pancake_types::types::Value;
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

    /* Each writing txn puts multiple objs, then commits. */
    for txn_i in 0..w_txns_ct {
        let task_fut = async move {
            sleep_async(1).await;

            let retry_limit = (w_txns_ct - 1) as usize;

            let mut entries = BTreeMap::new();
            let pv = Arc::new(gen::gen_str_pv(gen_pv_str(txn_i)));
            for obj_i in 0..objs_ct {
                let pk = Arc::new(gen::gen_str_pk(gen_pk_str(obj_i)));
                entries.insert(pk, pv.clone());
            }

            let txn_fut = Txn::run(db, retry_limit, |txn| {
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

    /* Each writing txn puts multiple objs, then aborts. */
    for txn_i in w_txns_ct..(w_txns_ct + w_abort_txns_ct) {
        let task_fut = async move {
            sleep_async(1).await;

            let mut entries = BTreeMap::new();
            let pv = Arc::new(gen::gen_str_pv(gen_pv_str(txn_i)));
            for obj_i in 0..objs_ct {
                let pk = Arc::new(gen::gen_str_pk(gen_pk_str(obj_i)));
                entries.insert(pk, pv.clone());
            }

            let txn_fut = Txn::run(db, 0, |txn| {
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

    join_tasks(tasks).await?;

    /* Collect entries. */
    let db_adap = OneStmtSsiDbAdaptor { db };
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
    Each writing txn puts multiple objs, then commits.
    */
    for txn_i in 0..w_txns_ct {
        let task_fut = async move {
            sleep_async(stagger_ms * txn_i).await;

            let retry_limit = (w_txns_ct - 1) as usize;

            let mut entries = BTreeMap::new();
            let pv = Arc::new(gen::gen_str_pv(gen_pv_str(txn_i)));
            for obj_i in 0..objs_ct {
                let pk = Arc::new(gen::gen_str_pk(gen_pk_str(obj_i)));
                entries.insert(pk, pv.clone());
            }

            let txn_fut = Txn::run(db, retry_limit, |txn| {
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
    Each writing txn puts multiple objs, then aborts.
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

            let txn_fut = Txn::run(db, 0, |txn| {
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

            /* Sleep to ensure that the whole reading txn is concurrent with at least one writing txn. */
            let txn_dur = stagger_ms * 2;
            let sleep_dur = txn_dur / objs_ct;

            let pks = (0..objs_ct)
                .map(|obj_i| Arc::new(gen::gen_str_pk(gen_pk_str(obj_i))))
                .collect::<Vec<_>>();

            let txn_fut = Txn::run(db, 0, |txn| {
                let mut res_opt_pvs = pks.iter().map(|pk| txn.get_pk_one(pk));

                let first_opt_pkpv = res_opt_pvs.next().unwrap()?;
                let first_opt_pv = first_opt_pkpv.map(|(_, pv)| pv);

                for res_opt_pv in res_opt_pvs {
                    let curr_opt_pv = res_opt_pv?.map(|(_, pv)| pv);
                    /* Assert that all PVs are equal.
                    (We're asserting beyond Read Committed. Are we asserting MAV or Snapshot?) */
                    if first_opt_pv != curr_opt_pv {
                        return Err(anyhow!(
                            "Read unequal Opt<PV>s: {:?} {:?}",
                            first_opt_pv,
                            curr_opt_pv
                        ));
                    }

                    sleep_sync(sleep_dur);
                }

                Ok(ClientCommitDecision::Commit(first_opt_pv))
            });
            txn_fut.await
        };
        let task: JoinHandle<Result<Option<Arc<Value>>>> = tokio::spawn(task_fut);
        r_tasks.push(task);
    }

    let w_res = join_tasks(w_tasks).await;
    let r_res = join_tasks(r_tasks).await;

    w_res?;
    let read_opt_pvs = r_res?;

    let did_r_txns_see_diff_snaps = read_opt_pvs
        .iter()
        .skip(1)
        .any(|opt_pv| opt_pv != &read_opt_pvs[0]);
    if !did_r_txns_see_diff_snaps {
        eprintln!("Test should be set up s.t. reading txns see different snapshots.");
    }
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
