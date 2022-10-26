use super::super::helpers::{
    etc::{join_tasks, sleep_async, sleep_sync},
    gen,
};
use anyhow::{anyhow, Result};
use pancake_engine_ssi::{ClientCommitDecision, Txn, DB};
use pancake_types::types::Value;
use std::sync::Arc;
use tokio::task::JoinHandle;

pub async fn repeatable_read(db: &'static DB) -> Result<()> {
    let w_txns_ct = 20;
    let r_txns_ct = 5;
    let stagger_ms = 10;
    let mut w_tasks = vec![];
    let mut r_tasks = vec![];

    let pk = Arc::new(gen::gen_str_pk("the_repeatable_key"));
    let gen_pv_str = |txn_i: u64| format!("from txn {}", txn_i);

    /*
    Launch writing txns by staggered delays.
    Each writing txn puts the same PK.
    */
    for txn_i in 0..w_txns_ct {
        let pk = Arc::clone(&pk);
        let pv = Arc::new(gen::gen_str_pv(gen_pv_str(txn_i)));

        let task_fut = async move {
            sleep_async(stagger_ms * txn_i).await;

            let retry_limit = (w_txns_ct - 1) as usize;

            let txn_fut = Txn::run(db, retry_limit, |txn| {
                txn.put(&pk, &Some(pv.clone()))?;
                Ok(ClientCommitDecision::Commit(()))
            });
            txn_fut.await
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

            let txn_fut = Txn::run(db, 0, |txn| {
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

    Ok(())
}
