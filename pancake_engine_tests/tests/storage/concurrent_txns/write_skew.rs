use super::super::helpers::{
    etc::{join_tasks, sleep_async},
    gen,
    one_stmt::{OneStmtDbAdaptor, OneStmtSsiDbAdaptor},
};
use anyhow::Result;
use pancake_engine_ssi::{ClientCommitDecision, Txn, DB};
use pancake_types::serde::{Datum, DatumType};
use pancake_types::types::{PrimaryKey, SubValueSpec, Value};
use std::sync::Arc;
use tokio::task::JoinHandle;

fn gen_pk(doctor_id: usize) -> PrimaryKey {
    gen::gen_str_pk(format!("doctor.{doctor_id}"))
}
fn gen_pv(is_on_call: bool) -> Value {
    Value(Datum::Bytes(vec![is_on_call as u8]))
}
fn gen_sv_spec() -> SubValueSpec {
    SubValueSpec::whole(DatumType::Bytes)
}

fn pk_is_doctor(pk: &PrimaryKey) -> bool {
    if let PrimaryKey(Datum::Str(s)) = pk {
        return s.starts_with("doctor.");
    }
    false
}
fn pv_is_on_call(pv: &Value) -> bool {
    if let Value(Datum::Bytes(bytes)) = pv {
        return bytes.len() == 1 && bytes[0] != 0;
    }
    false
}

pub async fn no_write_skew(db: &'static DB) -> Result<()> {
    let db_adap = OneStmtSsiDbAdaptor { db };

    let sv_spec = Arc::new(gen_sv_spec());

    db_adap.nonmut_create_scnd_idx(sv_spec.clone()).await?;

    let tot_doctors_ct = 15usize;
    let oncall_doctors_thresh = 5usize;

    /* Put all doctors as being on-call. */
    for doctor_id in 0..tot_doctors_ct {
        let pk = Arc::new(gen_pk(doctor_id));
        let pv = Arc::new(gen_pv(true));
        db_adap.nonmut_put(pk, Some(pv)).await?;
    }

    /* Check the initial condition: all doctors are on-call. */
    let beginning_oncall_ct = db_adap
        .get_sv_range(&sv_spec, None, None)
        .await?
        .into_iter()
        .filter(|(pk, pv)| pk_is_doctor(pk) && pv_is_on_call(pv))
        .count();
    assert_eq!(tot_doctors_ct, beginning_oncall_ct);

    /* Each txn attempts to take a different doctor off-call. */
    let mut tasks = vec![];
    for doctor_id in 0..tot_doctors_ct {
        let sv_spec = Arc::clone(&sv_spec);
        let retry_limit = tot_doctors_ct - oncall_doctors_thresh;

        let task_fut = async move {
            sleep_async(1).await;

            let pk = Arc::new(gen_pk(doctor_id));
            let pv = Arc::new(gen_pv(false));

            let txn_fut = Txn::run(db, retry_limit, |txn| {
                let mut on_call_count = 0;
                let entries = txn.get_sv_range(&sv_spec, None, None)?;
                for entry in entries {
                    let (svpk, pv) = entry.try_borrow()?;
                    if pk_is_doctor(&svpk.pk) && pv_is_on_call(&pv) {
                        on_call_count += 1;
                    }
                }

                if on_call_count <= oncall_doctors_thresh {
                    return Ok(ClientCommitDecision::Abort(()));
                }

                txn.put(&pk, &Some(pv.clone()))?;
                return Ok(ClientCommitDecision::Commit(()));
            });
            txn_fut.await
        };
        let task: JoinHandle<Result<()>> = tokio::spawn(task_fut);
        tasks.push(task);
    }
    join_tasks(tasks).await?;

    /*
    Check the ending condition: Since each doctor greedily tries to
        go off-call, there are exactly `thresh` remaining on-call.
    */
    let final_oncall_ct = db_adap
        .get_sv_range(&sv_spec, None, None)
        .await?
        .into_iter()
        .filter(|(pk, pv)| pk_is_doctor(pk) && pv_is_on_call(pv))
        .count();
    assert_eq!(oncall_doctors_thresh, final_oncall_ct);

    Ok(())
}
