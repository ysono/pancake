use super::super::helpers::{
    etc::{coerce_ref_to_static, sleep},
    gen,
    one_stmt::{OneStmtDbAdaptor, OneStmtSsiDbAdaptor},
};
use anyhow::Result;
use pancake::storage::engine_ssi::oper::txn::{CloseResult, CommitResult, Txn};
use pancake::storage::engine_ssi::DB;
use pancake::storage::serde::{Datum, DatumType};
use pancake::storage::types::{PrimaryKey, SubValueSpec, Value};
use std::sync::Arc;
use tokio::task::JoinHandle;

fn gen_pk(doctor_id: usize) -> PrimaryKey {
    gen::gen_str_pk(format!("doctor.{}", doctor_id))
}
fn gen_pv(is_on_call: bool) -> Value {
    Value(Datum::Bytes(vec![is_on_call as u8]))
}
fn gen_spec() -> SubValueSpec {
    SubValueSpec::from(DatumType::Bytes)
}

fn pk_is_doctor(pk: &PrimaryKey) -> bool {
    if let PrimaryKey(Datum::Str(s)) = pk {
        return s.starts_with("doctor.");
    }
    false
}
fn pv_is_on_call(pv: &Value) -> bool {
    if let Value(Datum::Bytes(bytes)) = pv {
        if bytes.len() == 1 {
            return bytes[0] != 0;
        }
    }
    false
}

pub async fn no_write_skew(db: &'static DB) -> Result<()> {
    let db_adap = OneStmtSsiDbAdaptor { db };

    let spec = Arc::new(gen_spec());

    db_adap.nonmut_create_scnd_idx(spec.clone()).await?;

    let tot_doctors_ct = 15usize;
    let oncall_doctors_thresh = 5usize;

    /* Put all doctors as being on-call. */
    for uniq_id in 0..tot_doctors_ct {
        let pk = Arc::new(gen_pk(uniq_id));
        let pv = Arc::new(gen_pv(true));
        db_adap.nonmut_put(pk, Some(pv)).await?;
    }

    /* Check the initial condition: all doctors are on-call. */
    let beginning_oncall_ct = db_adap
        .get_sv_range(&spec, None, None)
        .await?
        .into_iter()
        .filter(|(pk, pv)| pk_is_doctor(pk) && pv_is_on_call(pv))
        .count();
    assert_eq!(tot_doctors_ct, beginning_oncall_ct);

    /* For each `uniq_id`, attempt to take the corresponding doctor off-call. */
    let mut tasks = vec![];
    let spec_ref = unsafe { coerce_ref_to_static(&spec) };
    for uniq_id in 0..tot_doctors_ct {
        let txn_fut = Txn::run(db, move |mut txn| {
            Box::pin(async move {
                sleep(1).await;

                let res: Result<()> = async {
                    let pk = Arc::new(gen_pk(uniq_id));
                    let pv = Arc::new(gen_pv(false));
                    txn.put(pk, Some(pv)).await?;

                    loop {
                        let oncall_ct: Result<usize> = txn
                            .get_sv_range(spec_ref, None, None, |entries| -> Result<usize> {
                                let mut oncall_ct = 0;
                                for entry in entries {
                                    let (svpk, pv) = entry.try_borrow()?;
                                    if pk_is_doctor(&svpk.pk) && pv_is_on_call(pv) {
                                        oncall_ct += 1;
                                    }
                                }
                                Ok(oncall_ct)
                            })
                            .await;
                        let oncall_ct = oncall_ct?;
                        /* This `oncall_ct` discounts the curr doctor,
                        who has been taken off-call in the view of the curr txn's snapshot. */

                        if oncall_ct >= oncall_doctors_thresh {
                            sleep(1).await;
                            match txn.try_commit().await? {
                                CommitResult::Conflict => (),
                                CommitResult::Success => break,
                            }
                        } else {
                            /* Already too low. Give up; do not take curr doctor off call. */
                            break;
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

    /*
    Check the ending condition: Since each doctor greedily tries to
        go off-call, there are exactly `thresh` remaining on-call.
    */
    let final_oncall_ct = db_adap
        .get_sv_range(&spec, None, None)
        .await?
        .into_iter()
        .filter(|(pk, pv)| pk_is_doctor(pk) && pv_is_on_call(pv))
        .count();
    assert_eq!(oncall_doctors_thresh, final_oncall_ct);

    Ok(())
}
