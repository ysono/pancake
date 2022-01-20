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
use rand::Rng;
use std::sync::Arc;
use tokio::task::JoinHandle;

fn gen_pk(item_id: usize) -> PrimaryKey {
    gen::gen_str_pk(format!("cart_item.{}", item_id))
}
fn gen_pv(price: i64) -> Value {
    Value(Datum::I64(price))
}
fn gen_spec() -> SubValueSpec {
    SubValueSpec::from(DatumType::I64)
}

fn pk_is_cart_item(pk: &PrimaryKey) -> bool {
    if let PrimaryKey(Datum::Str(s)) = pk {
        return s.starts_with("cart_item.");
    }
    false
}
fn extract_price(pv: &Value) -> Option<i64> {
    if let Value(Datum::I64(price)) = pv {
        return Some(*price);
    }
    None
}

pub async fn no_phantom_write(db: &'static DB) -> Result<()> {
    let db_adap = OneStmtSsiDbAdaptor { db };

    let spec = Arc::new(gen_spec());

    db_adap.nonmut_create_scnd_idx(spec.clone()).await?;

    /* Check the initial condition: cart is empty. */
    let beginning_cart_contents = db_adap
        .get_sv_range(&spec, None, None)
        .await?
        .into_iter()
        .filter(|(pk, _pv)| pk_is_cart_item(&pk))
        .count();
    assert_eq!(0, beginning_cart_contents);

    /* test params */
    let tot_price_thresh = 100;
    let inserts_ct = 200;
    let content_price_range = 5..10;

    /* In each txn, attempt to insert a content to the cart. */
    let mut tasks = vec![];
    let spec_ref = unsafe { coerce_ref_to_static(&spec) };
    for uniq_id in 0..inserts_ct {
        let price = rand::thread_rng().gen_range(content_price_range.clone());
        let txn_fut = Txn::run(db, move |mut txn| {
            Box::pin(async move {
                sleep(1).await;

                let res: Result<()> = async {
                    let pk = Arc::new(gen_pk(uniq_id));
                    let pv = Arc::new(gen_pv(price));
                    txn.put(pk, Some(pv)).await?;

                    loop {
                        let tot_price: Result<i64> = txn
                            .get_sv_range(spec_ref, None, None, |entries| -> Result<i64> {
                                let mut tot_price = 0;
                                for entry in entries {
                                    let (svpk, pv) = entry.try_borrow()?;
                                    if pk_is_cart_item(&svpk.pk) {
                                        if let Some(price) = extract_price(pv) {
                                            tot_price += price;
                                        }
                                    }
                                }
                                Ok(tot_price)
                            })
                            .await;
                        let tot_price = tot_price?;
                        /* This `tot_price` includes the locally inserted content. */

                        if tot_price <= tot_price_thresh {
                            sleep(1).await;
                            match txn.try_commit().await? {
                                CommitResult::Conflict => (),
                                CommitResult::Success => break,
                            }
                        } else {
                            /* Already too high. Give up; do not insert. */
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

    /* Check the ending condition. */
    let mut final_tot_price = 0;
    for (pk, pv) in db_adap.get_sv_range(&spec, None, None).await? {
        if pk_is_cart_item(&pk) {
            if let Some(price) = extract_price(&pv) {
                final_tot_price += price;
            }
        }
    }
    assert!(0 < final_tot_price && final_tot_price <= tot_price_thresh);

    Ok(())
}
