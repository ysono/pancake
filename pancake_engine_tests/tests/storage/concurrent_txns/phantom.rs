use super::super::helpers::{
    gen,
    one_stmt::{OneStmtDbAdaptor, OneStmtSsiDbAdaptor},
};
use anyhow::Result;
use pancake_engine_ssi::{ClientCommitDecision, Txn, DB};
use pancake_types::serde::{Datum, DatumType};
use pancake_types::types::{PrimaryKey, SubValueSpec, Value};
use std::sync::Arc;
use tokio::task::JoinHandle;

fn gen_pk(item_i: usize) -> PrimaryKey {
    gen::gen_str_pk(format!("cart_item.{item_i}"))
}
fn gen_pv(price: i64) -> Value {
    Value(Datum::I64(price))
}
fn gen_sv_spec() -> SubValueSpec {
    SubValueSpec::whole(DatumType::I64)
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

pub async fn no_phantom(db: &'static DB) -> Result<()> {
    let db_adap = OneStmtSsiDbAdaptor { db };

    let sv_spec = Arc::new(gen_sv_spec());

    db_adap.nonmut_create_scnd_idx(sv_spec.clone()).await?;

    /* Check the initial condition: cart is empty. */
    let beginning_cart_items_ct = db_adap
        .get_sv_range(&sv_spec, None, None)
        .await?
        .into_iter()
        .filter(|(pk, _pv)| pk_is_cart_item(&pk))
        .count();
    assert_eq!(0, beginning_cart_items_ct);

    /* test params */
    let tot_price_thresh = 80;
    let items_ct = 20;
    let item_price = 8;

    /* In each txn, attempt to insert a content to the cart. */
    let mut tasks = vec![];
    for item_i in 0..items_ct {
        let sv_spec = Arc::clone(&sv_spec);

        let retry_limit = items_ct - 1;

        let task_fut = async move {
            let pk = Arc::new(gen_pk(item_i));
            let pv = Arc::new(gen_pv(item_price));

            let txn_fut = Txn::run(db, retry_limit, |txn| {
                let entries = txn.get_sv_range(&sv_spec, None, None)?;
                let mut tot_price = 0;
                for entry in entries {
                    let (svpk, pv) = entry.try_borrow()?;
                    if pk_is_cart_item(&svpk.pk) {
                        if let Some(price) = extract_price(&pv) {
                            tot_price += price;
                        }
                    }
                }
                if tot_price + item_price > tot_price_thresh {
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
    for task in tasks.into_iter() {
        let res: Result<Result<()>, _> = task.await;
        res??;
    }

    /* Check the ending condition. */
    let mut final_tot_price = 0;
    for (pk, pv) in db_adap.get_sv_range(&sv_spec, None, None).await? {
        if pk_is_cart_item(&pk) {
            if let Some(price) = extract_price(&pv) {
                final_tot_price += price;
            }
        }
    }
    let exp_final_tot_price = (tot_price_thresh / item_price) * item_price;
    assert_eq!(final_tot_price, exp_final_tot_price);

    Ok(())
}
