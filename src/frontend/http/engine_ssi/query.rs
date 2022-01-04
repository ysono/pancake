use crate::frontend::api::{SearchRange, Statement};
use crate::frontend::http::resp;
use crate::storage::engine_ssi::oper::txn::{CloseResult, CommitResult, Txn};
use crate::storage::engine_ssi::DB;
use crate::storage::engines_common::Entry;
use crate::storage::types::{PKShared, PVShared, PrimaryKey, Value};
use anyhow::{anyhow, Result};
use hyper::{Body, Response};
use std::sync::Arc;

pub async fn query(db: &Arc<DB>, stmt: Statement) -> Result<Response<Body>> {
    match stmt {
        Statement::GetPK(SearchRange::One(pk)) => {
            let fut = Txn::run(db, |mut txn| {
                Box::pin(async {
                    let kv: Result<Option<(_, _)>> = txn.get_pk_one(&pk).await;
                    txn.close(kv).await
                })
            });
            let res: CloseResult<Option<(_, _)>> = fut.await;
            match res.into() {
                Err(e) => return resp::err(e),
                Ok(None) => return resp::no_content(),
                Ok(Some((pk, pv))) => {
                    let mut body = String::new();
                    pkpv_to_str(&mut body, &pk, &pv);
                    return resp::ok(body);
                }
            }
        }
        Statement::GetPK(SearchRange::Range { lo, hi }) => {
            let fut = Txn::run(db, |mut txn| {
                Box::pin(async {
                    let body: Result<Response<_>> = txn
                        .get_pk_range(lo.as_ref(), hi.as_ref(), |entries| entries_to_str(entries))
                        .await;
                    txn.close(body).await
                })
            });
            let res: CloseResult<Response<_>> = fut.await;
            res.into()
        }
        Statement::GetSV(spec, sv_range) => {
            let (sv_lo, sv_hi) = sv_range.as_ref();
            let fut = Txn::run(db, |mut txn| {
                Box::pin(async {
                    let body: Result<Response<_>> = txn
                        .get_sv_range(&spec, sv_lo, sv_hi, |entries| {
                            let mut pkpv_entries =
                                entries.map(|entry| entry.convert::<PKShared, PVShared>());
                            entries_to_str(&mut pkpv_entries)
                        })
                        .await;
                    txn.close(body).await
                })
            });
            let res: CloseResult<Response<_>> = fut.await;
            res.into()
        }
        Statement::Put(pk, opt_pv) => {
            enum PutResult {
                Ok,
                TooManyRetries,
            }

            let pk = Arc::new(pk);
            let opt_pv = opt_pv.map(|pv| Arc::new(pv));

            let fut = Txn::run(db, |mut txn| {
                Box::pin(async {
                    let put_res: Result<PutResult> = async {
                        let retries = 20;
                        for _ in 0..retries {
                            txn.put(pk.clone(), opt_pv.clone()).await?;
                            match txn.try_commit().await? {
                                CommitResult::Conflict => txn.clear().await?,
                                CommitResult::Success => return Ok(PutResult::Ok),
                            }
                        }
                        Ok(PutResult::TooManyRetries)
                    }
                    .await;
                    txn.close(put_res).await
                })
            });

            let res: CloseResult<PutResult> = fut.await;
            match res.into() {
                Err(e) => resp::err(e),
                Ok(PutResult::TooManyRetries) => resp::err(anyhow!("Too many retries")),
                Ok(PutResult::Ok) => resp::no_content(),
            }
        }
    }
}

fn entries_to_str<'a>(
    entries: &mut dyn Iterator<Item = Entry<'a, PKShared, PVShared>>,
) -> Result<Response<Body>> {
    let mut body = String::new();
    for entry in entries {
        match entry.try_borrow() {
            Err(e) => return resp::err(e),
            Ok((pk, pv)) => pkpv_to_str(&mut body, pk, pv),
        }
    }
    if body.is_empty() {
        return resp::no_content();
    } else {
        return resp::ok(body);
    }
}

fn pkpv_to_str(body: &mut String, pk: &PrimaryKey, pv: &Value) {
    let s = format!("Key:\r\n{:?}\r\nValue:\r\n{:?}\r\n", pk, pv);
    body.push_str(&s);
}
