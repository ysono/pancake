use crate::api::{SearchRange, Statement};
use crate::http::resp;
use anyhow::Result;
use hyper::{Body, Response};
use pancake_engine_common::Entry;
use pancake_engine_ssi::{ClientCommitDecision, Txn, DB};
use pancake_types::types::{PKShared, PVShared, PrimaryKey, Value};
use std::sync::Arc;

pub async fn query(db: &Arc<DB>, stmt: Statement) -> Result<Response<Body>> {
    match stmt {
        Statement::GetPK(SearchRange::One(pk)) => {
            let fut = Txn::run(db, 0, |txn| {
                let opt_pkpv = txn.get_pk_one(&pk)?;
                Ok(ClientCommitDecision::Commit(opt_pkpv))
            });
            let res_opt_pkpv = fut.await;

            match res_opt_pkpv {
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
            let fut = Txn::run(db, 0, |txn| {
                let entries = txn.get_pk_range(lo.as_ref(), hi.as_ref());
                let body = entries_to_str(entries)?;
                Ok(ClientCommitDecision::Commit(body))
            });
            let res: Result<Response<Body>> = fut.await;
            res
        }
        Statement::GetSV(sv_spec, sv_range) => {
            let (sv_lo, sv_hi) = sv_range.as_ref();
            let fut = Txn::run(db, 0, |txn| {
                let entries = txn.get_sv_range(&sv_spec, sv_lo, sv_hi)?;
                let pkpv_entries = entries.map(|entry| entry.convert::<PKShared, PVShared>());
                let body = entries_to_str(pkpv_entries)?;
                Ok(ClientCommitDecision::Commit(body))
            });
            let res: Result<Response<Body>> = fut.await;
            res
        }
        Statement::Put(pk, opt_pv) => {
            let pk = Arc::new(pk);
            let opt_pv = opt_pv.map(Arc::new);

            const RETRY_LIMIT: usize = 5;

            let fut = Txn::run(db, RETRY_LIMIT, |txn| {
                txn.put(&pk, &opt_pv)?;
                Ok(ClientCommitDecision::Commit(()))
            });
            let res: Result<()> = fut.await;
            match res {
                Err(e) => resp::err(e),
                Ok(()) => resp::no_content(),
            }
        }
    }
}

fn entries_to_str<'a>(
    entries: impl Iterator<Item = Entry<'a, PKShared, PVShared>>,
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
    let s = format!("Key:\r\n{pk:?}\r\nValue:\r\n{pv:?}\r\n");
    body.push_str(&s);
}
