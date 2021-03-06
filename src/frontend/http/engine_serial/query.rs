use crate::frontend::api::{SearchRange, Statement};
use crate::frontend::http::resp;
use crate::storage::engine_serial::db::DB;
use crate::storage::types::{PrimaryKey, Value};
use anyhow::Result;
use hyper::{Body, Response};
use std::sync::{Arc, RwLock};

pub fn query(db: &Arc<RwLock<DB>>, stmt: Statement) -> Result<Response<Body>> {
    match stmt {
        Statement::GetPK(SearchRange::One(pk)) => {
            let db = db.read().unwrap();
            match db.get_pk_one(&pk) {
                None => return resp::no_content(),
                Some(entry) => match entry.try_borrow() {
                    Err(e) => return resp::err(e),
                    Ok((pk, pv)) => {
                        let mut body = String::new();
                        pkpv_to_str(&mut body, &pk, &pv);
                        return resp::ok(body);
                    }
                },
            }
        }
        Statement::GetPK(SearchRange::Range { lo, hi }) => {
            let db = db.read().unwrap();
            let mut body = String::new();
            for entry in db.get_pk_range(lo.as_ref(), hi.as_ref()) {
                match entry.try_borrow() {
                    Err(e) => return resp::err(e),
                    Ok((pk, pv)) => pkpv_to_str(&mut body, pk, pv),
                }
            }
            return resp::ok(body);
        }
        Statement::GetSV(spec, sv_range) => {
            let (sv_lo, sv_hi) = sv_range.as_ref();
            let db = db.read().unwrap();
            let res_iter = db.get_sv_range(&spec, sv_lo, sv_hi);
            match res_iter {
                Err(e) => return resp::err(e),
                Ok(entries) => {
                    let mut body = String::new();
                    for entry in entries {
                        match entry.try_borrow() {
                            Err(e) => return resp::err(e),
                            Ok((pk, pv)) => pkpv_to_str(&mut body, pk, pv),
                        }
                    }
                    return resp::ok(body);
                }
            }
        }
        Statement::Put(pk, opt_pv) => {
            let mut db = db.write().unwrap();
            let res = match opt_pv {
                None => db.put(Arc::new(pk), None),
                Some(pv) => db.put(Arc::new(pk), Some(Arc::new(pv))),
            };
            match res {
                Err(e) => resp::err(e),
                Ok(()) => resp::no_content(),
            }
        }
    }
}

fn pkpv_to_str(body: &mut String, pk: &PrimaryKey, pv: &Value) {
    let s = format!("Key:\r\n{:?}\r\nValue:\r\n{:?}\r\n", pk, pv);
    body.push_str(&s);
}
