use super::resp;
use crate::frontend::api::{SearchRange, Statement};
use crate::storage::db::DB;
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
                Some(entry) => match entry.borrow_res() {
                    Err(e) => return resp::err(e),
                    Ok((pk, pv)) => {
                        let mut s = String::new();
                        pkpv_to_str(&mut s, &pk, &pv);
                        return resp::ok(s);
                    }
                },
            }
        }
        Statement::GetPK(SearchRange::Range { lo, hi }) => {
            let db = db.read().unwrap();
            let mut s = String::new();
            for entry in db.get_pk_range(lo.as_ref(), hi.as_ref()) {
                match entry.borrow_res() {
                    Err(e) => return resp::err(e),
                    Ok((pk, pv)) => pkpv_to_str(&mut s, pk, pv),
                }
            }
            return resp::ok(s);
        }
        Statement::GetSV(spec, sv_range) => {
            let (sv_lo, sv_hi) = sv_range.as_ref();
            let db = db.read().unwrap();
            let res_iter = db.get_sv_range(&spec, sv_lo, sv_hi);
            match res_iter {
                Err(e) => return resp::err(e),
                Ok(entries) => {
                    let mut s = String::new();
                    for entry in entries {
                        match entry.borrow_res() {
                            Err(e) => return resp::err(e),
                            Ok((pk, pv)) => pkpv_to_str(&mut s, pk, pv),
                        }
                    }
                    return resp::ok(s);
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

fn pkpv_to_str(s: &mut String, pk: &PrimaryKey, pv: &Value) {
    let s2 = format!("Key:\r\n{:?}\r\nValue:\r\n{:?}\r\n", pk, pv);
    s.push_str(&s2);
}
