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
                Err(e) => resp::err(e),
                Ok(None) => resp::no_content(),
                Ok(Some(pv)) => {
                    let mut s = String::new();
                    pkpv_to_str(&mut s, &pk, &pv);
                    resp::ok(s)
                }
            }
        }
        Statement::GetPK(SearchRange::Range { lo, hi }) => {
            let db = db.read().unwrap();
            let res_iter = db.get_pk_range(lo.as_ref(), hi.as_ref());
            match res_iter {
                Err(e) => resp::err(e),
                Ok(kvs) => {
                    let mut s = String::new();
                    for res_kv in kvs {
                        let (pk, pv) = res_kv?;
                        pkpv_to_str(&mut s, &pk, &pv);
                    }
                    resp::ok(s)
                }
            }
        }
        Statement::GetSV(spec, sv_range) => {
            let (sv_lo, sv_hi) = sv_range.as_ref();
            let db = db.read().unwrap();
            let res_iter = db.get_sv_range(&spec, sv_lo, sv_hi);
            match res_iter {
                Err(e) => resp::err(e),
                Ok(kvs) => {
                    let mut s = String::new();
                    for res_kv in kvs {
                        let (pk, pv) = res_kv?;
                        pkpv_to_str(&mut s, &pk, &pv);
                    }
                    resp::ok(s)
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
