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
            match db.get(&pk) {
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
            match db.get_range(lo.as_ref(), hi.as_ref()) {
                Err(e) => resp::err(e),
                Ok(kvs) => {
                    let mut s = String::new();
                    for (pk, pv) in kvs.iter() {
                        pkpv_to_str(&mut s, pk, pv);
                    }
                    resp::ok(s)
                }
            }
        }
        Statement::GetSV(spec, sv_range) => {
            let (sv_lo, sv_hi) = sv_range.as_ref();
            let db = db.read().unwrap();
            match db.get_by_sub_value(&spec, sv_lo, sv_hi) {
                Err(e) => resp::err(e),
                Ok(kvs) => {
                    let mut s = String::new();
                    for (pk, pv) in kvs.iter() {
                        pkpv_to_str(&mut s, pk, pv);
                    }
                    resp::ok(s)
                }
            }
        }
        Statement::Put(pk, opt_pv) => {
            let mut db = db.write().unwrap();
            let res = match opt_pv {
                None => db.delete(pk),
                Some(pv) => db.put(pk, pv),
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