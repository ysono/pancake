use crate::{
    common::http_utils::{self, entries_to_string, kv_to_string, AppError},
    oper::api::{Operation, SearchRange, Statement},
};
use axum::http::StatusCode;
use pancake_engine_serial::DB;
use std::sync::Arc;
use tokio::sync::RwLock;

pub async fn handle_oper(
    db: &RwLock<DB>,
    oper: Operation,
) -> Result<(StatusCode, String), AppError> {
    match oper {
        Operation::Query(stmt) => {
            return handle_stmt(db, stmt).await;
        }
        Operation::CreateScndIdx(sv_spec) => {
            let mut db = db.write().await;
            db.create_scnd_idx(Arc::new(sv_spec))?;
            return http_utils::ok("");
        }
        Operation::DelScndIdx(sv_spec) => {
            let mut db = db.write().await;
            db.delete_scnd_idx(&sv_spec)?;
            return http_utils::ok("");
        }
    }
}

pub async fn handle_stmt(
    db: &RwLock<DB>,
    stmt: Statement,
) -> Result<(StatusCode, String), AppError> {
    match stmt {
        Statement::GetPK(SearchRange::One(pk)) => {
            let db = db.read().await;
            match db.get_pk_one(&pk) {
                None => return Ok((StatusCode::NOT_FOUND, "".to_string())),
                Some(entry) => {
                    let (pk, pv) = entry.try_borrow()?;
                    let mut body = String::new();
                    kv_to_string(&mut body, pk, pv);
                    return http_utils::ok(body);
                }
            }
        }
        Statement::GetPK(SearchRange::Range { lo, hi }) => {
            let db = db.read().await;
            let entries = db.get_pk_range(lo.as_ref(), hi.as_ref());
            let body = entries_to_string(entries)?;
            return http_utils::ok(body);
        }
        Statement::GetSV(sv_spec, sv_range) => {
            let db = db.read().await;
            let (sv_lo, sv_hi) = sv_range.as_ref();
            let entries = db.get_sv_range(&sv_spec, sv_lo, sv_hi)?;
            let body = entries_to_string(entries)?;
            return http_utils::ok(body);
        }
        Statement::Put(pk, opt_pv) => {
            let mut db = db.write().await;
            let pk = Arc::new(pk);
            let opt_pv = opt_pv.map(Arc::new);
            db.put(pk, opt_pv)?;
            return http_utils::ok("");
        }
    }
}
