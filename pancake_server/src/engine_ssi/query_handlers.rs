use crate::{
    common::http_utils::{self, entries_to_string, kv_to_string, AppError},
    oper::api::{Operation, SearchRange, Statement},
};
use anyhow::Result;
use axum::http::StatusCode;
use pancake_engine_ssi::{
    ClientCommitDecision, ScndIdxCreationJobErr, ScndIdxDeletionJobErr, Txn, DB,
};
use pancake_types::types::{PKShared, PVShared};
use std::sync::Arc;

pub async fn handle_oper(db: &DB, oper: Operation) -> Result<(StatusCode, String), AppError> {
    match oper {
        Operation::Query(stmt) => {
            return handle_stmt(db, stmt).await;
        }
        Operation::CreateScndIdx(sv_spec) => {
            let sv_spec = Arc::new(sv_spec);
            match db.create_scnd_idx(&sv_spec).await {
                Ok(()) => return http_utils::ok(""),
                Err(ScndIdxCreationJobErr::Existent { is_readable }) => {
                    if is_readable {
                        return Ok((StatusCode::NOT_MODIFIED, "".to_string()));
                    } else {
                        return Ok((
                            StatusCode::PROCESSING,
                            "The secondary index is being created.".to_string(),
                        ));
                    }
                }
                Err(ScndIdxCreationJobErr::Busy) => {
                    return Ok((
                        StatusCode::TOO_MANY_REQUESTS,
                        "Too many other existing secondary index creation jobs are in progress."
                            .to_string(),
                    ));
                }
                Err(ScndIdxCreationJobErr::InternalError(e)) => return Err(AppError(e)),
            }
        }
        Operation::DelScndIdx(spec) => match db.delete_scnd_idx(&spec).await {
            Ok(()) => return http_utils::ok(""),
            Err(ScndIdxDeletionJobErr::CreationInProgress) => {
                return Ok((StatusCode::BAD_REQUEST, "The secondary index is being created right now, and cannot be deleted until the creation is done.".to_string() ));
            }
            Err(ScndIdxDeletionJobErr::InternalError(e)) => return Err(AppError(e)),
        },
    }
}

pub async fn handle_stmt(db: &DB, stmt: Statement) -> Result<(StatusCode, String), AppError> {
    match stmt {
        Statement::GetPK(SearchRange::One(pk)) => {
            let opt_pkpv = Txn::run(db, 0, |txn| {
                let opt_pkpv = txn.get_pk_one(&pk)?;
                Ok(ClientCommitDecision::Commit(opt_pkpv))
            })
            .await?;
            match opt_pkpv {
                None => Ok((StatusCode::NOT_FOUND, "".to_string())),
                Some((pk, pv)) => {
                    let mut body = String::new();
                    kv_to_string(&mut body, &pk, &pv);
                    return http_utils::ok(body);
                }
            }
        }
        Statement::GetPK(SearchRange::Range { lo, hi }) => {
            let body = Txn::run(db, 0, |txn| {
                let entries = txn.get_pk_range(lo.as_ref(), hi.as_ref());
                let body = entries_to_string(entries)?;
                Ok(ClientCommitDecision::Commit(body))
            })
            .await?;
            return http_utils::ok(body);
        }
        Statement::GetSV(sv_spec, sv_range) => {
            let (sv_lo, sv_hi) = sv_range.as_ref();
            let body = Txn::run(db, 0, |txn| {
                let scnd_entries = txn.get_sv_range(&sv_spec, sv_lo, sv_hi)?;
                let pkpv_entries = scnd_entries.map(|entry| entry.convert::<PKShared, PVShared>());
                let body = entries_to_string(pkpv_entries)?;
                Ok(ClientCommitDecision::Commit(body))
            })
            .await?;
            return http_utils::ok(body);
        }
        Statement::Put(pk, opt_pv) => {
            let pk = Arc::new(pk);
            let opt_pv = opt_pv.map(Arc::new);

            const RETRY_LIMIT: usize = 5;

            Txn::run(db, RETRY_LIMIT, |txn| {
                txn.put(&pk, &opt_pv)?;
                Ok(ClientCommitDecision::Commit(()))
            })
            .await?;

            return http_utils::ok("");
        }
    }
}
