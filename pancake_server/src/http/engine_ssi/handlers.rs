use super::query;
use crate::api::{Operation, SearchRange, Statement};
use crate::http::resp;
use crate::query::basic::{self as query_basic};
use crate::wasm::engine_ssi::WasmEngine;
use anyhow::{anyhow, Error, Result};
use hyper::{Body, Request, Response};
use pancake_engine_ssi::{ScndIdxCreationJobErr, ScndIdxDeletionJobErr, DB};
use pancake_types::serde::Datum;
use pancake_types::types::{PrimaryKey, Value};
use routerify::prelude::*;
use routerify::RouterBuilder;
use std::sync::Arc;

pub fn add_routes(rb: RouterBuilder<Body, Error>) -> RouterBuilder<Body, Error> {
    rb.get("/ssi/key/:key", get_handler)
        .put("/ssi/key/:key", put_handler)
        .delete("/ssi/key/:key", delete_handler)
        .post("/ssi/query", query_handler)
        .post("/ssi/wasm", wasm_handler)
}

async fn get_handler(req: Request<Body>) -> Result<Response<Body>> {
    let key: &String = req.param("key").unwrap();
    let key = PrimaryKey(Datum::Str(key.clone()));

    let stmt = Statement::GetPK(SearchRange::One(key));

    let db = req.data::<Arc<DB>>().unwrap();

    query::query(db, stmt).await
}

async fn put_handler(req: Request<Body>) -> Result<Response<Body>> {
    let (parts, body) = req.into_parts();

    let key: &String = parts.param("key").unwrap();
    let key = PrimaryKey(Datum::Str(key.clone()));

    let val = hyper::body::to_bytes(body).await?;
    let val = String::from_utf8(val.to_vec())?;
    let val = Value(Datum::Str(val));

    let stmt = Statement::Put(key, Some(val));

    let db = parts.data::<Arc<DB>>().unwrap();

    query::query(db, stmt).await
}

async fn delete_handler(req: Request<Body>) -> Result<Response<Body>> {
    let key: &String = req.param("key").unwrap();
    let key = PrimaryKey(Datum::Str(key.clone()));

    let stmt = Statement::Put(key, None);

    let db = req.data::<Arc<DB>>().unwrap();

    query::query(db, stmt).await
}

async fn query_handler(req: Request<Body>) -> Result<Response<Body>> {
    let (parts, body) = req.into_parts();

    let body = hyper::body::to_bytes(body).await?;
    let body = String::from_utf8(body.to_vec())?;

    let db = parts.data::<Arc<DB>>().unwrap();

    match query_basic::parse(&body)? {
        Operation::Query(stmt) => {
            return query::query(db, stmt).await;
        }
        Operation::CreateScndIdx(sv_spec) => {
            let sv_spec = Arc::new(sv_spec);
            match db.create_scnd_idx(&sv_spec).await {
                Ok(()) => return resp::ok(""),
                Err(ScndIdxCreationJobErr::Existent { is_readable }) => {
                    if is_readable {
                        return resp::not_modified("");
                    } else {
                        let s = "The secondary index is being created.";
                        return resp::err(anyhow!(s));
                    }
                }
                Err(ScndIdxCreationJobErr::Busy) => {
                    let s =
                        "Too many other existing secondary index creation jobs are in progress.";
                    return resp::err(anyhow!(s));
                }
                Err(ScndIdxCreationJobErr::InternalError(e)) => return resp::err(e),
            }
        }
        Operation::DelScndIdx(sv_spec) => match db.delete_scnd_idx(&sv_spec).await {
            Ok(()) => return resp::ok(""),
            Err(ScndIdxDeletionJobErr::CreationInProgress) => {
                return resp::err(anyhow!("The secondary index is being created right now, and cannot be deleted until the creation is done."));
            }
            Err(ScndIdxDeletionJobErr::InternalError(e)) => resp::err(e),
        },
    }
}

async fn wasm_handler(req: Request<Body>) -> Result<Response<Body>> {
    let (parts, body) = req.into_parts();

    let compo_bytes = hyper::body::to_bytes(body).await?;

    let retry_limit = 5;

    let engine = parts.data::<WasmEngine>().unwrap();

    let wasm_res = engine.serve(&compo_bytes, retry_limit).await;
    match wasm_res {
        Err(e) => resp::err(e),
        Ok(s) => resp::ok(s),
    }
}
