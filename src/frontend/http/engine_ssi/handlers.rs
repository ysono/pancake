use super::query;
use crate::frontend::api::{Operation, SearchRange, Statement};
use crate::frontend::http::resp;
use crate::frontend::query::basic::{self as query_basic};
use crate::storage::engine_ssi::oper::scnd_idx_mod::{
    self, CreateScndIdxResult, DeleteScndIdxResult,
};
use crate::storage::engine_ssi::DB;
use crate::storage::serde::Datum;
use crate::storage::types::{PrimaryKey, Value};
use anyhow::{Error, Result};
use hyper::{Body, Request, Response};
use routerify::prelude::*;
use routerify::RouterBuilder;
use std::sync::Arc;

pub fn add_routes(rb: RouterBuilder<Body, Error>) -> RouterBuilder<Body, Error> {
    rb.get("/ssi/key/:key", get_handler)
        .put("/ssi/key/:key", put_handler)
        .delete("/ssi/key/:key", delete_handler)
        .post("/ssi/query", query_handler)
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

    let val: Vec<u8> = hyper::body::to_bytes(body).await?.to_vec();
    let val = String::from_utf8(val.into_iter().collect())?;
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
    let body = String::from_utf8(body.into_iter().collect())?;

    let db = parts.data::<Arc<DB>>().unwrap();

    match query_basic::parse(&body)? {
        Operation::Query(stmt) => {
            return query::query(db, stmt).await;
        }
        Operation::CreateScndIdx(spec) => {
            match scnd_idx_mod::create_scnd_idx(&db, Arc::new(spec)).await {
                Err(e) => return resp::err(e),
                Ok(CreateScndIdxResult::NoOp(msg)) => return resp::ok(msg),
                Ok(CreateScndIdxResult::Success) => return resp::no_content(),
            }
        }
        Operation::DelScndIdx(spec) => match scnd_idx_mod::delete_scnd_idx(&db, &spec).await {
            Err(e) => return resp::err(e),
            Ok(DeleteScndIdxResult::NoOp(msg)) => return resp::ok(msg),
            Ok(DeleteScndIdxResult::Success) => return resp::no_content(),
        },
    }
}
