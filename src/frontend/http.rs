mod query;
mod resp;

use crate::frontend::api::{Operation, SearchRange, Statement};
use crate::frontend::query::basic::{self as query_basic};
use crate::storage::db::DB;
use crate::storage::serde::Datum;
use crate::storage::types::{PrimaryKey, Value};
use anyhow::{Error, Result};
use hyper::{Body, Request, Response, Server, StatusCode};
use routerify::prelude::*;
use routerify::{Middleware, RequestInfo, Router, RouterService};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

async fn logger(req: Request<Body>) -> Result<Request<Body>> {
    println!(
        "{} {} {}",
        req.remote_addr(),
        req.method(),
        req.uri().path()
    );
    Ok(req)
}

async fn get_handler(req: Request<Body>) -> Result<Response<Body>> {
    let key: &String = req.param("key").unwrap();
    let key = PrimaryKey(Datum::Str(key.clone()));

    let stmt = Statement::GetPK(SearchRange::One(key));

    let db = req.data::<Arc<RwLock<DB>>>().unwrap();

    query::query(db, stmt)
}

async fn put_handler(req: Request<Body>) -> Result<Response<Body>> {
    let (parts, body) = req.into_parts();

    let key: &String = parts.param("key").unwrap();
    let key = PrimaryKey(Datum::Str(key.clone()));

    let val: Vec<u8> = hyper::body::to_bytes(body).await?.to_vec();
    let val = String::from_utf8(val.into_iter().collect())?;
    let val = Value(Datum::Str(val));

    let stmt = Statement::Put(key, Some(val));

    let db = parts.data::<Arc<RwLock<DB>>>().unwrap();

    query::query(db, stmt)
}

async fn delete_handler(req: Request<Body>) -> Result<Response<Body>> {
    let key: &String = req.param("key").unwrap();
    let key = PrimaryKey(Datum::Str(key.clone()));

    let stmt = Statement::Put(key, None);

    let db = req.data::<Arc<RwLock<DB>>>().unwrap();

    query::query(db, stmt)
}

async fn query_handler(req: Request<Body>) -> Result<Response<Body>> {
    let (parts, body) = req.into_parts();

    let body = hyper::body::to_bytes(body).await?;
    let body = String::from_utf8(body.into_iter().collect())?;

    let db = parts.data::<Arc<RwLock<DB>>>().unwrap();

    match query_basic::parse(&body)? {
        Operation::Query(stmt) => {
            return query::query(db, stmt);
        }
        Operation::CreateScndIdx(spec) => {
            let mut db = db.write().unwrap();
            let res = db.create_scnd_idx(Arc::new(spec));
            match res {
                Err(e) => return resp::err(e),
                Ok(()) => return resp::no_content(),
            }
        }
        Operation::DelScndIdx(spec) => {
            let mut db = db.write().unwrap();
            let res = db.delete_scnd_idx(&spec);
            match res {
                Err(e) => return resp::err(e),
                Ok(()) => return resp::no_content(),
            }
        }
    }
}

async fn error_handler(err: routerify::RouteError, _: RequestInfo) -> Response<Body> {
    eprintln!("{}", err);
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(Body::from(format!("Something went wrong: {}", err)))
        .unwrap()
}

fn router(db: Arc<RwLock<DB>>) -> Router<Body, Error> {
    Router::builder()
        .data(db)
        .middleware(Middleware::pre(logger))
        .get("/key/:key", get_handler)
        .put("/key/:key", put_handler)
        .delete("/key/:key", delete_handler)
        .post("/query", query_handler)
        .err_handler_with_info(error_handler)
        .build()
        .unwrap()
}

pub async fn main(db: Arc<RwLock<DB>>) {
    let router = router(db);

    let service = RouterService::new(router).unwrap();

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));

    let server = Server::bind(&addr).serve(service);

    println!("App is running on: {}", addr);
    if let Err(err) = server.await {
        eprintln!("Server error: {}", err);
    }
}
