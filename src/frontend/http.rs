use crate::storage::db::DB;
use crate::storage::types::{Datum, PrimaryKey, Value};
use anyhow::{anyhow, Error, Result};
use futures::executor::block_on;
use hyper::{Body, Request, Response, Server, StatusCode};
use routerify::prelude::*;
use routerify::{Middleware, RequestInfo, Router, RouterService};
use std::env;
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

    let db = req.data::<Arc<RwLock<DB>>>().unwrap();
    let db = db.read().unwrap();

    match db.get(&key)? {
        None => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .map_err(|e| anyhow!(e)),
        Some(dat) => {
            let body: String = match dat.0 {
                Datum::Bytes(bytes) => bytes.iter().map(|b| format!("{:#x}", b)).collect(),
                Datum::I64(i) => i.to_string(),
                Datum::Str(s) => s,
                Datum::Tuple(vec) => format!("{:?}", vec),
            };
            Ok(Response::new(Body::from(body)))
        }
    }
}

async fn put_handler(req: Request<Body>) -> Result<Response<Body>> {
    let (parts, body) = req.into_parts();

    let key: &String = parts.param("key").unwrap();
    let key = PrimaryKey(Datum::Str(key.clone()));

    let val: Vec<u8> = block_on(hyper::body::to_bytes(body))?.to_vec();
    let val = Value(Datum::Bytes(val));

    let db = parts.data::<Arc<RwLock<DB>>>().unwrap();
    let mut db = db.write().unwrap();

    db.put(key, val)?;

    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .map_err(|e| anyhow!(e))
}

async fn delete_handler(req: Request<Body>) -> Result<Response<Body>> {
    let key: &String = req.param("key").unwrap();
    let key = PrimaryKey(Datum::Str(key.clone()));

    let db = req.data::<Arc<RwLock<DB>>>().unwrap();
    let mut db = db.write().unwrap();

    db.delete(key)?;

    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .map_err(|e| anyhow!(e))
}

async fn error_handler(err: routerify::RouteError, _: RequestInfo) -> Response<Body> {
    eprintln!("{}", err);
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(Body::from(format!("Something went wrong: {}", err)))
        .unwrap()
}

fn router() -> Router<Body, Error> {
    let path = env::temp_dir().join("pancake");
    let db = DB::open(path).unwrap();
    let db: Arc<RwLock<DB>> = Arc::new(RwLock::new(db));

    Router::builder()
        .data(db)
        .middleware(Middleware::pre(logger))
        .get("/key/:key", get_handler)
        .put("/key/:key", put_handler)
        .delete("/key/:key", delete_handler)
        .err_handler_with_info(error_handler)
        .build()
        .unwrap()
}

pub async fn main() {
    let router = router();

    let service = RouterService::new(router).unwrap();

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));

    let server = Server::bind(&addr).serve(service);

    println!("App is running on: {}", addr);
    if let Err(err) = server.await {
        eprintln!("Server error: {}", err);
    }
}
