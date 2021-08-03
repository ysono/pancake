use crate::storage::api::{Datum, Key, Value};
use crate::storage::LSM;
use anyhow::{anyhow, Error, Result};
use futures::executor::block_on;
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
    let key = Key(Datum::Str(key.clone()));

    let lsm = req.data::<Arc<RwLock<LSM>>>().unwrap();
    let lsm = lsm.read().unwrap();

    let val: Value = lsm.get(key)?;

    match val.0 {
        None => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .map_err(|e| anyhow!(e)),
        Some(val) => {
            let body: String = match val {
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
    let key = Key(Datum::Str(key.clone()));

    let val: Vec<u8> = block_on(hyper::body::to_bytes(body))?.to_vec();
    let val = Value::from(Datum::Bytes(val));

    let lsm = parts.data::<Arc<RwLock<LSM>>>().unwrap();
    let mut lsm = lsm.write().unwrap();

    lsm.put(key, val)?;

    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .map_err(|e| anyhow!(e))
}

async fn delete_handler(req: Request<Body>) -> Result<Response<Body>> {
    let key: &String = req.param("key").unwrap();
    let key = Key(Datum::Str(key.clone()));

    let lsm = req.data::<Arc<RwLock<LSM>>>().unwrap();
    let mut lsm = lsm.write().unwrap();

    lsm.put(key, Value(None))?;

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
    let path = "/tmp/pancake";
    let lsm: Arc<RwLock<LSM>> = Arc::new(RwLock::new(LSM::open(path).unwrap()));

    Router::builder()
        .data(lsm)
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
