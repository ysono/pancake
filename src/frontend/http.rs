use crate::storage::api::{Key, Value};
use crate::storage::LSM;
use futures::executor::block_on;
use hyper::{Body, Request, Response, Server, StatusCode};
use routerify::prelude::*;
use routerify::{RequestInfo, Router, RouterService};
use std::sync::{Arc, RwLock};
use std::{convert::Infallible, net::SocketAddr};

async fn get_handler(req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let key: &String = req.param("key").unwrap();
    let key = Key::from(key.clone());

    let lsm = req.data::<Arc<RwLock<LSM>>>().unwrap();
    let lsm = lsm.read().unwrap();

    let maybe_val: Option<Value> = lsm.get(key).unwrap();
    let val_repr = match maybe_val {
        Some(Value::Bytes(bytes)) => String::from_utf8(bytes).unwrap(),
        x => format!("{:?}", x),
    };
    Ok(Response::new(Body::from(format!("Got {:?}", val_repr))))
}

async fn put_handler(req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let (parts, body) = req.into_parts();

    let key_raw: &String = parts.param("key").unwrap();
    let key = Key::from(key_raw.clone());

    let val: Vec<u8> = block_on(hyper::body::to_bytes(body)).unwrap().to_vec();
    let val = Value::Bytes(val);
    let val_repr = match &val {
        Value::Bytes(bytes) => String::from_utf8(bytes.clone()).unwrap(),
        x => format!("{:?}", x),
    };

    let lsm = parts.data::<Arc<RwLock<LSM>>>().unwrap();
    let mut lsm = lsm.write().unwrap();

    lsm.put(key, val).unwrap();

    Ok(Response::new(Body::from(format!(
        "Put {:?} {:?}",
        key_raw, val_repr
    ))))
}

async fn error_handler(err: routerify::RouteError, _: RequestInfo) -> Response<Body> {
    eprintln!("{}", err);
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(Body::from(format!("Something went wrong: {}", err)))
        .unwrap()
}

fn router() -> Router<Body, Infallible> {
    let lsm: Arc<RwLock<LSM>> = Arc::new(RwLock::new(LSM::init().unwrap()));

    Router::builder()
        .data(lsm)
        .get("/get/:key", get_handler)
        .put("/put/:key", put_handler)
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
