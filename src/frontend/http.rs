mod handlers;
pub(self) mod query;
pub(self) mod resp;

use crate::storage::engine_serial::db::DB;
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

async fn error_handler(err: routerify::RouteError, _: RequestInfo) -> Response<Body> {
    eprintln!("{}", err);
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(Body::from(format!("Something went wrong: {}", err)))
        .unwrap()
}

fn router(db: Arc<RwLock<DB>>) -> Router<Body, Error> {
    let rb = Router::builder()
        .middleware(Middleware::pre(logger))
        .err_handler_with_info(error_handler)
        .data(db);
    handlers::add_routers(rb).build().unwrap()
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
