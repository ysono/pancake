mod engine_serial;
mod engine_ssi;
mod resp;

use crate::storage::engine_serial::db::DB as SerialDb;
use crate::storage::engine_ssi::DB as SsiDb;
use anyhow::{Error, Result};
use hyper::{Body, Request, Response, Server, StatusCode};
use routerify::prelude::*;
use routerify::{Middleware, RequestInfo, Router, RouterService};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use tokio::sync::oneshot;

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

fn router(serial_db: Arc<RwLock<SerialDb>>, ssi_db: Arc<SsiDb>) -> Router<Body, Error> {
    let mut rb = Router::builder()
        .middleware(Middleware::pre(logger))
        .err_handler_with_info(error_handler)
        .data(serial_db)
        .data(ssi_db);
    rb = engine_serial::handlers::add_routes(rb);
    rb = engine_ssi::handlers::add_routes(rb);
    rb.build().unwrap()
}

pub async fn main(
    serial_db: Arc<RwLock<SerialDb>>,
    ssi_db: Arc<SsiDb>,
    terminate_rx: oneshot::Receiver<()>,
) {
    let router = router(serial_db, ssi_db);

    let service = RouterService::new(router).unwrap();

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));

    let server = Server::bind(&addr).serve(service);
    println!("Frontend is running on: {}", addr);

    let server = server.with_graceful_shutdown(async move {
        terminate_rx.await.ok();
    });
    let server_res = server.await;
    println!("Frontend is exiting.");
    if let Err(err) = server_res {
        eprintln!("Frontend error: {}", err);
    }
}
