use crate::frontend::query::basic::{self as query, Query};
use crate::storage::db::DB;
use crate::storage::types::{Datum, PrimaryKey, Value};
use anyhow::{Error, Result};
use futures::executor::block_on;
use hyper::{Body, Request, Response, Server, StatusCode};
use routerify::prelude::*;
use routerify::{Middleware, RequestInfo, Router, RouterService};
use std::env;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

mod db_helpers {
    use crate::storage::db::DB;
    use crate::storage::types::{Datum, PrimaryKey, SubValue, SubValueSpec, Value};
    use anyhow::{anyhow, Result};
    use hyper::{Body, Response, StatusCode};

    pub fn put(db: &mut DB, key: PrimaryKey, val: Value) -> Result<Response<Body>> {
        db.put(key, val)?;

        no_content()
    }

    pub fn delete(db: &mut DB, key: PrimaryKey) -> Result<Response<Body>> {
        db.delete(key)?;

        no_content()
    }

    pub fn get(db: &DB, key: &PrimaryKey) -> Result<Response<Body>> {
        match db.get(&key)? {
            None => Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty())
                .map_err(|e| anyhow!(e)),
            Some(dat) => {
                let mut body: String = match dat.0 {
                    Datum::Bytes(bytes) => bytes.iter().map(|b| format!("{:#x}", b)).collect(),
                    Datum::I64(i) => i.to_string(),
                    Datum::Str(s) => s,
                    Datum::Tuple(vec) => format!("{:?}", vec),
                };
                body.push_str("\r\n");
                Ok(Response::new(Body::from(body)))
            }
        }
    }

    pub fn get_between(
        db: &DB,
        val_lo: &Option<PrimaryKey>,
        val_hi: &Option<PrimaryKey>,
    ) -> Result<Response<Body>> {
        let kvs = db.get_range(val_lo.as_ref(), val_hi.as_ref())?;

        kvs_to_resp(&kvs)
    }

    pub fn get_where(
        db: &DB,
        spec: &SubValueSpec,
        subval_lo: &Option<SubValue>,
        subval_hi: &Option<SubValue>,
    ) -> Result<Response<Body>> {
        let kvs = db.get_by_sub_value(spec, subval_lo.as_ref(), subval_hi.as_ref())?;

        kvs_to_resp(&kvs)
    }

    pub fn create_sec_idx(db: &mut DB, spec: SubValueSpec) -> Result<Response<Body>> {
        db.create_sec_idx(spec)?;

        no_content()
    }

    fn kvs_to_resp(kvs: &[(PrimaryKey, Value)]) -> Result<Response<Body>> {
        let kv_strs = kvs.into_iter().map(|(k, v)| {
            [
                String::from("Key:"),
                format!("{:?}", k),
                String::from("Value:"),
                format!("{:?}", v),
            ]
            .join("\r\n")
        });
        let mut out_str = itertools::Itertools::intersperse(kv_strs, String::from("\r\n\r\n"))
            .collect::<String>();
        out_str.push_str("\r\n");

        Ok(Response::new(Body::from(out_str)))
    }

    fn no_content() -> Result<Response<Body>> {
        Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Body::empty())
            .map_err(|e| anyhow!(e))
    }
}

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

    db_helpers::get(&db, &key)
}

async fn put_handler(req: Request<Body>) -> Result<Response<Body>> {
    let (parts, body) = req.into_parts();

    let key: &String = parts.param("key").unwrap();
    let key = PrimaryKey(Datum::Str(key.clone()));

    let val: Vec<u8> = block_on(hyper::body::to_bytes(body))?.to_vec();
    let val = String::from_utf8(val.into_iter().collect())?;
    let val = Value(Datum::Str(val));

    let db = parts.data::<Arc<RwLock<DB>>>().unwrap();
    let mut db = db.write().unwrap();

    db_helpers::put(&mut db, key, val)
}

async fn delete_handler(req: Request<Body>) -> Result<Response<Body>> {
    let key: &String = req.param("key").unwrap();
    let key = PrimaryKey(Datum::Str(key.clone()));

    let db = req.data::<Arc<RwLock<DB>>>().unwrap();
    let mut db = db.write().unwrap();

    db_helpers::delete(&mut db, key)
}

async fn query_handler(req: Request<Body>) -> Result<Response<Body>> {
    let (parts, body) = req.into_parts();

    let body = block_on(hyper::body::to_bytes(body))?;
    let body = String::from_utf8(body.into_iter().collect())?;

    let db = parts.data::<Arc<RwLock<DB>>>().unwrap();
    let mut db = db.write().unwrap();

    let query = query::parse(&body)?;
    match query {
        Query::Put(key, val) => db_helpers::put(&mut db, key, val),
        Query::Del(key) => db_helpers::delete(&mut db, key),
        Query::Get(key) => db_helpers::get(&db, &key),
        Query::GetBetween(key_lo, key_hi) => db_helpers::get_between(&db, &key_lo, &key_hi),
        Query::GetWhere(spec, subval) => db_helpers::get_where(&db, &spec, &subval, &subval),
        Query::GetWhereBetween(spec, subval_lo, subval_hi) => {
            db_helpers::get_where(&db, &spec, &subval_lo, &subval_hi)
        }
        Query::CreateSecIdx(spec) => db_helpers::create_sec_idx(&mut db, spec),
    }
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
        .post("/query", query_handler)
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
