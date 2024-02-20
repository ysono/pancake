use crate::{
    common::http_utils::{self, logger, AppError},
    engine_ssi::{query_handlers, wasm::WasmEngine},
    oper::{
        api::{SearchRange, Statement},
        query_basic::parse as parse_query,
    },
};
use anyhow::{anyhow, Result};
use axum::{
    body::{to_bytes, Body},
    extract::{Path, State},
    http::StatusCode,
    middleware,
    routing::{delete, get, post, put},
    Router,
};
use derive_more::Constructor;
use pancake_engine_ssi::DB;
use pancake_types::{
    serde::Datum,
    types::{PrimaryKey, Value},
};
use shorthand::ShortHand;
use std::sync::Arc;

#[derive(ShortHand, Constructor)]
pub struct AppState {
    db: Arc<DB>,
    wasm_engine: WasmEngine,
}

pub fn create_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/key/:key", get(get_one))
        .route("/key/:key", put(put_one))
        .route("/key/:key", delete(delete_one))
        .route("/query", post(query))
        .route("/wasm", post(wasm))
        .layer(middleware::from_fn(logger))
        .with_state(state)
}

async fn get_one(
    Path(key): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Result<(StatusCode, String), AppError> {
    let pk = PrimaryKey(Datum::Str(key));
    let stmt = Statement::GetPK(SearchRange::One(pk));

    query_handlers::handle_stmt(state.db(), stmt).await
}

async fn put_one(
    Path(key): Path<String>,
    State(state): State<Arc<AppState>>,
    body: String,
) -> Result<(StatusCode, String), AppError> {
    let pk = PrimaryKey(Datum::Str(key));
    let pv = Value(Datum::Str(body));
    let stmt = Statement::Put(pk, Some(pv));

    query_handlers::handle_stmt(state.db(), stmt).await
}

async fn delete_one(
    Path(key): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Result<(StatusCode, String), AppError> {
    let pk = PrimaryKey(Datum::Str(key));
    let stmt = Statement::Put(pk, None);

    query_handlers::handle_stmt(state.db(), stmt).await
}

async fn query(
    State(state): State<Arc<AppState>>,
    body: String,
) -> Result<(StatusCode, String), AppError> {
    let db = state.db();

    let oper = parse_query(&body)?;

    query_handlers::handle_oper(db, oper).await
}

async fn wasm(
    State(state): State<Arc<AppState>>,
    body: Body,
) -> Result<(StatusCode, String), AppError> {
    let bytes = to_bytes(body, i32::MAX as usize)
        .await
        .map_err(|e| anyhow!(e))?;

    let retry_limit = 5;

    let body = state.wasm_engine().serve(&bytes, retry_limit).await?;

    http_utils::ok(body)
}
