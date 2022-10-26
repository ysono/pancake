use anyhow::{anyhow, Result};
use hyper::{Body, Response, StatusCode};

pub fn no_content() -> Result<Response<Body>> {
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .map_err(|e| anyhow!(e))
}

pub fn err(e: anyhow::Error) -> Result<Response<Body>> {
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(Body::from(e.to_string()))
        .map_err(|e| anyhow!(e))
}

pub fn ok(body: String) -> Result<Response<Body>> {
    Response::builder()
        .status(StatusCode::OK)
        .body(Body::from(body))
        .map_err(|e| anyhow!(e))
}
