use anyhow::{anyhow, Result};
use hyper::{Body, Response, StatusCode};

pub fn no_content() -> Result<Response<Body>> {
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .map_err(|e| anyhow!(e))
}

pub fn not_modified<B: Into<Body>>(body: B) -> Result<Response<Body>> {
    Response::builder()
        .status(StatusCode::NOT_MODIFIED)
        .body(body.into())
        .map_err(|e| anyhow!(e))
}

pub fn err(e: anyhow::Error) -> Result<Response<Body>> {
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(Body::from(e.to_string()))
        .map_err(|e| anyhow!(e))
}

pub fn ok<B: Into<Body>>(body: B) -> Result<Response<Body>> {
    Response::builder()
        .status(StatusCode::OK)
        .body(body.into())
        .map_err(|e| anyhow!(e))
}
