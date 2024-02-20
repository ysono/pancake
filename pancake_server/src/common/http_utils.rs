use anyhow;
use axum::{
    http::{Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use derive_more::From;
use pancake_engine_common::Entry;
use std::fmt::Debug;

pub async fn logger(req: Request<axum::body::Body>, next: Next) -> impl IntoResponse {
    println!("{} {}", req.method(), req.uri().path());
    next.run(req).await
}

#[derive(From)]
pub struct AppError(pub anyhow::Error);
impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, self.0.to_string()).into_response()
    }
}

pub fn ok<S: Into<String>>(body: S) -> Result<(StatusCode, String), AppError> {
    let body = body.into();
    if body.len() == 0 {
        Ok((StatusCode::NO_CONTENT, body))
    } else {
        Ok((StatusCode::OK, body))
    }
}

pub fn entries_to_string<'a, K, V>(
    entries: impl Iterator<Item = Entry<'a, K, V>>,
) -> Result<String, anyhow::Error>
where
    K: 'a + Debug,
    V: 'a + Debug,
{
    let mut body = String::new();
    for entry in entries {
        let (pk, pv) = entry.try_borrow()?;
        kv_to_string(&mut body, pk, pv);
    }
    Ok(body)
}

pub fn kv_to_string<K, V>(body: &mut String, k: &K, v: &V)
where
    K: Debug,
    V: Debug,
{
    let s = format!("Key:\r\n{k:?}\r\nValue:\r\n{v:?}\r\n");
    body.push_str(&s);
}
