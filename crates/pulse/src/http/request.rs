use std::sync::Arc;
use std::time::{Duration, SystemTime};

use axum::body::{Body, Bytes};
use axum::http::{Method, Uri};
use percent_encoding::percent_decode_str;
use tokio::time::timeout;

use crate::apps::App;
use crate::auth::{self, SignedRequest};
use crate::http::error::ApiError;
use crate::hub::Hub;

const BODY_READ_TIMEOUT: Duration = Duration::from_secs(60);

pub(crate) fn find_app(hub: &Hub, app_id: &str) -> Result<Arc<App>, ApiError> {
    hub.apps.by_id(app_id).cloned().ok_or(ApiError::AppNotFound)
}

pub(crate) async fn read_body(body: Body, max_bytes: usize) -> Result<Bytes, ApiError> {
    timeout(BODY_READ_TIMEOUT, axum::body::to_bytes(body, max_bytes))
        .await
        .map_err(|_elapsed| ApiError::BodyTimeout)?
        .map_err(ApiError::BodyRead)
}

pub(crate) fn authorize(
    app: &App,
    method: &Method,
    uri: &Uri,
    body: &[u8],
) -> Result<(), ApiError> {
    // signatures are computed over the decoded path, like the official server
    let path = percent_decode_str(uri.path()).decode_utf8_lossy();
    let request = SignedRequest {
        method: method.as_str(),
        path: &path,
        query: uri.query().unwrap_or(""),
        body,
    };

    auth::verify_request(app, &request, SystemTime::now()).map_err(|source| {
        ApiError::Unauthorized {
            app: app.key.clone(),
            path: path.clone().into_owned(),
            source,
        }
    })
}

pub(crate) fn query_param(uri: &Uri, name: &str) -> String {
    form_urlencoded::parse(uri.query().unwrap_or("").as_bytes())
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.into_owned())
        .unwrap_or_default()
}
