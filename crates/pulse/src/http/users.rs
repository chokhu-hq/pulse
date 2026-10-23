use std::sync::Arc;

use axum::Json;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{Method, Uri};
use serde_json::{Value, json};

use crate::http::error::ApiError;
use crate::http::request::{authorize, find_app, read_body};
use crate::hub::Hub;

const MAX_BODY_BYTES: usize = 10 * 1024;

pub(crate) async fn terminate_connections(
    State(hub): State<Arc<Hub>>,
    Path((app_id, user_id)): Path<(String, String)>,
    (method, uri): (Method, Uri),
    body: Body,
) -> Result<Json<Value>, ApiError> {
    let app = find_app(&hub, &app_id)?;
    let body = read_body(body, MAX_BODY_BYTES).await?;
    authorize(&app, &method, &uri, &body)?;

    hub.terminate_user_connections(&app.key, &user_id);
    Ok(Json(json!({})))
}
