use axum::extract::ws::WebSocketUpgrade;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, header};
use axum::response::Response;
use tracing::warn;

use crate::http::error::ApiError;
use crate::http::router::Shared;
use crate::session;

// pusher messages are small; tungstenite would otherwise allocate 128 KiB per connection
const READ_BUFFER_BYTES: usize = 4096;

pub(crate) async fn upgrade(
    State(shared): State<Shared>,
    Path(key): Path<String>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Result<Response, ApiError> {
    let Some(app) = shared.hub.apps.by_key(&key).cloned() else {
        warn!(key = %key, "unknown app key");
        return Err(ApiError::UnknownApp);
    };

    let origin = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok());
    if !app.origins.allows(origin) {
        warn!(app = %key, origin = origin.unwrap_or_default(), "origin not allowed");
        return Err(ApiError::OriginNotAllowed);
    }

    let max_bytes = app.max_message_bytes.get();
    let Shared { hub, tracker } = shared;
    Ok(ws
        .read_buffer_size(READ_BUFFER_BYTES)
        .max_message_size(max_bytes)
        .max_frame_size(max_bytes)
        .on_upgrade(move |socket| tracker.track_future(session::run(hub, app, socket))))
}
