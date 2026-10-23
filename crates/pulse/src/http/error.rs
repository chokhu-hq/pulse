use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use tracing::{debug, warn};

use crate::auth::RequestAuthError;
use crate::channel::InvalidChannelName;
use crate::socket_id::InvalidSocketId;

// response bodies match the previous go server so existing clients see the same messages
#[derive(Debug)]
pub(crate) enum ApiError {
    AppNotFound,
    UnknownApp,
    OriginNotAllowed,
    BodyRead(axum::Error),
    BodyTimeout,
    Unauthorized {
        app: String,
        path: String,
        source: RequestAuthError,
    },
    InvalidJson(serde_json::Error),
    InvalidChannel(InvalidChannelName),
    InvalidSocketId(InvalidSocketId),
    BatchTooLarge {
        max: usize,
    },
    UserCountNeedsPresencePrefix,
    UserCountNeedsPresenceChannel,
    UsersNeedPresenceChannel,
}

impl From<InvalidChannelName> for ApiError {
    fn from(err: InvalidChannelName) -> Self {
        Self::InvalidChannel(err)
    }
}

impl From<InvalidSocketId> for ApiError {
    fn from(err: InvalidSocketId) -> Self {
        Self::InvalidSocketId(err)
    }
}

impl From<serde_json::Error> for ApiError {
    fn from(err: serde_json::Error) -> Self {
        Self::InvalidJson(err)
    }
}

impl ApiError {
    fn status_and_body(&self) -> (StatusCode, String) {
        let bad_request = |body: &str| (StatusCode::BAD_REQUEST, body.to_owned());
        match self {
            Self::AppNotFound => (StatusCode::NOT_FOUND, "App not found".to_owned()),
            Self::UnknownApp => (StatusCode::NOT_FOUND, "Unknown app".to_owned()),
            Self::OriginNotAllowed => (StatusCode::FORBIDDEN, "Origin not allowed".to_owned()),
            Self::BodyRead(_) => bad_request("Failed to read request body"),
            Self::BodyTimeout => (
                StatusCode::REQUEST_TIMEOUT,
                "Timed out reading request body".to_owned(),
            ),
            Self::Unauthorized { source, .. } => (
                StatusCode::UNAUTHORIZED,
                format!("Authentication failed: {source}"),
            ),
            Self::InvalidJson(_) => bad_request("Invalid JSON"),
            Self::InvalidChannel(_) => bad_request("Invalid channel name"),
            Self::InvalidSocketId(_) => bad_request("Invalid socket_id"),
            Self::BatchTooLarge { max } => (
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("Batch size exceeds maximum of {max} events"),
            ),
            Self::UserCountNeedsPresencePrefix => {
                bad_request("user_count requires filter_by_prefix=presence-")
            }
            Self::UserCountNeedsPresenceChannel => {
                bad_request("user_count is only available for presence channels")
            }
            Self::UsersNeedPresenceChannel => {
                bad_request("Users are only available for presence channels")
            }
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, body) = self.status_and_body();
        match &self {
            Self::Unauthorized { app, path, source } => {
                warn!(error = %source, app = %app, path = %path, "authentication failed");
            }
            Self::BodyRead(err) => debug!(error = %err, "read request body"),
            Self::InvalidJson(err) => debug!(error = %err, "parse request body"),
            _ => debug!(status = %status, body = %body, "rejected request"),
        }

        (status, body).into_response()
    }
}
