use std::sync::Arc;

use axum::extract::{FromRef, State};
use axum::http::header;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{Value, json};
use tokio_util::task::TaskTracker;

use crate::http::{channels, events, users, websocket};
use crate::hub::Hub;

#[derive(Debug, Clone)]
pub(crate) struct Shared {
    pub(crate) hub: Arc<Hub>,
    pub(crate) tracker: TaskTracker,
}

impl FromRef<Shared> for Arc<Hub> {
    fn from_ref(input: &Shared) -> Self {
        Self::clone(&input.hub)
    }
}

pub(crate) fn router(hub: Arc<Hub>, tracker: TaskTracker) -> Router {
    Router::new()
        .route("/app/{key}", get(websocket::upgrade))
        .route("/apps/{app_id}/events", post(events::events))
        .route("/apps/{app_id}/batch_events", post(events::batch_events))
        .route("/apps/{app_id}/channels", get(channels::list))
        .route("/apps/{app_id}/channels/{channel}", get(channels::show))
        .route(
            "/apps/{app_id}/channels/{channel}/users",
            get(channels::users),
        )
        .route(
            "/apps/{app_id}/users/{user_id}/terminate_connections",
            post(users::terminate_connections),
        )
        .route("/stats", get(stats))
        .route("/apps", get(apps))
        .route("/metrics", get(metrics))
        .route("/health", get(health))
        .with_state(Shared { hub, tracker })
}

async fn health() -> &'static str {
    "OK"
}

async fn stats(State(hub): State<Arc<Hub>>) -> Json<Value> {
    Json(json!({
        "current_connections": hub.connection_count(),
        "max_connections": hub.max_connections,
        "total_channels": hub.channels.len(),
        "apps_loaded": hub.apps.len(),
    }))
}

async fn apps(State(hub): State<Arc<Hub>>) -> Json<Value> {
    let apps: Vec<Value> = hub
        .apps
        .iter()
        .map(|app| json!({ "id": app.id, "key": app.key }))
        .collect();
    Json(json!({ "count": apps.len(), "apps": apps }))
}

async fn metrics(State(hub): State<Arc<Hub>>) -> impl IntoResponse {
    let connections = hub
        .app_connection_counts()
        .iter()
        .map(|(app_key, count)| {
            let app_key = app_key
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('\n', "\\n");
            format!("pulse_connections_active{{app_key=\"{app_key}\"}} {count}\n")
        })
        .collect::<Vec<_>>()
        .concat();
    let body = format!(
        "# HELP pulse_connections_active Number of active WebSocket connections\n\
         # TYPE pulse_connections_active gauge\n\
         {connections}\
         # HELP pulse_channels_active Number of active channels\n\
         # TYPE pulse_channels_active gauge\n\
         pulse_channels_active {channels}\n\
         # HELP pulse_apps_loaded Number of apps loaded from configuration\n\
         # TYPE pulse_apps_loaded gauge\n\
         pulse_apps_loaded {apps}\n",
        channels = hub.channels.len(),
        apps = hub.apps.len(),
    );

    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn metrics_escapes_special_characters_in_app_keys() {
        let app_key = "key\"\\\nlabel";
        let config = Config::parse(&format!(
            "[[apps]]\nid = 'app'\nkey = {}\nsecret = 'secret'\nenabled = true\n",
            serde_json::to_string(app_key).unwrap()
        ))
        .unwrap();
        let hub = Arc::new(Hub::new(config, CancellationToken::new()));
        let conn = hub.register(hub.apps.by_key(app_key).unwrap()).unwrap();
        let response = metrics(State(Arc::clone(&hub))).await.into_response();
        let body = axum::body::to_bytes(response.into_body(), 16 * 1024)
            .await
            .unwrap();
        let body = std::str::from_utf8(&body).unwrap();
        assert!(
            body.lines()
                .any(|line| line == r#"pulse_connections_active{app_key="key\"\\\nlabel"} 1"#),
            "metrics must escape quotes, backslashes and newlines: {body}"
        );
        drop(conn);
    }
}
