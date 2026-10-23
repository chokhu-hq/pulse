use std::collections::HashMap;
use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::{Method, Uri};
use serde::Serialize;
use serde_json::{Value, json};

use crate::apps::App;
use crate::channel::{self, ChannelKey, ChannelKind, ChannelName};
use crate::http::error::ApiError;
use crate::http::request::{authorize, find_app, query_param};
use crate::hub::Hub;

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct ChannelInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    subscription_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    user_count: Option<usize>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
struct ChannelStatus {
    occupied: bool,
    #[serde(flatten)]
    info: ChannelInfo,
}

pub(crate) fn channel_info(hub: &Hub, app: &App, channel: &ChannelName, info: &str) -> ChannelInfo {
    let key = ChannelKey::new(&app.key, channel);
    let is_presence = matches!(channel.kind(), ChannelKind::Presence);
    let mut result = ChannelInfo::default();
    for attribute in info.split(',').map(str::trim) {
        match attribute {
            "subscription_count" => {
                result.subscription_count = Some(hub.channels.subscriber_count(&key));
            }
            "user_count" if is_presence => result.user_count = Some(hub.presence.user_count(&key)),
            _ => {}
        }
    }
    result
}

fn wants_user_count(info: &str) -> bool {
    info.split(',').any(|attribute| attribute == "user_count")
}

pub(crate) async fn list(
    State(hub): State<Arc<Hub>>,
    Path(app_id): Path<String>,
    (method, uri): (Method, Uri),
) -> Result<Json<Value>, ApiError> {
    let app = find_app(&hub, &app_id)?;
    authorize(&app, &method, &uri, &[])?;

    let info = query_param(&uri, "info");
    let prefix = query_param(&uri, "filter_by_prefix");
    if wants_user_count(&info) && !channel::is_presence_prefix(&prefix) {
        return Err(ApiError::UserCountNeedsPresencePrefix);
    }

    let channels = hub
        .channels
        .names(&app.key, &prefix)
        .into_iter()
        .map(|channel| {
            let info = channel_info(&hub, &app, &channel, &info);
            (channel.to_string(), info)
        })
        .collect::<HashMap<_, _>>();
    Ok(Json(json!({ "channels": channels })))
}

pub(crate) async fn show(
    State(hub): State<Arc<Hub>>,
    Path((app_id, channel)): Path<(String, String)>,
    (method, uri): (Method, Uri),
) -> Result<Json<Value>, ApiError> {
    let app = find_app(&hub, &app_id)?;
    authorize(&app, &method, &uri, &[])?;

    let channel = ChannelName::try_from(channel)?;
    let info = query_param(&uri, "info");
    if wants_user_count(&info) && !matches!(channel.kind(), ChannelKind::Presence) {
        return Err(ApiError::UserCountNeedsPresenceChannel);
    }

    let key = ChannelKey::new(&app.key, &channel);
    let status = ChannelStatus {
        occupied: hub.channels.subscriber_count(&key) > 0,
        info: channel_info(&hub, &app, &channel, &info),
    };
    Ok(Json(json!(status)))
}

pub(crate) async fn users(
    State(hub): State<Arc<Hub>>,
    Path((app_id, channel)): Path<(String, String)>,
    (method, uri): (Method, Uri),
) -> Result<Json<Value>, ApiError> {
    let app = find_app(&hub, &app_id)?;
    authorize(&app, &method, &uri, &[])?;

    let channel = ChannelName::try_from(channel)?;
    if !matches!(channel.kind(), ChannelKind::Presence) {
        return Err(ApiError::UsersNeedPresenceChannel);
    }

    let key = ChannelKey::new(&app.key, &channel);
    let users: Vec<Value> = hub
        .presence
        .data(&key)
        .ids
        .into_iter()
        .map(|id| json!({ "id": id }))
        .collect();
    Ok(Json(json!({ "users": users })))
}
