use std::collections::HashMap;
use std::sync::Arc;

use axum::Json;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{Method, Uri};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::apps::App;
use crate::channel::{ChannelKey, ChannelName};
use crate::http::channels::{ChannelInfo, channel_info};
use crate::http::error::ApiError;
use crate::http::request::{authorize, find_app, read_body};
use crate::hub::Hub;
use crate::protocol::Outgoing;
use crate::socket_id::SocketId;

const MIN_EVENT_BODY_BYTES: usize = 10 * 1024;
const MIN_BATCH_BODY_BYTES: usize = 100 * 1024;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
struct Trigger {
    name: Option<String>,
    channel: Option<String>,
    channels: Option<Vec<String>>,
    data: Option<String>,
    socket_id: Option<String>,
    info: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
struct Batch {
    batch: Option<Vec<BatchEvent>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
struct BatchEvent {
    name: Option<String>,
    channel: Option<String>,
    data: Option<String>,
    socket_id: Option<String>,
    info: Option<String>,
}

#[derive(Debug)]
struct ValidEvent {
    channel: ChannelName,
    exclude: Option<SocketId>,
    name: String,
    data: String,
    info: String,
}

impl TryFrom<BatchEvent> for ValidEvent {
    type Error = ApiError;

    fn try_from(event: BatchEvent) -> Result<Self, Self::Error> {
        Ok(Self {
            channel: ChannelName::try_from(event.channel.unwrap_or_default())?,
            exclude: parse_socket_id(event.socket_id)?,
            name: event.name.unwrap_or_default(),
            data: event.data.unwrap_or_default(),
            info: event.info.unwrap_or_default(),
        })
    }
}

#[derive(Debug)]
struct Publication<'a> {
    channel: &'a ChannelName,
    event: &'a str,
    data: &'a str,
    exclude: Option<&'a SocketId>,
}

fn publish(hub: &Hub, app: &App, publication: &Publication<'_>) {
    let key = ChannelKey::new(&app.key, publication.channel);
    hub.channels.broadcast(&key, publication.exclude, || {
        let Publication {
            channel,
            event,
            data,
            ..
        } = publication;
        Outgoing {
            event,
            channel: Some(channel.as_str()),
            data,
            user_id: None,
        }
        .encode()
    });
}

fn parse_socket_id(socket_id: Option<String>) -> Result<Option<SocketId>, ApiError> {
    Ok(socket_id
        .filter(|id| !id.is_empty())
        .map(|id| id.parse())
        .transpose()?)
}

pub(crate) async fn events(
    State(hub): State<Arc<Hub>>,
    Path(app_id): Path<String>,
    (method, uri): (Method, Uri),
    body: Body,
) -> Result<Json<Value>, ApiError> {
    let app = find_app(&hub, &app_id)?;
    let body = read_body(body, app.max_message_bytes.get().max(MIN_EVENT_BODY_BYTES)).await?;
    authorize(&app, &method, &uri, &body)?;

    let trigger: Trigger = serde_json::from_slice(&body)?;
    let channels = trigger
        .channels
        .into_iter()
        .flatten()
        .chain(trigger.channel.filter(|channel| !channel.is_empty()))
        .map(ChannelName::try_from)
        .collect::<Result<Vec<_>, _>>()?;
    let exclude = parse_socket_id(trigger.socket_id)?;
    let (event, data) = (
        trigger.name.unwrap_or_default(),
        trigger.data.unwrap_or_default(),
    );

    for channel in &channels {
        publish(
            &hub,
            &app,
            &Publication {
                channel,
                event: &event,
                data: &data,
                exclude: exclude.as_ref(),
            },
        );
    }

    let Some(info) = trigger.info.filter(|info| !info.is_empty()) else {
        return Ok(Json(json!({})));
    };
    let infos: HashMap<&str, ChannelInfo> = channels
        .iter()
        .map(|channel| (channel.as_str(), channel_info(&hub, &app, channel, &info)))
        .collect();
    Ok(Json(json!({ "channels": infos })))
}

pub(crate) async fn batch_events(
    State(hub): State<Arc<Hub>>,
    Path(app_id): Path<String>,
    (method, uri): (Method, Uri),
    body: Body,
) -> Result<Json<Value>, ApiError> {
    let app = find_app(&hub, &app_id)?;
    let max_bytes = app
        .max_message_bytes
        .get()
        .saturating_mul(10)
        .max(MIN_BATCH_BODY_BYTES);
    let body = read_body(body, max_bytes).await?;
    authorize(&app, &method, &uri, &body)?;

    let batch = serde_json::from_slice::<Batch>(&body)?
        .batch
        .unwrap_or_default();
    let max = app.max_batch_events.get();
    if batch.len() > max {
        return Err(ApiError::BatchTooLarge { max });
    }

    let events = batch
        .into_iter()
        .map(ValidEvent::try_from)
        .collect::<Result<Vec<_>, _>>()?;
    let responses: Vec<ChannelInfo> = events
        .iter()
        .map(|event| {
            let ValidEvent {
                channel,
                exclude,
                name,
                data,
                info,
            } = event;
            publish(
                &hub,
                &app,
                &Publication {
                    channel,
                    event: name,
                    data,
                    exclude: exclude.as_ref(),
                },
            );
            channel_info(&hub, &app, channel, info)
        })
        .collect();
    Ok(Json(json!({ "batch": responses })))
}
