use std::sync::Arc;

use serde::Deserialize;
use tracing::debug;

use crate::auth;
use crate::channel::{ChannelName, InvalidChannelName};
use crate::connection::Connection;
use crate::hub::Hub;
use crate::protocol::{self, ErrorCode, Incoming};
use crate::session::Session;
use crate::subscriptions::Subscription;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
struct SubscribeData {
    #[serde(default)]
    channel: String,
    auth: Option<String>,
    channel_data: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, rename_all = "snake_case")]
struct SigninData {
    auth: String,
    user_data: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
struct SignedInUser {
    id: String,
}

#[derive(Debug, thiserror::Error)]
enum InvalidSubscription {
    #[error("parse subscription data")]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Channel(#[from] InvalidChannelName),
}

pub(crate) fn handle(hub: &Hub, session: &mut Session, text: &str) {
    let msg: Incoming = match serde_json::from_str(text) {
        Ok(msg) => msg,
        Err(err) => {
            debug!(connection = %session.conn.id, error = %err, "parse message");
            session.conn.send_error("Invalid JSON", None);
            return;
        }
    };

    match msg.event.as_str() {
        protocol::EVENT_PING => session.conn.send_text(protocol::pong()),
        protocol::EVENT_SUBSCRIBE => subscribe(hub, session, &msg),
        protocol::EVENT_UNSUBSCRIBE => unsubscribe(hub, session, &msg),
        protocol::EVENT_SIGNIN => signin(&session.conn, &msg),
        _ => client_event(hub, session, &msg),
    }
}

fn parse_subscription(data: &str) -> Result<Subscription, InvalidSubscription> {
    let data: SubscribeData = serde_json::from_str(data)?;
    let channel = ChannelName::try_from(data.channel)?;

    Ok(Subscription {
        channel,
        auth: data.auth,
        channel_data: data.channel_data,
    })
}

fn subscribe(hub: &Hub, session: &mut Session, msg: &Incoming) {
    let data = msg.data();
    let data = if data.is_empty() { "{}" } else { data.as_str() };

    match parse_subscription(data) {
        Ok(subscription) => hub.subscribe(session, subscription),
        Err(err) => {
            debug!(connection = %session.conn.id, error = %err, "invalid subscription");
            session.conn.send_error("Invalid subscription data", None);
        }
    }
}

fn unsubscribe(hub: &Hub, session: &mut Session, msg: &Incoming) {
    match parse_subscription(&msg.data()) {
        Ok(subscription) => hub.unsubscribe(session, &subscription.channel),
        Err(err) => {
            debug!(connection = %session.conn.id, error = %err, "invalid unsubscription");
            session.conn.send_error("Invalid subscription data", None);
        }
    }
}

fn signin(conn: &Connection, msg: &Incoming) {
    let data = match serde_json::from_str::<SigninData>(&msg.data()) {
        Ok(data) => data,
        Err(err) => {
            debug!(connection = %conn.id, error = %err, "invalid signin");
            return conn.send_error("Invalid signin data", None);
        }
    };

    let unauthorized = Some(ErrorCode::ConnectionUnauthorized);
    if conn.user_id().is_some() {
        return conn.send_error("Connection is already signed in", unauthorized);
    }
    if !auth::verify_user(&conn.app, &data.auth, &conn.id, &data.user_data) {
        return conn.send_error("Invalid signin signature", unauthorized);
    }

    let user = match serde_json::from_str::<SignedInUser>(&data.user_data) {
        Ok(user) if !user.id.is_empty() => user,
        Ok(_) | Err(_) => return conn.send_error("user_data must contain an id", unauthorized),
    };
    if conn.sign_in(user.id) {
        conn.send_text(protocol::signin_success(&data.user_data));
    }
}

fn client_event(hub: &Hub, session: &mut Session, msg: &Incoming) {
    if !msg.event.starts_with(protocol::CLIENT_EVENT_PREFIX) {
        return;
    }

    let conn = Arc::clone(&session.conn);
    let unauthorized = Some(ErrorCode::ConnectionUnauthorized);
    if !conn.app.allows_client_events {
        conn.send_error("Client events are not enabled for this app", unauthorized);
        return;
    }

    let Some(channel) = msg.channel.clone() else {
        conn.send_error("Client events require a channel", unauthorized);
        return;
    };
    // a malformed name can never have been subscribed to
    let Ok(channel) = ChannelName::try_from(channel) else {
        return;
    };

    if !channel.kind().allows_client_events() {
        conn.send_error(
            "Client events are only supported on private and presence channels",
            unauthorized,
        );
        return;
    }

    if !session.channels.contains(&channel) {
        return;
    }
    if !session.limiter.allow() {
        conn.send_error(
            "Rate limit exceeded for client events",
            Some(ErrorCode::ClientEventRateLimited),
        );
        return;
    }
    hub.forward_client_event(&conn, &channel, msg);
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use serde_json::json;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::config::Config;
    use crate::rate_limit::RateLimiter;

    #[test]
    fn unsubscribed_client_events_do_not_use_rate_limit() {
        let hub = Hub::new(
            Config::parse(
                r#"[[apps]]
id = "app"
key = "key"
secret = "secret"
enabled = true
enable_client_events = true
max_event_rate = 1
max_event_burst = 1
"#,
            )
            .unwrap(),
            CancellationToken::new(),
        );
        let app = hub.apps.by_key("key").unwrap();
        let conn = hub.register(app).unwrap();
        conn.take(Vec::new());
        let mut session = Session {
            conn,
            channels: HashSet::new(),
            limiter: RateLimiter::new(app.client_event_rate, app.client_event_burst),
        };
        let event = json!({ "event": "client-typing", "channel": "private-room", "data": "{}" });

        handle(&hub, &mut session, &event.to_string());
        handle(&hub, &mut session, &event.to_string());
        assert!(
            session.conn.take(Vec::new()).messages.is_empty(),
            "unsubscribed events are dropped without a rate limit error"
        );

        session
            .channels
            .insert(ChannelName::try_from("private-room".to_owned()).unwrap());
        handle(&hub, &mut session, &event.to_string());
        assert!(
            session.conn.take(Vec::new()).messages.is_empty(),
            "the first subscribed event is within the burst"
        );
    }
}
