use std::collections::HashSet;

use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::channel::ChannelName;
use crate::config::Config;
use crate::hub::Hub;
use crate::inbound;
use crate::protocol::Outgoing;
use crate::rate_limit::RateLimiter;
use crate::session::Session;

fn session(hub: &Hub) -> Session {
    let app = hub.apps.by_key("key").unwrap();
    let conn = hub.register(app).unwrap();
    conn.take(Vec::new());
    Session {
        conn,
        channels: HashSet::new(),
        limiter: RateLimiter::new(app.client_event_rate, app.client_event_burst),
    }
}

fn hub() -> Hub {
    Hub::new(
        Config::parse(
            r#"[[apps]]
id = "app"
key = "key"
secret = "secret"
enabled = true
"#,
        )
        .unwrap(),
        CancellationToken::new(),
    )
}

fn sign(parts: &[&str]) -> String {
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;

    let mut mac = Hmac::<Sha256>::new_from_slice(b"secret").unwrap();
    for part in parts {
        mac.update(part.as_bytes());
    }
    let signature: String = mac
        .finalize()
        .into_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("key:{signature}")
}

fn subscribe(hub: &Hub, session: &mut Session, user: Option<&str>) {
    let data = user.map(|id| json!({ "user_id": id }).to_string());
    let mut parts = vec![session.conn.id.as_str(), ":", "presence-room"];
    if let Some(data) = &data {
        parts.extend([":", data]);
    }
    let auth = sign(&parts);
    inbound::handle(
        hub,
        session,
        &json!({
            "event": "pusher:subscribe",
            "data": { "channel": "presence-room", "auth": auth, "channel_data": data }
        })
        .to_string(),
    );
}

fn messages(session: &Session) -> Vec<Value> {
    session
        .conn
        .take(Vec::new())
        .messages
        .iter()
        .map(|message| serde_json::from_str(message).unwrap())
        .collect()
}

#[test]
fn presence_snapshot_includes_members_without_user_info() {
    let hub = hub();
    let mut alice = session(&hub);
    subscribe(&hub, &mut alice, Some("alice"));
    let messages = messages(&alice);
    let snapshot: Value = serde_json::from_str(messages[0]["data"].as_str().unwrap()).unwrap();
    let presence = &snapshot["presence"];
    assert_eq!(presence["count"], 1);
    assert!(presence["hash"].as_object().unwrap().contains_key("alice"));
}

#[test]
fn duplicate_presence_subscription_does_not_announce_another_join() {
    let hub = hub();
    let mut observer = session(&hub);
    let mut alice = session(&hub);
    subscribe(&hub, &mut observer, Some("observer"));
    subscribe(&hub, &mut alice, Some("alice"));
    messages(&observer);
    subscribe(&hub, &mut alice, Some("alice"));
    assert!(
        messages(&observer).is_empty(),
        "duplicate subscription must be idempotent"
    );
}

#[test]
fn replacing_presence_identity_announces_departure_before_join() {
    let hub = hub();
    let mut observer = session(&hub);
    let mut member = session(&hub);
    subscribe(&hub, &mut observer, Some("observer"));
    subscribe(&hub, &mut member, Some("alice"));
    messages(&observer);
    subscribe(&hub, &mut member, Some("bob"));
    let events = messages(&observer);
    let channel = ChannelName::try_from("presence-room".to_owned()).unwrap();
    let key = crate::channel::ChannelKey::new("key", &channel);
    if hub.presence.user_id(&key, &member.conn.id).as_deref() == Some("alice") {
        assert!(
            events.is_empty(),
            "rejecting identity changes must preserve peers"
        );
        return;
    }
    assert_eq!(
        events.len(),
        2,
        "replacing an identity must remove the previous member"
    );
    assert_eq!(events[0]["event"], "pusher_internal:member_removed");
    let removed: Value = serde_json::from_str(events[0]["data"].as_str().unwrap()).unwrap();
    assert_eq!(removed["user_id"], "alice");
    assert_eq!(events[1]["event"], "pusher_internal:member_added");
}

#[test]
fn presence_without_channel_data_uses_signed_in_identity() {
    let hub = hub();
    let mut alice = session(&hub);
    let user_data = json!({ "id": "alice" }).to_string();
    let auth = sign(&[alice.conn.id.as_str(), "::user::", &user_data]);
    inbound::handle(
        &hub,
        &mut alice,
        &json!({ "event": "pusher:signin", "data": { "auth": auth, "user_data": user_data } })
            .to_string(),
    );
    assert_eq!(alice.conn.user_id(), Some("alice"));
    messages(&alice);
    subscribe(&hub, &mut alice, None);
    let channel = ChannelName::try_from("presence-room".to_owned()).unwrap();
    let key = crate::channel::ChannelKey::new("key", &channel);
    assert_eq!(
        hub.presence.user_id(&key, &alice.conn.id).as_deref(),
        Some("alice")
    );
}

#[test]
fn outgoing_event_preserves_empty_string_data() {
    let encoded = Outgoing {
        event: "update",
        channel: Some("news"),
        data: "",
        user_id: None,
    }
    .encode();
    let event: Value = serde_json::from_str(&encoded).unwrap();
    assert_eq!(event.get("data"), Some(&Value::String(String::new())));
}
