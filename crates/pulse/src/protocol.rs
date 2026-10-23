use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::value::RawValue;

use crate::channel::ChannelName;
use crate::presence::{Member, PresenceData};
use crate::socket_id::SocketId;

pub(crate) const EVENT_PING: &str = "pusher:ping";
pub(crate) const EVENT_SUBSCRIBE: &str = "pusher:subscribe";
pub(crate) const EVENT_UNSUBSCRIBE: &str = "pusher:unsubscribe";
pub(crate) const EVENT_SIGNIN: &str = "pusher:signin";
pub(crate) const CLIENT_EVENT_PREFIX: &str = "client-";

pub(crate) const OVER_CAPACITY: &str =
    r#"{"event":"pusher:error","data":{"message":"Over capacity","code":4100}}"#;

#[derive(Debug, Clone, Copy)]
pub(crate) enum ErrorCode {
    ConnectionUnauthorized,
    OverCapacity,
    InvalidSignature,
    ClientEventRateLimited,
}

impl From<ErrorCode> for u16 {
    fn from(code: ErrorCode) -> Self {
        match code {
            ErrorCode::ConnectionUnauthorized => 4009,
            ErrorCode::OverCapacity => 4100,
            ErrorCode::InvalidSignature => 4101,
            ErrorCode::ClientEventRateLimited => 4301,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct Incoming {
    #[serde(default)]
    pub(crate) event: String,
    pub(crate) channel: Option<String>,
    data: Option<Box<RawValue>>,
}

impl Incoming {
    // pusher clients send data either as a json encoded string or as an inline object
    pub(crate) fn data(&self) -> String {
        let Some(raw) = &self.data else {
            return String::new();
        };

        serde_json::from_str::<String>(raw.get())
            .unwrap_or_else(|_not_a_string| raw.get().to_owned())
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct Outgoing<'a> {
    pub(crate) event: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) channel: Option<&'a str>,
    #[serde(skip_serializing_if = "str::is_empty")]
    pub(crate) data: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) user_id: Option<&'a str>,
}

impl Outgoing<'_> {
    pub(crate) fn encode(&self) -> String {
        to_json(self)
    }
}

#[expect(
    clippy::expect_used,
    reason = "only string keyed structs and maps are serialized, which cannot fail"
)]
fn to_json(value: &impl Serialize) -> String {
    serde_json::to_string(value).expect("string keyed data serializes")
}

fn with_data(event: &str, channel: Option<&ChannelName>, data: &impl Serialize) -> String {
    let data = to_json(data);
    Outgoing {
        event,
        channel: channel.map(ChannelName::as_str),
        data: &data,
        user_id: None,
    }
    .encode()
}

pub(crate) fn connection_established(socket_id: &SocketId, activity_timeout_secs: u64) -> String {
    let data =
        json!({ "socket_id": socket_id.as_str(), "activity_timeout": activity_timeout_secs });
    with_data("pusher:connection_established", None, &data)
}

pub(crate) fn error(message: &str, code: Option<ErrorCode>) -> String {
    let data = match code {
        Some(code) => json!({ "message": message, "code": u16::from(code) }),
        None => json!({ "message": message }),
    };

    with_data("pusher:error", None, &data)
}

pub(crate) fn pong() -> String {
    Outgoing {
        event: "pusher:pong",
        channel: None,
        data: "{}",
        user_id: None,
    }
    .encode()
}

pub(crate) fn subscription_succeeded(channel: &ChannelName) -> String {
    let event = "pusher_internal:subscription_succeeded";
    Outgoing {
        event,
        channel: Some(channel.as_str()),
        data: "{}",
        user_id: None,
    }
    .encode()
}

pub(crate) fn presence_subscription_succeeded(
    channel: &ChannelName,
    presence: &PresenceData,
) -> String {
    with_data(
        "pusher_internal:subscription_succeeded",
        Some(channel),
        &json!({ "presence": presence }),
    )
}

pub(crate) fn member_added(channel: &ChannelName, member: &Member) -> String {
    with_data("pusher_internal:member_added", Some(channel), member)
}

pub(crate) fn member_removed(channel: &ChannelName, user_id: &str) -> String {
    with_data(
        "pusher_internal:member_removed",
        Some(channel),
        &json!({ "user_id": user_id }),
    )
}

pub(crate) fn signin_success(user_data: &str) -> String {
    with_data(
        "pusher:signin_success",
        None,
        &json!({ "user_data": user_data }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_string_and_inline_data() {
        let cases = [
            (r#"{"event":"e","data":"{\"a\":1}"}"#, r#"{"a":1}"#),
            (r#"{"event":"e","data":{"a":1}}"#, r#"{"a":1}"#),
            (r#"{"event":"e","data":null}"#, ""),
            (r#"{"event":"e"}"#, ""),
        ];

        for (input, expected) in cases {
            let msg: Incoming = serde_json::from_str(input).unwrap();
            assert_eq!(msg.data(), expected, "data of {input}");
        }
    }

    #[test]
    fn encodes_data_as_json_string() {
        let channel = ChannelName::try_from("chat".to_owned()).unwrap();

        assert_eq!(
            member_removed(&channel, "u1"),
            r#"{"event":"pusher_internal:member_removed","channel":"chat","data":"{\"user_id\":\"u1\"}"}"#,
            "member removed"
        );
        assert_eq!(
            error("nope", Some(ErrorCode::ConnectionUnauthorized)),
            r#"{"event":"pusher:error","data":"{\"code\":4009,\"message\":\"nope\"}"}"#,
            "error with code"
        );
    }

    #[test]
    fn omits_empty_fields() {
        let event = Outgoing {
            event: "e",
            channel: None,
            data: "",
            user_id: None,
        };

        assert_eq!(
            event.encode(),
            r#"{"event":"e"}"#,
            "empty fields are omitted"
        );
    }
}
