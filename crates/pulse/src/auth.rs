use std::num::ParseIntError;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hmac::{Hmac, KeyInit, Mac};
use md5::{Digest, Md5};
use sha2::Sha256;
use subtle::ConstantTimeEq;

use crate::apps::{App, Secret};
use crate::channel::ChannelName;
use crate::socket_id::SocketId;

const MAX_CLOCK_SKEW: Duration = Duration::from_secs(600);

#[derive(Debug)]
pub(crate) struct ChannelAuth<'a> {
    pub(crate) socket_id: &'a SocketId,
    pub(crate) channel: &'a ChannelName,
    pub(crate) channel_data: Option<&'a str>,
}

#[derive(Debug)]
pub(crate) struct SignedRequest<'a> {
    pub(crate) method: &'a str,
    pub(crate) path: &'a str,
    pub(crate) query: &'a str,
    pub(crate) body: &'a [u8],
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum RequestAuthError {
    #[error("missing {0}")]
    Missing(&'static str),
    #[error("invalid auth_key")]
    InvalidKey,
    #[error("unsupported auth_version: {0}")]
    UnsupportedVersion(String),
    #[error("invalid auth_timestamp")]
    InvalidTimestamp(#[source] ParseIntError),
    #[error("auth_timestamp too old or in future")]
    Expired,
    #[error("missing body_md5 for non-empty body")]
    MissingBodyMd5,
    #[error("body_md5 mismatch")]
    BodyMd5Mismatch,
    #[error("invalid auth_signature")]
    InvalidSignature,
}

fn to_hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .flat_map(|b| [b >> 4, b & 0x0f])
        .filter_map(|n| char::from_digit(u32::from(n), 16))
        .collect()
}

#[expect(clippy::expect_used, reason = "hmac accepts keys of any length")]
fn sign(secret: &Secret, parts: &[&str]) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("hmac accepts any key length");
    for part in parts {
        mac.update(part.as_bytes());
    }

    to_hex(&mac.finalize().into_bytes())
}

fn constant_time_eq(a: &str, b: &str) -> bool {
    a.as_bytes().ct_eq(b.as_bytes()).into()
}

fn matches_app_signature(app: &App, auth: &str, signature: &str) -> bool {
    let Some(signed) = auth
        .strip_prefix(app.key.as_str())
        .and_then(|rest| rest.strip_prefix(':'))
    else {
        return false;
    };

    constant_time_eq(signed, signature)
}

pub(crate) fn verify_channel(app: &App, auth: &str, request: &ChannelAuth<'_>) -> bool {
    let (socket_id, channel) = (request.socket_id.as_str(), request.channel.as_str());
    let signature = match request.channel_data {
        Some(data) => sign(&app.secret, &[socket_id, ":", channel, ":", data]),
        None => sign(&app.secret, &[socket_id, ":", channel]),
    };

    matches_app_signature(app, auth, &signature)
}

pub(crate) fn verify_user(app: &App, auth: &str, socket_id: &SocketId, user_data: &str) -> bool {
    let signature = sign(&app.secret, &[socket_id.as_str(), "::user::", user_data]);
    matches_app_signature(app, auth, &signature)
}

fn param<'a>(params: &'a [(String, String)], name: &str) -> Option<&'a str> {
    params
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.as_str())
        .filter(|value| !value.is_empty())
}

pub(crate) fn verify_request(
    app: &App,
    request: &SignedRequest<'_>,
    now: SystemTime,
) -> Result<(), RequestAuthError> {
    let mut params: Vec<(String, String)> = form_urlencoded::parse(request.query.as_bytes())
        .into_owned()
        .collect();
    let required = |name| param(&params, name).ok_or(RequestAuthError::Missing(name));

    let key = required("auth_key")?;
    let timestamp = required("auth_timestamp")?;
    let version = required("auth_version")?;
    let signature = required("auth_signature")?.to_owned();

    if key != app.key {
        return Err(RequestAuthError::InvalidKey);
    }
    if version != "1.0" {
        return Err(RequestAuthError::UnsupportedVersion(version.to_owned()));
    }

    let timestamp: u64 = timestamp
        .parse()
        .map_err(RequestAuthError::InvalidTimestamp)?;
    let now = now
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs());
    if now.abs_diff(timestamp) > MAX_CLOCK_SKEW.as_secs() {
        return Err(RequestAuthError::Expired);
    }

    if !request.body.is_empty() {
        let body_md5 = param(&params, "body_md5").ok_or(RequestAuthError::MissingBodyMd5)?;
        if body_md5 != to_hex(&Md5::digest(request.body)) {
            return Err(RequestAuthError::BodyMd5Mismatch);
        }
    }

    params.retain(|(key, _)| key != "auth_signature");
    params.sort_by(|a, b| a.0.cmp(&b.0));
    let query = params
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&");
    let expected = sign(
        &app.secret,
        &[
            &request.method.to_uppercase(),
            "\n",
            request.path,
            "\n",
            &query,
        ],
    );

    if !constant_time_eq(&signature, &expected) {
        return Err(RequestAuthError::InvalidSignature);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apps::AppFile;

    // credentials and expected signatures from the pusher authentication docs
    fn docs_app() -> App {
        let json = r#"{"id":"3","key":"278d425bdf160c739803","secret":"7ad3773142a6692b25b8","enabled":true}"#;
        App::try_from(serde_json::from_str::<AppFile>(json).unwrap()).unwrap()
    }

    fn channel(name: &str) -> ChannelName {
        ChannelName::try_from(name.to_owned()).unwrap()
    }

    #[test]
    fn verifies_private_channel_auth() {
        let app = docs_app();
        let socket_id: SocketId = "1234.1234".parse().unwrap();
        let channel = channel("private-foobar");
        let request = ChannelAuth {
            socket_id: &socket_id,
            channel: &channel,
            channel_data: None,
        };
        let auth =
            "278d425bdf160c739803:58df8b0c36d6982b82c3ecf6b4662e34fe8c25bba48f5369f135bf843651c3a4";

        assert!(verify_channel(&app, auth, &request), "docs signature");
        assert!(
            !verify_channel(&app, "278d425bdf160c739803:deadbeef", &request),
            "bad signature"
        );
        assert!(
            !verify_channel(&app, &auth.replacen("278d", "999d", 1), &request),
            "wrong app key"
        );
    }

    #[test]
    fn verifies_presence_channel_auth() {
        let app = docs_app();
        let socket_id: SocketId = "1234.1234".parse().unwrap();
        let channel = channel("presence-foobar");
        let data = r#"{"user_id":10,"user_info":{"name":"Mr. Channels"}}"#;
        let request = ChannelAuth {
            socket_id: &socket_id,
            channel: &channel,
            channel_data: Some(data),
        };
        let auth =
            "278d425bdf160c739803:31935e7d86dba64c2a90aed31fdc61869f9b22ba9d8863bba239c03ca481bc80";

        assert!(verify_channel(&app, auth, &request), "docs signature");
    }

    type Case<'a> = (&'a str, &'a str, SystemTime, &'a [u8], Option<&'a str>);

    #[test]
    fn verifies_signed_requests() {
        let app = docs_app();
        let body = br#"{"name":"foo","channels":["project-3"],"data":"{\"some\":\"data\"}"}"#;
        let query = "auth_key=278d425bdf160c739803&auth_timestamp=1353088179&auth_version=1.0\
            &body_md5=ec365a775a4cd0599faeb73354201b6f\
            &auth_signature=da454824c97ba181a32ccc17a72625ba02771f50b50e1e7430e47a1f3f457e6c";
        let signed_at = UNIX_EPOCH + Duration::from_secs(1_353_088_179);
        let late = signed_at + Duration::from_secs(601);

        let cases: [Case<'_>; 4] = [
            ("valid", "/apps/3/events", signed_at, body, None),
            (
                "expired",
                "/apps/3/events",
                late,
                body,
                Some("auth_timestamp too old or in future"),
            ),
            (
                "tampered body",
                "/apps/3/events",
                signed_at,
                b"{}",
                Some("body_md5 mismatch"),
            ),
            (
                "wrong path",
                "/apps/4/events",
                signed_at,
                body,
                Some("invalid auth_signature"),
            ),
        ];

        for (case, path, now, body, expected) in cases {
            let request = SignedRequest {
                method: "POST",
                path,
                query,
                body,
            };
            let result = verify_request(&app, &request, now);
            assert_eq!(
                result.err().map(|e| e.to_string()).as_deref(),
                expected,
                "{case}"
            );
        }
    }
}
