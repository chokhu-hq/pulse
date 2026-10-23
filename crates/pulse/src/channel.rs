use std::fmt;

// pusher's documented limit for channel names
const MAX_NAME_LEN: usize = 164;
const SERVER_TO_USER_PREFIX: &str = "#server-to-user-";

#[derive(Debug, Clone, Copy)]
pub(crate) enum ChannelKind {
    Public,
    Private,
    PrivateEncrypted,
    Presence,
    ServerToUser,
}

impl ChannelKind {
    pub(crate) const fn requires_auth(self) -> bool {
        match self {
            Self::Private | Self::PrivateEncrypted | Self::Presence => true,
            Self::Public | Self::ServerToUser => false,
        }
    }

    // encrypted channels are excluded because the server cannot read their payloads
    pub(crate) const fn allows_client_events(self) -> bool {
        match self {
            Self::Private | Self::Presence => true,
            Self::Public | Self::PrivateEncrypted | Self::ServerToUser => false,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct ChannelName(String);

#[derive(Debug, thiserror::Error)]
#[error("invalid channel name")]
pub(crate) struct InvalidChannelName;

impl ChannelName {
    pub(crate) fn kind(&self) -> ChannelKind {
        let name = self.0.as_str();
        if name.starts_with('#') {
            ChannelKind::ServerToUser
        } else if name.starts_with("private-encrypted-") {
            ChannelKind::PrivateEncrypted
        } else if name.starts_with("private-") {
            ChannelKind::Private
        } else if name.starts_with("presence-") {
            ChannelKind::Presence
        } else {
            ChannelKind::Public
        }
    }

    pub(crate) fn is_user_channel_of(&self, user_id: &str) -> bool {
        self.0.strip_prefix(SERVER_TO_USER_PREFIX) == Some(user_id)
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ChannelName {
    type Error = InvalidChannelName;

    fn try_from(name: String) -> Result<Self, Self::Error> {
        let body = name.strip_prefix('#').unwrap_or(&name);
        let is_valid_char = |c: char| c.is_ascii_alphanumeric() || "_-=@,.;".contains(c);
        if body.is_empty() || name.len() > MAX_NAME_LEN || !body.chars().all(is_valid_char) {
            return Err(InvalidChannelName);
        }

        Ok(Self(name))
    }
}

impl fmt::Display for ChannelName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

pub(crate) fn is_presence_prefix(prefix: &str) -> bool {
    prefix.starts_with("presence-")
}

// namespaced per app so tenants sharing a channel name stay isolated
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ChannelKey(String);

impl ChannelKey {
    pub(crate) fn new(app_key: &str, channel: &ChannelName) -> Self {
        Self(format!("{app_key}\0{channel}"))
    }

    // keys are only built from validated names, so the suffix needs no revalidation
    pub(crate) fn channel_of(&self, app_key: &str) -> Option<ChannelName> {
        let name = self.0.strip_prefix(app_key)?.strip_prefix('\0')?;
        Some(ChannelName(name.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(s: &str) -> ChannelName {
        ChannelName::try_from(s.to_owned()).expect("valid channel name")
    }

    #[test]
    fn kinds() {
        let cases = [
            ("chat", "Public"),
            ("private-chat", "Private"),
            ("private-encrypted-chat", "PrivateEncrypted"),
            ("presence-lobby", "Presence"),
            ("#server-to-user-1", "ServerToUser"),
        ];

        for (input, expected) in cases {
            assert_eq!(
                format!("{:?}", name(input).kind()),
                expected,
                "kind of {input}"
            );
        }
    }

    #[test]
    fn validation() {
        let long = "a".repeat(MAX_NAME_LEN + 1);
        let cases = [
            ("chat-room_1=@,.;", true),
            ("#server-to-user-42", true),
            ("", false),
            ("#", false),
            ("a b", false),
            ("a#b", false),
            ("caf\u{e9}", false),
            (long.as_str(), false),
        ];

        for (input, valid) in cases {
            assert_eq!(
                ChannelName::try_from(input.to_owned()).is_ok(),
                valid,
                "channel name {input:?}"
            );
        }
    }

    #[test]
    fn keys_are_namespaced_per_app() {
        let key = ChannelKey::new("app", &name("chat"));

        assert_eq!(key.channel_of("app"), Some(name("chat")), "own app");
        assert_eq!(key.channel_of("ap"), None, "app key prefix of another app");
    }

    #[test]
    fn user_channels() {
        assert!(
            name("#server-to-user-7").is_user_channel_of("7"),
            "own channel"
        );
        assert!(
            !name("#server-to-user-7").is_user_channel_of("8"),
            "other user"
        );
    }
}
