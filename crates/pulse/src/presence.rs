use std::collections::HashMap;
use std::sync::{PoisonError, RwLock};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::channel::ChannelKey;
use crate::socket_id::SocketId;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct Member {
    pub(crate) user_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) user_info: Option<Map<String, Value>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct PresenceData {
    pub(crate) ids: Vec<String>,
    pub(crate) hash: HashMap<String, Map<String, Value>>,
    pub(crate) count: usize,
}

#[derive(Debug)]
pub(crate) enum Departure {
    LastConnection(String),
    OtherConnectionsRemain,
}

#[derive(Debug, Default)]
struct Members {
    by_connection: HashMap<SocketId, Member>,
    connections_per_user: HashMap<String, usize>,
}

impl Members {
    // returns whether this was the user's last connection in the channel
    fn release(&mut self, user_id: &str) -> bool {
        let Some(count) = self.connections_per_user.get_mut(user_id) else {
            return true;
        };
        if *count > 1 {
            *count -= 1;
            return false;
        }

        self.connections_per_user.remove(user_id);
        true
    }
}

#[derive(Debug, Default)]
pub(crate) struct Presence {
    channels: RwLock<HashMap<ChannelKey, Members>>,
}

impl Presence {
    // returns whether this is the user's first connection in the channel
    pub(crate) fn join(&self, channel: &ChannelKey, conn_id: &SocketId, member: Member) -> bool {
        let mut channels = self
            .channels
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        let members = channels.entry(channel.clone()).or_default();
        if let Some(previous) = members.by_connection.remove(conn_id) {
            members.release(&previous.user_id);
        }

        let count = members
            .connections_per_user
            .entry(member.user_id.clone())
            .or_default();
        *count += 1;
        let is_first = *count == 1;
        members.by_connection.insert(conn_id.clone(), member);
        is_first
    }

    pub(crate) fn leave(&self, channel: &ChannelKey, conn_id: &SocketId) -> Option<Departure> {
        let mut channels = self
            .channels
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        let members = channels.get_mut(channel)?;
        let member = members.by_connection.remove(conn_id)?;
        let departure = if members.release(&member.user_id) {
            Departure::LastConnection(member.user_id)
        } else {
            Departure::OtherConnectionsRemain
        };

        if members.by_connection.is_empty() {
            channels.remove(channel);
        }
        Some(departure)
    }

    pub(crate) fn user_id(&self, channel: &ChannelKey, conn_id: &SocketId) -> Option<String> {
        let channels = self.channels.read().unwrap_or_else(PoisonError::into_inner);
        channels
            .get(channel)?
            .by_connection
            .get(conn_id)
            .map(|member| member.user_id.clone())
    }

    pub(crate) fn user_count(&self, channel: &ChannelKey) -> usize {
        let channels = self.channels.read().unwrap_or_else(PoisonError::into_inner);
        channels
            .get(channel)
            .map_or(0, |members| members.connections_per_user.len())
    }

    pub(crate) fn data(&self, channel: &ChannelKey) -> PresenceData {
        let channels = self.channels.read().unwrap_or_else(PoisonError::into_inner);
        let Some(members) = channels.get(channel) else {
            return PresenceData {
                ids: Vec::new(),
                hash: HashMap::new(),
                count: 0,
            };
        };

        let ids: Vec<String> = members.connections_per_user.keys().cloned().collect();
        let hash = members
            .by_connection
            .values()
            .map(|member| {
                (
                    member.user_id.clone(),
                    member.user_info.clone().unwrap_or_default(),
                )
            })
            .collect();
        PresenceData {
            count: ids.len(),
            ids,
            hash,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::ChannelName;

    fn key() -> ChannelKey {
        ChannelKey::new(
            "app",
            &ChannelName::try_from("presence-room".to_owned()).unwrap(),
        )
    }

    fn member(user_id: &str) -> Member {
        Member {
            user_id: user_id.to_owned(),
            user_info: None,
        }
    }

    #[test]
    fn counts_users_across_connections() {
        let presence = Presence::default();
        let (a, b): (SocketId, SocketId) = ("1.1".parse().unwrap(), "2.2".parse().unwrap());

        assert!(
            presence.join(&key(), &a, member("alice")),
            "first connection joins"
        );
        assert!(
            !presence.join(&key(), &b, member("alice")),
            "second connection is not a new member"
        );
        assert_eq!(
            presence.user_count(&key()),
            1,
            "one user with two connections"
        );

        assert!(
            matches!(
                presence.leave(&key(), &a),
                Some(Departure::OtherConnectionsRemain)
            ),
            "one left"
        );
        let last = presence.leave(&key(), &b);
        assert!(
            matches!(last, Some(Departure::LastConnection(ref id)) if id == "alice"),
            "got {last:?}"
        );
        assert_eq!(presence.user_count(&key()), 0, "channel is empty");
    }

    #[test]
    fn rejoining_replaces_previous_member() {
        let presence = Presence::default();
        let conn: SocketId = "1.1".parse().unwrap();

        presence.join(&key(), &conn, member("alice"));
        assert!(
            presence.join(&key(), &conn, member("bob")),
            "new user on the same connection"
        );

        assert_eq!(
            presence.data(&key()).ids,
            vec!["bob".to_owned()],
            "alice was replaced"
        );
    }

    #[test]
    fn requires_user_id() {
        assert!(
            serde_json::from_str::<Member>(r#"{"user_info":{}}"#).is_err(),
            "user_id is required"
        );
    }
}
