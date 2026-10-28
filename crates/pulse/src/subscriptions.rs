use std::sync::Arc;

use tracing::debug;

use crate::auth::{self, ChannelAuth};
use crate::channel::{ChannelKey, ChannelKind, ChannelName};
use crate::connection::Connection;
use crate::hub::Hub;
use crate::presence::{Departure, Member};
use crate::protocol::{self, ErrorCode, Incoming, Outgoing};
use crate::session::Session;

#[derive(Debug)]
pub(crate) struct Subscription {
    pub(crate) channel: ChannelName,
    pub(crate) auth: Option<String>,
    pub(crate) channel_data: Option<String>,
}

#[derive(Debug, Clone, Copy)]
enum Rejection {
    NotAllowed,
    InvalidChannelData,
    AuthRequired,
    InvalidSignature,
    ChannelLimit,
}

impl Rejection {
    const fn message(self) -> &'static str {
        match self {
            Self::NotAllowed => "Not allowed to subscribe to this channel",
            Self::InvalidChannelData => "Invalid channel_data for presence channel",
            Self::AuthRequired => "Authentication required for private/presence channels",
            Self::InvalidSignature => "Invalid authentication signature",
            Self::ChannelLimit => "Connection channel limit exceeded",
        }
    }

    const fn code(self) -> ErrorCode {
        match self {
            Self::InvalidSignature => ErrorCode::InvalidSignature,
            Self::NotAllowed
            | Self::InvalidChannelData
            | Self::AuthRequired
            | Self::ChannelLimit => ErrorCode::ConnectionUnauthorized,
        }
    }
}

impl Hub {
    pub(crate) fn subscribe(&self, session: &mut Session, subscription: Subscription) {
        let conn = Arc::clone(&session.conn);
        let member = match check(session, &subscription) {
            Ok(member) => member,
            Err(rejection) => return conn.send_error(rejection.message(), Some(rejection.code())),
        };

        let channel = subscription.channel;
        let key = ChannelKey::new(&conn.app.key, &channel);
        self.channels.subscribe(&key, &conn);
        debug!(connection = %conn.id, channel = %channel, "subscribed to channel");

        match member {
            Some(member) => self.join_presence(&conn, &channel, member),
            None => conn.send_text(protocol::subscription_succeeded(&channel)),
        }
        session.channels.insert(channel);
    }

    fn join_presence(&self, conn: &Connection, channel: &ChannelName, member: Member) {
        let key = ChannelKey::new(&conn.app.key, channel);
        let _gate = self.lock_presence(&key);
        match self.presence.user_id(&key, &conn.id) {
            Some(previous) if previous == member.user_id => {
                let data = self.presence.data(&key);
                return conn.send_text(protocol::presence_subscription_succeeded(channel, &data));
            }
            Some(_) => self.leave_presence(&key, conn, channel),
            None => {}
        }

        let added = protocol::member_added(channel, &member);
        if self.presence.join(&key, &conn.id, member) {
            self.channels.broadcast(&key, Some(&conn.id), || added);
        }

        let data = self.presence.data(&key);
        conn.send_text(protocol::presence_subscription_succeeded(channel, &data));
    }

    pub(crate) fn unsubscribe(&self, session: &mut Session, channel: &ChannelName) {
        self.leave_channel(&session.conn, channel);
        session.channels.remove(channel);
        debug!(connection = %session.conn.id, channel = %channel, "unsubscribed from channel");
    }

    pub(crate) fn leave_channel(&self, conn: &Connection, channel: &ChannelName) {
        let key = ChannelKey::new(&conn.app.key, channel);
        {
            let _gate = self.lock_presence(&key);
            self.leave_presence(&key, conn, channel);
        }
        self.channels.unsubscribe(&key, &conn.id);
    }

    fn leave_presence(&self, key: &ChannelKey, conn: &Connection, channel: &ChannelName) {
        if let Some(Departure::LastConnection(user_id)) = self.presence.leave(key, &conn.id) {
            self.channels.broadcast(key, Some(&conn.id), || {
                protocol::member_removed(channel, &user_id)
            });
        }
    }

    pub(crate) fn forward_client_event(
        &self,
        conn: &Connection,
        channel: &ChannelName,
        msg: &Incoming,
    ) {
        let key = ChannelKey::new(&conn.app.key, channel);
        let user_id = match channel.kind() {
            ChannelKind::Presence => self.presence.user_id(&key, &conn.id),
            ChannelKind::Public
            | ChannelKind::Private
            | ChannelKind::PrivateEncrypted
            | ChannelKind::ServerToUser => None,
        };
        let data = msg.data();

        self.channels.broadcast(&key, Some(&conn.id), || {
            Outgoing {
                event: &msg.event,
                channel: Some(channel.as_str()),
                data: &data,
                user_id: user_id.as_deref(),
            }
            .encode()
        });
    }
}

fn check(session: &Session, subscription: &Subscription) -> Result<Option<Member>, Rejection> {
    let conn = &session.conn;
    let channel = &subscription.channel;
    let kind = channel.kind();

    if matches!(kind, ChannelKind::ServerToUser)
        && !conn
            .user_id()
            .is_some_and(|id| channel.is_user_channel_of(id))
    {
        return Err(Rejection::NotAllowed);
    }

    let member = match kind {
        ChannelKind::Presence => Some(presence_member(conn, subscription.channel_data.as_deref())?),
        ChannelKind::Public
        | ChannelKind::Private
        | ChannelKind::PrivateEncrypted
        | ChannelKind::ServerToUser => None,
    };

    if kind.requires_auth() {
        let auth = subscription
            .auth
            .as_deref()
            .ok_or(Rejection::AuthRequired)?;
        let request = ChannelAuth {
            socket_id: &conn.id,
            channel,
            channel_data: subscription.channel_data.as_deref(),
        };
        if !auth::verify_channel(&conn.app, auth, &request) {
            return Err(Rejection::InvalidSignature);
        }
    }

    let at_limit = conn
        .app
        .max_channels_per_connection
        .is_some_and(|max| session.channels.len() >= max.get());
    if at_limit && !session.channels.contains(channel) {
        return Err(Rejection::ChannelLimit);
    }
    Ok(member)
}

fn presence_member(conn: &Connection, channel_data: Option<&str>) -> Result<Member, Rejection> {
    match channel_data.filter(|data| !data.is_empty()) {
        Some(data) => serde_json::from_str(data).map_err(|err| {
            debug!(connection = %conn.id, error = %err, "invalid presence channel_data");
            Rejection::InvalidChannelData
        }),
        None => Ok(Member {
            user_id: conn
                .user_id()
                .map_or_else(|| conn.id.to_string(), str::to_owned),
            user_info: None,
        }),
    }
}
