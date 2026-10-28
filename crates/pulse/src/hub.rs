use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::apps::{App, Apps};
use crate::channel::ChannelKey;
use crate::channels::Channels;
use crate::config::Config;
use crate::connection::{Close, Connection};
use crate::presence::Presence;
use crate::protocol::{self, ErrorCode};
use crate::session::{ACTIVITY_TIMEOUT, Session};
use crate::socket_id::SocketId;

#[derive(Debug, thiserror::Error)]
pub(crate) enum RegisterError {
    #[error("connection limit reached")]
    OverCapacity,
    #[error("server is shutting down")]
    ShuttingDown,
    #[error("generate socket id")]
    SocketId(#[from] getrandom::Error),
}

#[derive(Debug, Default)]
struct Registry {
    connections: HashMap<SocketId, Arc<Connection>>,
    per_app: HashMap<String, usize>,
}

const PRESENCE_GATES: usize = 64;

// sharded by channel so a membership change and its broadcast cannot interleave with another on the same channel
#[derive(Debug)]
struct PresenceGates {
    hasher: RandomState,
    gates: [Mutex<()>; PRESENCE_GATES],
}

impl PresenceGates {
    fn new() -> Self {
        Self {
            hasher: RandomState::new(),
            gates: std::array::from_fn(|_| Mutex::new(())),
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "the shard is reduced modulo the array length"
    )]
    fn lock(&self, key: &ChannelKey) -> MutexGuard<'_, ()> {
        let shard = usize::try_from(self.hasher.hash_one(key)).unwrap_or_default() % PRESENCE_GATES;
        self.gates[shard]
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

#[derive(Debug)]
pub(crate) struct Hub {
    pub(crate) apps: Apps,
    pub(crate) max_connections: NonZeroUsize,
    pub(crate) channels: Channels,
    pub(crate) presence: Presence,
    pub(crate) shutdown: CancellationToken,
    registry: Mutex<Registry>,
    presence_gates: PresenceGates,
}

impl Hub {
    pub(crate) fn new(config: Config, shutdown: CancellationToken) -> Self {
        Self {
            apps: config.apps,
            max_connections: config.max_connections,
            channels: Channels::default(),
            presence: Presence::default(),
            shutdown,
            registry: Mutex::default(),
            presence_gates: PresenceGates::new(),
        }
    }

    pub(crate) fn register(&self, app: &Arc<App>) -> Result<Arc<Connection>, RegisterError> {
        if self.shutdown.is_cancelled() {
            return Err(RegisterError::ShuttingDown);
        }

        let conn = Arc::new(Connection::new(SocketId::generate()?, Arc::clone(app)));
        let active = {
            let mut registry = self.registry.lock().unwrap_or_else(PoisonError::into_inner);
            let app_count = registry.per_app.get(&app.key).copied().unwrap_or(0);
            let app_full = app
                .max_connections
                .is_some_and(|max| app_count >= max.get());
            if registry.connections.len() >= self.max_connections.get() || app_full {
                return Err(RegisterError::OverCapacity);
            }

            registry
                .connections
                .insert(conn.id.clone(), Arc::clone(&conn));
            *registry.per_app.entry(app.key.clone()).or_default() += 1;
            registry.connections.len()
        };
        debug!(id = %conn.id, app = %app.key, active_connections = active, "connection registered");

        conn.send_text(protocol::connection_established(
            &conn.id,
            ACTIVITY_TIMEOUT.as_secs(),
        ));
        Ok(conn)
    }

    pub(crate) fn unregister(&self, session: &Session) {
        let conn = &session.conn;
        let active = {
            let mut registry = self.registry.lock().unwrap_or_else(PoisonError::into_inner);
            if registry.connections.remove(&conn.id).is_none() {
                return;
            }
            if let Some(count) = registry.per_app.get_mut(&conn.app.key) {
                *count = count.saturating_sub(1);
            }
            registry.per_app.retain(|_, count| *count > 0);
            registry.connections.len()
        };
        debug!(id = %conn.id, active_connections = active, "connection unregistered");

        for channel in &session.channels {
            self.leave_channel(conn, channel);
        }
    }

    pub(crate) fn terminate_user_connections(&self, app_key: &str, user_id: &str) {
        let targets: Vec<_> = {
            let registry = self.registry.lock().unwrap_or_else(PoisonError::into_inner);
            registry
                .connections
                .values()
                .filter(|conn| conn.app.key == app_key && conn.user_id() == Some(user_id))
                .map(Arc::clone)
                .collect()
        };

        for conn in targets {
            conn.send_error(
                "Connection terminated",
                Some(ErrorCode::ConnectionUnauthorized),
            );
            conn.close(Close::Normal);
        }
    }

    pub(crate) fn lock_presence(&self, key: &ChannelKey) -> MutexGuard<'_, ()> {
        self.presence_gates.lock(key)
    }

    pub(crate) fn connection_count(&self) -> usize {
        self.registry
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .connections
            .len()
    }

    pub(crate) fn app_connection_counts(&self) -> Vec<(String, usize)> {
        let registry = self.registry.lock().unwrap_or_else(PoisonError::into_inner);
        registry
            .per_app
            .iter()
            .map(|(key, count)| (key.clone(), *count))
            .collect()
    }
}
