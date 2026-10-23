use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};

use axum::extract::ws::Utf8Bytes;

use crate::channel::{ChannelKey, ChannelName};
use crate::connection::Connection;
use crate::socket_id::SocketId;

type Subscribers = RwLock<HashMap<SocketId, Arc<Connection>>>;

// two lock levels so a large fan-out only blocks its own channel, never subscribes elsewhere
#[derive(Debug, Default)]
pub(crate) struct Channels {
    by_key: RwLock<HashMap<ChannelKey, Arc<Subscribers>>>,
}

impl Channels {
    pub(crate) fn subscribe(&self, key: &ChannelKey, conn: &Arc<Connection>) {
        let mut channels = self.by_key.write().unwrap_or_else(PoisonError::into_inner);
        let subscribers = channels.entry(key.clone()).or_default();
        subscribers
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(conn.id.clone(), Arc::clone(conn));
    }

    pub(crate) fn unsubscribe(&self, key: &ChannelKey, conn_id: &SocketId) {
        let Some(subscribers) = self.subscribers(key) else {
            return;
        };
        let is_empty = {
            let mut subscribers = subscribers.write().unwrap_or_else(PoisonError::into_inner);
            subscribers.remove(conn_id);
            subscribers.is_empty()
        };
        if !is_empty {
            return;
        }

        // recheck under the outer write lock since a subscriber may have joined in between
        let mut channels = self.by_key.write().unwrap_or_else(PoisonError::into_inner);
        let still_empty = channels
            .get(key)
            .is_some_and(|s| s.read().unwrap_or_else(PoisonError::into_inner).is_empty());
        if still_empty {
            channels.remove(key);
        }
    }

    // encodes once and shares the payload across subscribers; skipped entirely for empty channels
    pub(crate) fn broadcast(
        &self,
        key: &ChannelKey,
        exclude: Option<&SocketId>,
        encode: impl FnOnce() -> String,
    ) {
        let Some(subscribers) = self.subscribers(key) else {
            return;
        };
        let subscribers = subscribers.read().unwrap_or_else(PoisonError::into_inner);
        if subscribers.is_empty() {
            return;
        }

        let message = Utf8Bytes::from(encode());
        for (id, conn) in subscribers.iter() {
            if exclude != Some(id) {
                conn.send(message.clone());
            }
        }
    }

    pub(crate) fn subscriber_count(&self, key: &ChannelKey) -> usize {
        self.subscribers(key).map_or(0, |s| {
            s.read().unwrap_or_else(PoisonError::into_inner).len()
        })
    }

    pub(crate) fn names(&self, app_key: &str, prefix: &str) -> Vec<ChannelName> {
        let channels = self.by_key.read().unwrap_or_else(PoisonError::into_inner);
        channels
            .keys()
            .filter_map(|key| key.channel_of(app_key))
            .filter(|name| name.as_str().starts_with(prefix))
            .collect()
    }

    pub(crate) fn len(&self) -> usize {
        self.by_key
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    fn subscribers(&self, key: &ChannelKey) -> Option<Arc<Subscribers>> {
        self.by_key
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(key)
            .cloned()
    }
}
