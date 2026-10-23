use std::mem;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use axum::extract::ws::{CloseFrame, Utf8Bytes};
use tokio::sync::Notify;
use tracing::warn;

use crate::apps::App;
use crate::protocol::{self, ErrorCode};
use crate::socket_id::SocketId;

// broadcast payloads are shared across subscribers, so this mostly bounds how far one client may lag
const MAX_QUEUED_BYTES: usize = 1 << 20;

#[derive(Debug, Clone, Copy)]
pub(crate) enum Close {
    Normal,
    SlowConsumer,
}

impl Close {
    pub(crate) fn frame(self) -> Option<CloseFrame> {
        match self {
            Self::Normal => None,
            Self::SlowConsumer => Some(CloseFrame {
                code: ErrorCode::OverCapacity.into(),
                reason: "Client too slow to receive messages".into(),
            }),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Batch {
    pub(crate) messages: Vec<Utf8Bytes>,
    pub(crate) close: Option<Close>,
}

#[derive(Debug, Default)]
struct Outbox {
    queue: Vec<Utf8Bytes>,
    queued_bytes: usize,
    close: Option<Close>,
}

// shared between the connection's own task, which writes, and everything that sends to it
#[derive(Debug)]
pub(crate) struct Connection {
    pub(crate) id: SocketId,
    pub(crate) app: Arc<App>,
    user_id: OnceLock<String>,
    outbox: Mutex<Outbox>,
    wake: Notify,
}

impl Connection {
    pub(crate) fn new(id: SocketId, app: Arc<App>) -> Self {
        Self {
            id,
            app,
            user_id: OnceLock::new(),
            outbox: Mutex::default(),
            wake: Notify::new(),
        }
    }

    pub(crate) fn user_id(&self) -> Option<&str> {
        self.user_id.get().map(String::as_str)
    }

    pub(crate) fn sign_in(&self, user_id: String) -> bool {
        self.user_id.set(user_id).is_ok()
    }

    pub(crate) fn send(&self, message: Utf8Bytes) {
        let mut outbox = self.outbox.lock().unwrap_or_else(PoisonError::into_inner);
        if matches!(outbox.close, Some(Close::SlowConsumer)) {
            return;
        }

        if outbox.queued_bytes + message.len() > MAX_QUEUED_BYTES {
            let queued_bytes = outbox.queued_bytes;
            outbox.close = Some(Close::SlowConsumer);
            drop(outbox);
            warn!(connection = %self.id, app = %self.app.key, queued_bytes, "disconnecting slow client");
            self.wake.notify_one();
            return;
        }

        outbox.queued_bytes += message.len();
        outbox.queue.push(message);
        drop(outbox);
        self.wake.notify_one();
    }

    pub(crate) fn send_text(&self, message: String) {
        self.send(Utf8Bytes::from(message));
    }

    pub(crate) fn send_error(&self, message: &str, code: Option<ErrorCode>) {
        self.send_text(protocol::error(message, code));
    }

    // already queued messages are still delivered before the close frame
    pub(crate) fn close(&self, close: Close) {
        let mut outbox = self.outbox.lock().unwrap_or_else(PoisonError::into_inner);
        if outbox.close.is_some() {
            return;
        }

        outbox.close = Some(close);
        drop(outbox);
        self.wake.notify_one();
    }

    pub(crate) async fn woken(&self) {
        self.wake.notified().await;
    }

    pub(crate) fn take(&self, spare: Vec<Utf8Bytes>) -> Batch {
        let mut outbox = self.outbox.lock().unwrap_or_else(PoisonError::into_inner);
        outbox.queued_bytes = 0;
        Batch {
            messages: mem::replace(&mut outbox.queue, spare),
            close: outbox.close,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apps::AppFile;

    fn connection() -> Connection {
        let app: AppFile =
            serde_json::from_str(r#"{"id":"a","key":"k","secret":"s","enabled":true}"#).unwrap();
        Connection::new(
            "1.1".parse().unwrap(),
            Arc::new(App::try_from(app).unwrap()),
        )
    }

    #[test]
    fn closes_slow_consumers() {
        let conn = connection();
        let big = Utf8Bytes::from("x".repeat(MAX_QUEUED_BYTES / 2));

        conn.send(big.clone());
        conn.send(big.clone());
        conn.send(big);

        let batch = conn.take(Vec::new());
        assert_eq!(
            batch.messages.len(),
            2,
            "messages past the limit are dropped"
        );
        assert!(
            matches!(batch.close, Some(Close::SlowConsumer)),
            "connection is closed as slow"
        );
    }

    #[test]
    fn signs_in_once() {
        let conn = connection();

        assert!(conn.sign_in("a".to_owned()), "first sign in");
        assert!(!conn.sign_in("b".to_owned()), "second sign in is rejected");
        assert_eq!(conn.user_id(), Some("a"), "first user is kept");
    }
}
