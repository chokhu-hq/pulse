use std::collections::HashSet;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, Utf8Bytes, WebSocket};
use futures_util::SinkExt;
use tokio::time::{Instant, Sleep, interval_at, sleep, timeout};
use tracing::{debug, error, warn};

use crate::apps::App;
use crate::channel::ChannelName;
use crate::connection::{Batch, Close, Connection};
use crate::hub::{Hub, RegisterError};
use crate::inbound;
use crate::protocol;
use crate::rate_limit::RateLimiter;

pub(crate) const ACTIVITY_TIMEOUT: Duration = Duration::from_secs(120);
const PING_INTERVAL: Duration = Duration::from_secs(54);
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
// reuse the write buffer between flushes, but let a burst-sized one go so idle connections stay small
const MAX_SPARE_MESSAGES: usize = 64;

#[derive(Debug)]
pub(crate) struct Session {
    pub(crate) conn: Arc<Connection>,
    pub(crate) channels: HashSet<ChannelName>,
    pub(crate) limiter: RateLimiter,
}

enum Flow {
    Continue,
    Stop,
}

#[derive(Debug, thiserror::Error)]
enum WriteError {
    #[error("write timed out")]
    Timeout,
    #[error("write to websocket")]
    Socket(#[from] axum::Error),
}

pub(crate) async fn run(hub: Arc<Hub>, app: Arc<App>, ws: WebSocket) {
    let conn = match hub.register(&app) {
        Ok(conn) => conn,
        Err(err) => return reject(ws, &app, err).await,
    };

    let limiter = RateLimiter::new(app.client_event_rate, app.client_event_burst);
    let mut session = Session {
        conn,
        channels: HashSet::new(),
        limiter,
    };
    serve(&hub, &mut session, ws).await;
    hub.unregister(&session);
}

async fn reject(mut ws: WebSocket, app: &App, err: RegisterError) {
    match err {
        RegisterError::OverCapacity => {
            warn!(app = %app.key, "max connections reached");
            if let Err(err) = send(&mut ws, Message::Text(protocol::OVER_CAPACITY.into())).await {
                debug!(error = %err, "send over capacity error");
            }
        }
        RegisterError::ShuttingDown => {}
        RegisterError::SocketId(_) => error!(error = %err, "register connection"),
    }
}

async fn serve(hub: &Hub, session: &mut Session, mut ws: WebSocket) {
    let conn = Arc::clone(&session.conn);
    let mut ping = interval_at(Instant::now() + PING_INTERVAL, PING_INTERVAL);
    let mut idle = Box::pin(sleep(ACTIVITY_TIMEOUT));
    let mut last_read = Instant::now();
    let mut spare = Vec::new();

    loop {
        let flow = tokio::select! {
            frame = ws.recv() => {
                last_read = Instant::now();
                on_frame(hub, session, frame)
            }
            () = conn.woken() => flush(&mut ws, conn.take(mem::take(&mut spare)), &mut spare).await,
            _ = ping.tick() => match send(&mut ws, Message::Ping(Vec::new().into())).await {
                Ok(()) => Flow::Continue,
                Err(err) => stop(&err),
            },
            () = &mut idle => check_idle(idle.as_mut(), last_read),
            () = hub.shutdown.cancelled() => {
                conn.close(Close::Normal);
                flush(&mut ws, conn.take(mem::take(&mut spare)), &mut spare).await
            }
        };

        if matches!(flow, Flow::Stop) {
            break;
        }
    }
}

fn on_frame(hub: &Hub, session: &mut Session, frame: Option<Result<Message, axum::Error>>) -> Flow {
    let message = match frame {
        Some(Ok(message)) => message,
        Some(Err(err)) => {
            debug!(connection = %session.conn.id, error = %err, "read websocket");
            return Flow::Stop;
        }
        None => return Flow::Stop,
    };

    match message {
        Message::Text(text) => inbound::handle(hub, session, text.as_str()),
        Message::Binary(data) => inbound::handle(hub, session, &String::from_utf8_lossy(&data)),
        Message::Ping(_) | Message::Pong(_) | Message::Close(_) => {}
    }
    Flow::Continue
}

// the deadline moves lazily so reads only record a timestamp instead of resetting a timer
fn check_idle(idle: Pin<&mut Sleep>, last_read: Instant) -> Flow {
    let deadline = last_read + ACTIVITY_TIMEOUT;
    if deadline <= Instant::now() {
        return Flow::Stop;
    }

    idle.reset(deadline);
    Flow::Continue
}

async fn flush(ws: &mut WebSocket, batch: Batch, spare: &mut Vec<Utf8Bytes>) -> Flow {
    let Batch {
        mut messages,
        close,
    } = batch;
    if let Err(err) = write_batch(ws, &mut messages).await {
        return stop(&err);
    }
    if messages.capacity() <= MAX_SPARE_MESSAGES {
        *spare = messages;
    }

    let Some(close) = close else {
        return Flow::Continue;
    };
    if let Err(err) = send(ws, Message::Close(close.frame())).await {
        debug!(error = %err, "send close frame");
    }
    Flow::Stop
}

// feeds the whole batch before flushing so a burst costs a single write syscall
async fn write_batch(ws: &mut WebSocket, messages: &mut Vec<Utf8Bytes>) -> Result<(), WriteError> {
    let write = async {
        for message in messages.drain(..) {
            ws.feed(Message::Text(message)).await?;
        }
        ws.flush().await
    };

    timeout(WRITE_TIMEOUT, write)
        .await
        .map_err(|_elapsed| WriteError::Timeout)??;
    Ok(())
}

async fn send(ws: &mut WebSocket, message: Message) -> Result<(), WriteError> {
    timeout(WRITE_TIMEOUT, ws.send(message))
        .await
        .map_err(|_elapsed| WriteError::Timeout)??;
    Ok(())
}

fn stop(err: &WriteError) -> Flow {
    debug!(error = %err, "closing connection after failed write");
    Flow::Stop
}
