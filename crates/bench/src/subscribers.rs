use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, bail};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::server::APP_KEY;

pub(crate) const CHANNEL: &str = "bench";
pub(crate) const EVENT_MARKER: &str = r#""event":"ev""#;

const MAX_CONCURRENT_HANDSHAKES: usize = 200;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

// readers are aborted when the returned set is dropped, closing every connection
pub(crate) async fn subscribe_all(
    addr: SocketAddr,
    count: usize,
    received: &Arc<AtomicU64>,
) -> anyhow::Result<JoinSet<()>> {
    let permits = Arc::new(Semaphore::new(MAX_CONCURRENT_HANDSHAKES));
    let mut handshakes = JoinSet::new();
    for _ in 0..count {
        let permit = Arc::clone(&permits)
            .acquire_owned()
            .await
            .context("handshake permit")?;
        handshakes.spawn(async move {
            let socket = timeout(HANDSHAKE_TIMEOUT, subscribe(addr))
                .await
                .context("subscribe timed out")?;
            drop(permit);
            socket
        });
    }

    let mut readers = JoinSet::new();
    while let Some(joined) = handshakes.join_next().await {
        let socket = joined.context("subscribe task")??;
        readers.spawn(count_events(socket, Arc::clone(received)));
    }
    Ok(readers)
}

async fn subscribe(addr: SocketAddr) -> anyhow::Result<Socket> {
    let url = format!("ws://{addr}/app/{APP_KEY}?protocol=7");
    let (mut socket, _) = tokio_tungstenite::connect_async(url)
        .await
        .context("connect websocket")?;
    expect_message(&mut socket, "pusher:connection_established").await?;

    let subscribe = format!(r#"{{"event":"pusher:subscribe","data":{{"channel":"{CHANNEL}"}}}}"#);
    socket
        .send(Message::text(subscribe))
        .await
        .context("send subscribe")?;
    expect_message(&mut socket, "pusher_internal:subscription_succeeded").await?;
    Ok(socket)
}

async fn expect_message(socket: &mut Socket, event: &str) -> anyhow::Result<()> {
    let message = socket
        .next()
        .await
        .context("connection closed")?
        .context("read message")?;
    let text = message.to_text().context("non text message")?;
    if !text.contains(event) {
        bail!("expected {event}, got {text}");
    }
    Ok(())
}

async fn count_events(mut socket: Socket, received: Arc<AtomicU64>) {
    while let Some(Ok(message)) = socket.next().await {
        if let Message::Text(text) = message
            && text.as_str().contains(EVENT_MARKER)
        {
            received.fetch_add(1, Ordering::Relaxed);
        }
    }
}
