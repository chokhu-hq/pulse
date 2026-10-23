use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail};
use hmac::{Hmac, KeyInit, Mac};
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Request, StatusCode};
use md5::{Digest, Md5};
use serde_json::json;
use sha2::Sha256;
use tokio::task::JoinSet;

use crate::http_client::{self, Sender};
use crate::server::{APP_ID, APP_KEY, APP_SECRET};
use crate::subscribers::CHANNEL;

#[derive(Debug)]
struct Publisher {
    addr: SocketAddr,
    path: String,
    body: Bytes,
    body_md5: String,
    next: Arc<AtomicUsize>,
    events: usize,
}

pub(crate) fn event_body(payload_bytes: usize) -> anyhow::Result<Bytes> {
    let data = serde_json::to_string(&"x".repeat(payload_bytes))?;
    Ok(Bytes::from(
        json!({ "name": "ev", "channel": CHANNEL, "data": data }).to_string(),
    ))
}

// publishers share one counter so exactly `events` requests are sent across all of them
pub(crate) async fn publish_all(
    addr: SocketAddr,
    body: Bytes,
    events: usize,
    publishers: usize,
) -> anyhow::Result<()> {
    let next = Arc::new(AtomicUsize::new(0));
    let body_md5 = to_hex(&Md5::digest(&body));
    let mut tasks = JoinSet::new();
    for _ in 0..publishers {
        let publisher = Publisher {
            addr,
            path: format!("/apps/{APP_ID}/events"),
            body: body.clone(),
            body_md5: body_md5.clone(),
            next: Arc::clone(&next),
            events,
        };
        tasks.spawn(publisher.run());
    }

    while let Some(joined) = tasks.join_next().await {
        joined.context("publisher task")??;
    }
    Ok(())
}

impl Publisher {
    async fn run(self) -> anyhow::Result<()> {
        http_client::with_connection(self.addr, async |sender| {
            while self.next.fetch_add(1, Ordering::Relaxed) < self.events {
                self.publish(sender).await?;
            }
            Ok(())
        })
        .await
    }

    async fn publish(&self, sender: &mut Sender) -> anyhow::Result<()> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("clock before epoch")?
            .as_secs();
        let query = format!(
            "auth_key={APP_KEY}&auth_timestamp={timestamp}&auth_version=1.0&body_md5={}",
            self.body_md5
        );
        let signature = sign(&format!("POST\n{}\n{query}", self.path))?;
        let request = Request::post(format!("{}?{query}&auth_signature={signature}", self.path))
            .header("host", self.addr.to_string())
            .header("content-type", "application/json")
            .body(Full::new(self.body.clone()))?;

        let status = http_client::send(sender, request).await?;
        if status != StatusCode::OK {
            bail!("publish returned {status}");
        }
        Ok(())
    }
}

fn sign(message: &str) -> anyhow::Result<String> {
    let mut mac = Hmac::<Sha256>::new_from_slice(APP_SECRET.as_bytes()).context("hmac key")?;
    mac.update(message.as_bytes());
    Ok(to_hex(&mac.finalize().into_bytes()))
}

fn to_hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .flat_map(|b| [b >> 4, b & 0x0f])
        .filter_map(|n| char::from_digit(u32::from(n), 16))
        .collect()
}
