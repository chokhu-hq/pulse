use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context, bail};
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::client::conn::http1::{self, SendRequest};
use hyper::{Request, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;
use tokio::time::timeout;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) type Sender = SendRequest<Full<Bytes>>;

// runs `work` over one keep-alive connection, driving the connection in the same task
pub(crate) async fn with_connection<T>(
    addr: SocketAddr,
    work: impl AsyncFnOnce(&mut Sender) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    let stream = timeout(CONNECT_TIMEOUT, TcpStream::connect(addr))
        .await
        .context("connect timed out")?
        .context("connect")?;
    stream.set_nodelay(true).context("set tcp nodelay")?;
    let (mut sender, conn) = http1::handshake(TokioIo::new(stream))
        .await
        .context("http handshake")?;

    tokio::select! {
        result = conn => {
            result.context("http connection")?;
            bail!("http connection closed early")
        }
        result = work(&mut sender) => result,
    }
}

pub(crate) async fn send(
    sender: &mut Sender,
    request: Request<Full<Bytes>>,
) -> anyhow::Result<StatusCode> {
    let exchange = async {
        sender.ready().await.context("connection ready")?;
        let response = sender.send_request(request).await.context("send request")?;
        let status = response.status();
        response
            .into_body()
            .collect()
            .await
            .context("read response body")?;
        anyhow::Ok(status)
    };

    timeout(REQUEST_TIMEOUT, exchange)
        .await
        .context("request timed out")?
}

pub(crate) async fn get_ok(addr: SocketAddr, path: &str) -> anyhow::Result<()> {
    let request = Request::get(path)
        .header("host", addr.to_string())
        .body(Full::default())?;
    let status = with_connection(addr, async |sender| send(sender, request).await).await?;
    if status != StatusCode::OK {
        bail!("GET {path} returned {status}");
    }
    Ok(())
}
