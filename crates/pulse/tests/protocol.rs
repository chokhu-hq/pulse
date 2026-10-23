#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use futures_util::{SinkExt, StreamExt};
    use hmac::{Hmac, KeyInit, Mac};
    use md5::{Digest, Md5};
    use pulse::{Config, Server};
    use sha2::Sha256;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;
    use tokio::time::timeout;
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

    const STEP_TIMEOUT: Duration = Duration::from_secs(5);
    const CONFIG: &str = r#"
        [server]
        port = 0

        [[apps]]
        id = "app"
        key = "key"
        secret = "secret"
        enabled = true
    "#;

    type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

    struct Running {
        addr: SocketAddr,
        stop: oneshot::Sender<()>,
        task: JoinHandle<Result<(), pulse::ServeError>>,
    }

    fn start() -> Running {
        let server = Server::bind(Config::parse(CONFIG).unwrap()).unwrap();
        let port = server.local_addr().unwrap().port();
        let (stop, stopped) = oneshot::channel::<()>();
        let task = tokio::spawn(server.run(async {
            stopped.await.expect("stop sender kept alive by the test");
        }));

        Running {
            addr: SocketAddr::from((Ipv4Addr::LOCALHOST, port)),
            stop,
            task,
        }
    }

    async fn connect(addr: SocketAddr) -> Socket {
        let url = format!("ws://{addr}/app/key?protocol=7");
        let (socket, _) = timeout(STEP_TIMEOUT, tokio_tungstenite::connect_async(url))
            .await
            .unwrap()
            .unwrap();
        socket
    }

    async fn next_json(socket: &mut Socket) -> serde_json::Value {
        let message = timeout(STEP_TIMEOUT, socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        serde_json::from_str(message.to_text().unwrap()).unwrap()
    }

    fn hex(bytes: &[u8]) -> String {
        bytes
            .iter()
            .flat_map(|b| [b >> 4, b & 0x0f])
            .filter_map(|n| char::from_digit(u32::from(n), 16))
            .collect()
    }

    // a minimal signed http/1.1 request, returning the status code
    async fn post(addr: SocketAddr, path: &str, body: &str, secret: &str) -> u16 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let query = format!(
            "auth_key=key&auth_timestamp={now}&auth_version=1.0&body_md5={}",
            hex(&Md5::digest(body.as_bytes()))
        );
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(format!("POST\n{path}\n{query}").as_bytes());
        let signature = hex(&mac.finalize().into_bytes());

        let request = format!(
            "POST {path}?{query}&auth_signature={signature} HTTP/1.1\r\nHost: {addr}\r\n\
             Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = String::new();
        timeout(STEP_TIMEOUT, stream.read_to_string(&mut response))
            .await
            .unwrap()
            .unwrap();

        response.split(' ').nth(1).unwrap().parse().unwrap()
    }

    #[tokio::test]
    async fn delivers_signed_rest_events_to_subscribers() {
        let server = start();
        let mut socket = connect(server.addr).await;

        let established = next_json(&mut socket).await;
        assert_eq!(
            established["event"], "pusher:connection_established",
            "handshake event"
        );
        let data: serde_json::Value =
            serde_json::from_str(established["data"].as_str().unwrap()).unwrap();
        let socket_id = data["socket_id"].as_str().unwrap();
        assert!(
            socket_id.split_once('.').is_some(),
            "socket id {socket_id} has two parts"
        );

        let subscribe = r#"{"event":"pusher:subscribe","data":{"channel":"news"}}"#;
        socket.send(Message::text(subscribe)).await.unwrap();
        let succeeded = next_json(&mut socket).await;
        assert_eq!(
            succeeded["event"], "pusher_internal:subscription_succeeded",
            "subscribed"
        );

        let body = r#"{"name":"update","channel":"news","data":"{\"n\":1}"}"#;
        assert_eq!(
            post(server.addr, "/apps/app/events", body, "secret").await,
            200,
            "trigger accepted"
        );

        let event = next_json(&mut socket).await;
        assert_eq!(event["event"], "update", "event name");
        assert_eq!(event["channel"], "news", "event channel");
        assert_eq!(
            event["data"], r#"{"n":1}"#,
            "data is passed through as a string"
        );

        server.stop.send(()).unwrap();
        server.task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn rejects_requests_signed_with_the_wrong_secret() {
        let server = start();
        let body = r#"{"name":"update","channel":"news","data":"{}"}"#;

        assert_eq!(
            post(server.addr, "/apps/app/events", body, "wrong").await,
            401,
            "bad signature"
        );

        server.stop.send(()).unwrap();
        server.task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn closes_websockets_on_shutdown() {
        let server = start();
        let mut socket = connect(server.addr).await;
        next_json(&mut socket).await;

        server.stop.send(()).unwrap();
        let closed = timeout(STEP_TIMEOUT, socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert!(matches!(closed, Message::Close(_)), "got {closed:?}");
        timeout(STEP_TIMEOUT, server.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
