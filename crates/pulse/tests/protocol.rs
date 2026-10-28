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
        start_config(Config::parse(CONFIG).unwrap())
    }

    fn start_config(config: Config) -> Running {
        let server = Server::bind(config).unwrap();
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

    async fn stop(server: Running) {
        let _ = server.stop.send(());
        timeout(STEP_TIMEOUT, server.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn rejects_event_data_above_configured_limit() {
        let server =
            start_config(Config::parse(&format!("{CONFIG}\nmax_message_size = 1024")).unwrap());
        let body =
            serde_json::json!({ "name": "update", "channel": "news", "data": "x".repeat(1025) })
                .to_string();
        let status = post(server.addr, "/apps/app/events", &body, "secret").await;
        stop(server).await;
        assert_eq!(
            status, 413,
            "individual event data must respect max_message_size"
        );
    }

    #[tokio::test]
    async fn rejects_oversized_individual_batch_event() {
        let server =
            start_config(Config::parse(&format!("{CONFIG}\nmax_message_size = 1024")).unwrap());
        let body = serde_json::json!({ "batch": [{ "name": "update", "channel": "news", "data": "x".repeat(1025) }] }).to_string();
        let status = post(server.addr, "/apps/app/batch_events", &body, "secret").await;
        stop(server).await;
        assert_eq!(
            status, 413,
            "batch events must each respect max_message_size"
        );
    }

    #[tokio::test]
    async fn accepts_configured_batch_count_with_valid_event_sizes() {
        let server = start();
        let batch: Vec<_> = (0..100).map(|_| serde_json::json!({ "name": "update", "channel": "news", "data": "x".repeat(2048) })).collect();
        let body = serde_json::json!({ "batch": batch }).to_string();
        let status = post(server.addr, "/apps/app/batch_events", &body, "secret").await;
        stop(server).await;
        assert_eq!(
            status, 200,
            "100 events below 8192 bytes must fit the configured batch limit"
        );
    }

    #[tokio::test]
    async fn rejects_missing_publish_fields() {
        let server = start();
        let mut results = Vec::new();
        for body in [
            "{}",
            r#"{"channel":"news","data":"{}"}"#,
            r#"{"name":"update","channel":"news"}"#,
            r#"{"name":"update","data":"{}"}"#,
            r#"{"name":"","channel":"news","data":"{}"}"#,
        ] {
            results.push((
                body,
                post(server.addr, "/apps/app/events", body, "secret").await,
            ));
        }
        stop(server).await;
        for (body, status) in results {
            assert_eq!(status, 400, "invalid trigger accepted: {body}");
        }
    }

    #[tokio::test]
    async fn rejects_missing_batch_fields() {
        let server = start();
        let mut results = Vec::new();
        for body in [
            "{}",
            r#"{"batch":[{"channel":"news","data":"{}"}]}"#,
            r#"{"batch":[{"name":"update","channel":"news"}]}"#,
        ] {
            results.push((
                body,
                post(server.addr, "/apps/app/batch_events", body, "secret").await,
            ));
        }
        stop(server).await;
        for (body, status) in results {
            assert_eq!(status, 400, "invalid batch accepted: {body}");
        }
    }

    #[tokio::test]
    async fn duplicate_channels_deliver_only_once() {
        let server = start();
        let mut socket = connect(server.addr).await;
        next_json(&mut socket).await;
        socket
            .send(Message::text(
                r#"{"event":"pusher:subscribe","data":{"channel":"news"}}"#,
            ))
            .await
            .unwrap();
        next_json(&mut socket).await;
        let status = post(
            server.addr,
            "/apps/app/events",
            r#"{"name":"update","channel":"news","channels":["news","news"],"data":"{}"}"#,
            "secret",
        )
        .await;
        if status == 200 {
            assert_eq!(next_json(&mut socket).await["event"], "update");
            socket
                .send(Message::text(r#"{"event":"pusher:ping","data":{}}"#))
                .await
                .unwrap();
            let next = next_json(&mut socket).await;
            stop(server).await;
            assert_eq!(
                next["event"], "pusher:pong",
                "duplicate delivery preceded the ping response"
            );
        } else {
            stop(server).await;
            assert_eq!(
                status, 400,
                "duplicate channel requests must be rejected or deduplicated"
            );
        }
    }

    #[tokio::test]
    async fn closes_connections_above_http_limit() {
        let server = start_config(
            Config::parse(
                &format!("{CONFIG}\n").replace("port = 0", "port = 0\nmax_http_connections = 1"),
            )
            .unwrap(),
        );
        let mut held = TcpStream::connect(server.addr).await.unwrap();
        held.write_all(b"GET /health HTTP/1.1\r\n").await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        let mut extra = TcpStream::connect(server.addr).await.unwrap();
        extra
            .write_all(b"GET /health HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        timeout(STEP_TIMEOUT, extra.read_to_end(&mut response))
            .await
            .unwrap()
            .ok();
        drop(held);
        stop(server).await;
        assert!(
            response.is_empty(),
            "connection over the limit must be closed without a response"
        );
    }
}
