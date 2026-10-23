use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use hyper::server::conn::http1;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::http;
use crate::hub::Hub;
use crate::listener;

const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(60);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

/// A bound pulse server, ready to accept http and websocket connections.
#[derive(Debug)]
pub struct Server {
    listener: TcpListener,
    hub: Arc<Hub>,
}

/// Reasons the server fails to start or to stop cleanly.
#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    /// The listening socket could not be bound.
    #[error("bind port {port}")]
    Bind {
        /// Port that was requested.
        port: u16,
        /// Underlying io error.
        #[source]
        source: io::Error,
    },
    /// The bound address could not be read back.
    #[error("read listen address")]
    LocalAddr(#[source] io::Error),
    /// Requests or websocket sessions were still open when the shutdown deadline passed.
    #[error("connections still open {0:?} after shutdown")]
    ShutdownTimeout(Duration),
}

impl Server {
    /// Binds the configured port on all interfaces, dual stack when ipv6 is available.
    ///
    /// Must be called from within a tokio runtime, which the listener registers with.
    pub fn bind(config: Config) -> Result<Self, ServeError> {
        let port = config.port;
        let listener = listener::bind(port).map_err(|source| ServeError::Bind { port, source })?;

        info!(count = config.apps.len(), "loaded apps");
        for app in config.apps.iter() {
            info!(name = %app.name, key = %app.key, max_connections = app.max_connections.map(NonZeroUsize::get), "app registered");
        }

        let hub = Arc::new(Hub::new(config, CancellationToken::new()));
        Ok(Self { listener, hub })
    }

    /// Address the server is listening on.
    pub fn local_addr(&self) -> Result<SocketAddr, ServeError> {
        self.listener.local_addr().map_err(ServeError::LocalAddr)
    }

    /// Serves until `shutdown` resolves, then drains http requests and closes websocket sessions.
    pub async fn run(self, shutdown: impl Future<Output = ()>) -> Result<(), ServeError> {
        let Self { listener, hub } = self;
        let tracker = TaskTracker::new();
        let router = http::router(Arc::clone(&hub), tracker.clone());
        let mut shutdown = pin!(shutdown);
        info!(address = %self_addr(&listener), "pulse server listening");

        loop {
            tokio::select! {
                () = &mut shutdown => break,
                accepted = listener.accept() => match accepted {
                    Ok((stream, _)) => serve_connection(stream, &router, &tracker, &hub.shutdown),
                    Err(err) => {
                        warn!(error = %err, "accept connection");
                        sleep(ACCEPT_ERROR_BACKOFF).await;
                    }
                },
            }
        }

        info!("shutting down server");
        drop(listener);
        hub.shutdown.cancel();
        tracker.close();
        timeout(SHUTDOWN_TIMEOUT, tracker.wait())
            .await
            .map_err(|_elapsed| ServeError::ShutdownTimeout(SHUTDOWN_TIMEOUT))?;
        info!("server stopped");
        Ok(())
    }
}

fn self_addr(listener: &TcpListener) -> String {
    listener
        .local_addr()
        .map_or_else(|err| format!("unknown ({err})"), |addr| addr.to_string())
}

fn serve_connection(
    stream: TcpStream,
    router: &Router,
    tracker: &TaskTracker,
    shutdown: &CancellationToken,
) {
    // pusher frames are small and latency sensitive, matching go's default
    if let Err(err) = stream.set_nodelay(true) {
        debug!(error = %err, "set tcp nodelay");
    }

    let conn = http1::Builder::new()
        .timer(TokioTimer::new())
        .header_read_timeout(HEADER_READ_TIMEOUT)
        .serve_connection(
            TokioIo::new(stream),
            TowerToHyperService::new(router.clone()),
        )
        .with_upgrades();
    let shutdown = shutdown.clone();

    // upgraded connections complete here and live on as tracked websocket sessions
    tracker.spawn(async move {
        let mut conn = pin!(conn);
        let result = tokio::select! {
            result = conn.as_mut() => result,
            () = shutdown.cancelled() => {
                conn.as_mut().graceful_shutdown();
                conn.await
            }
        };
        if let Err(err) = result {
            debug!(error = %err, "serve http connection");
        }
    });
}
