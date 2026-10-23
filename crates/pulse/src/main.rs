mod cli;

use std::io::IsTerminal;

use anyhow::Context;
use clap::Parser;
use tracing::{Level, error, info};

use crate::cli::Args;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let mut config = pulse::Config::load(&args.config)
        .await
        .with_context(|| format!("load config {}", args.config.display()))?;
    config.debug |= args.debug;
    config.port = args.port.unwrap_or(config.port);
    config.max_connections = args.max_connections;

    init_tracing(config.debug);
    info!(path = %args.config.display(), debug = config.debug, "config loaded");

    let server = pulse::Server::bind(config).await?;
    server.run(shutdown_signal()).await?;
    Ok(())
}

fn init_tracing(debug: bool) {
    let level = if debug { Level::DEBUG } else { Level::INFO };
    let color = std::io::stderr().is_terminal() && std::env::var_os("NO_COLOR").is_none();
    tracing_subscriber::fmt()
        .with_max_level(level)
        .with_ansi(color)
        .with_writer(std::io::stderr)
        .init();
}

async fn ctrl_c() {
    if let Err(err) = tokio::signal::ctrl_c().await {
        error!(error = %err, "listen for ctrl-c");
        std::future::pending::<()>().await;
    }
}

#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = match signal(SignalKind::terminate()) {
        Ok(terminate) => terminate,
        Err(err) => {
            error!(error = %err, "listen for sigterm");
            return ctrl_c().await;
        }
    };

    tokio::select! {
        () = ctrl_c() => {}
        _ = terminate.recv() => {}
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    ctrl_c().await;
}
