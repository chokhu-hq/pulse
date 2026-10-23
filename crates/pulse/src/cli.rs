use std::num::NonZeroUsize;
use std::path::PathBuf;

use clap::Parser;

/// Pusher protocol v7 compatible websocket server.
#[derive(Debug, Parser)]
#[command(name = "pulse", version)]
pub(crate) struct Args {
    /// Path to the toml config file
    #[arg(long, env = "PULSE_CONFIG", default_value = "config.toml")]
    pub(crate) config: PathBuf,

    /// Port to listen on, overriding the config file
    #[arg(long, env = "PULSE_PORT")]
    pub(crate) port: Option<u16>,

    /// Maximum concurrent websocket connections across all apps
    #[arg(long, default_value = "100000")]
    pub(crate) max_connections: NonZeroUsize,

    /// Enable debug logging, overriding the config file
    #[arg(long)]
    pub(crate) debug: bool,
}
