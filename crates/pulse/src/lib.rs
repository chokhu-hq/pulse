//! Pusher protocol v7 compatible websocket server for linux.
#![warn(missing_docs)]

#[cfg(not(target_os = "linux"))]
compile_error!("pulse only supports linux");

mod apps;
mod auth;
mod channel;
mod channels;
mod config;
mod connection;
mod http;
mod hub;
mod inbound;
mod listener;
mod presence;
mod protocol;
mod rate_limit;
mod server;
mod session;
mod socket_id;
mod subscriptions;

pub use config::{Config, ConfigError};
pub use server::{ServeError, Server};
