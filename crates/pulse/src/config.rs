use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::apps::{AppFile, Apps};

const DEFAULT_PORT: u16 = 8080;
const DEFAULT_MAX_CONNECTIONS: NonZeroUsize = match NonZeroUsize::new(100_000) {
    Some(n) => n,
    None => NonZeroUsize::MIN,
};

/// Server configuration, loaded from the toml config file and then overridden by flags.
#[derive(Debug)]
pub struct Config {
    /// Enables debug logging.
    pub debug: bool,
    /// Port the http and websocket server listens on.
    pub port: u16,
    /// Maximum concurrent websocket connections across all apps.
    pub max_connections: NonZeroUsize,
    pub(crate) apps: Apps,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            debug: false,
            port: DEFAULT_PORT,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            apps: Apps::default(),
        }
    }
}

/// Reasons a config file cannot be loaded.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The config file could not be read.
    #[error("read config {}", path.display())]
    Read {
        /// Path of the config file.
        path: PathBuf,
        /// Underlying io error.
        #[source]
        source: std::io::Error,
    },
    /// The config file is not valid toml, has unknown fields or out of range values.
    #[error("parse config toml")]
    Parse(#[from] toml::de::Error),
    /// Two enabled apps share the same id or key.
    #[error("duplicate app {field} {value:?}")]
    DuplicateApp {
        /// Which field is duplicated.
        field: &'static str,
        /// The duplicated value.
        value: String,
    },
    /// An app has an empty id, key or secret.
    #[error("app is missing {field}")]
    MissingAppField {
        /// The empty field.
        field: &'static str,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
struct ConfigFile {
    #[serde(default)]
    server: ServerSection,
    #[serde(default)]
    apps: Vec<AppFile>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "snake_case")]
struct ServerSection {
    debug: bool,
    port: Option<u16>,
    // unused, but accepted so existing config files keep loading
    hostname: String,
    region: String,
}

impl Config {
    /// Reads and validates the config file at `path`.
    pub async fn load(path: &Path) -> Result<Self, ConfigError> {
        let contents =
            tokio::fs::read_to_string(path)
                .await
                .map_err(|source| ConfigError::Read {
                    path: path.to_owned(),
                    source,
                })?;

        Self::parse(&contents)
    }

    /// Parses and validates config file contents.
    pub fn parse(contents: &str) -> Result<Self, ConfigError> {
        let file: ConfigFile = toml::from_str(contents)?;

        Ok(Self {
            debug: file.server.debug,
            port: file.server.port.unwrap_or(DEFAULT_PORT),
            apps: Apps::from_files(file.apps)?,
            ..Self::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_readme_example() {
        let toml = r#"
            [server]
            debug = true
            port = 8099
            hostname = ""
            region = ""

            [[apps]]
            id = "app"
            key = "key"
            secret = "secret"
            name = "pulse app"
            enabled = true
            max_connections = 10000
            max_channels_per_connection = 50
            allowed_origins = ["*"]
            max_message_size = 8192
            max_batch_events = 100
            enable_client_events = true
            max_event_rate = 10
            max_event_burst = 20
        "#;

        let config = Config::parse(toml).unwrap();

        assert!(config.debug, "debug from file");
        assert_eq!(config.port, 8099, "port from file");
        assert_eq!(config.apps.len(), 1, "one enabled app");
    }

    #[test]
    fn defaults_port_when_missing() {
        assert_eq!(
            Config::parse("").unwrap().port,
            DEFAULT_PORT,
            "default port"
        );
    }

    #[test]
    fn rejects_bad_input() {
        let cases = [
            ("[server]\nport = 70000", "port out of range"),
            ("[server]\nport = \"8080\"", "port as a string"),
            ("[server]\nprot = 80", "unknown server field"),
            (
                "[[apps]]\nid = \"a\"\nkey = \"\"\nsecret = \"s\"\nenabled = true",
                "empty key",
            ),
            ("{\"server\": {}}", "json"),
        ];

        for (contents, case) in cases {
            assert!(
                Config::parse(contents).is_err(),
                "{case} should be rejected"
            );
        }
    }
}
