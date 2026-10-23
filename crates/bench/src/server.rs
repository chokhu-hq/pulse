use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, bail};
use tokio::process::{Child, Command};
use tokio::time::{Instant, interval};

use crate::http_client;

pub(crate) const APP_ID: &str = "bench-app";
pub(crate) const APP_KEY: &str = "bench-key";
pub(crate) const APP_SECRET: &str = "bench-secret";

const HEALTH_TIMEOUT: Duration = Duration::from_secs(10);
const HEALTH_POLL_INTERVAL: Duration = Duration::from_millis(100);

// the child is killed when this is dropped, so a failed scenario never leaks a server
pub(crate) struct Server {
    child: Child,
    pub(crate) addr: SocketAddr,
    config_dir: PathBuf,
}

impl Server {
    pub(crate) async fn start(bin: &Path) -> anyhow::Result<Self> {
        let port = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .and_then(|listener| listener.local_addr())
            .context("find a free port")?
            .port();
        let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));

        let config_dir =
            std::env::temp_dir().join(format!("pulse-bench-{}-{port}", std::process::id()));
        tokio::fs::create_dir_all(&config_dir)
            .await
            .context("create config dir")?;
        let config_path = config_dir.join("config.toml");
        tokio::fs::write(&config_path, config(port))
            .await
            .context("write config")?;

        let child = Command::new(bin)
            .arg("--config")
            .arg(&config_path)
            .arg("--max-connections=200000")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("start {}", bin.display()))?;

        let mut server = Self {
            child,
            addr,
            config_dir,
        };
        server.wait_healthy().await?;
        Ok(server)
    }

    pub(crate) fn pid(&self) -> anyhow::Result<u32> {
        self.child.id().context("server process has exited")
    }

    async fn wait_healthy(&mut self) -> anyhow::Result<()> {
        let deadline = Instant::now() + HEALTH_TIMEOUT;
        let mut poll = interval(HEALTH_POLL_INTERVAL);
        while Instant::now() < deadline {
            poll.tick().await;
            if let Some(status) = self.child.try_wait().context("check server process")? {
                bail!("server exited early with {status}");
            }
            if http_client::get_ok(self.addr, "/health").await.is_ok() {
                return Ok(());
            }
        }

        bail!("server did not become healthy on {}", self.addr)
    }

    pub(crate) async fn stop(mut self) -> anyhow::Result<()> {
        self.child.kill().await.context("stop server")?;
        tokio::fs::remove_dir_all(&self.config_dir)
            .await
            .context("remove config dir")
    }
}

// every value is a constant or a number, so nothing needs escaping
fn config(port: u16) -> String {
    format!(
        r#"[server]
port = {port}

[[apps]]
id = "{APP_ID}"
key = "{APP_KEY}"
secret = "{APP_SECRET}"
name = "bench"
enabled = true
max_message_size = 8192
enable_client_events = false
"#
    )
}
