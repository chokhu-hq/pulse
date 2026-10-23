#[cfg(not(target_os = "linux"))]
compile_error!("bench reads /proc and only supports linux");

mod cli;
mod http_client;
mod proc_stats;
mod publishers;
mod report;
mod scenario;
mod server;
mod subscribers;

use clap::Parser;

use crate::cli::Args;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let results = scenario::run_all(&args.bin).await?;
    report::write(&args, &results).await
}
