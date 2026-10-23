use std::path::PathBuf;

use clap::Parser;

/// Benchmarks a pulse binary and reports throughput, cpu and memory per scenario.
#[derive(Debug, Parser)]
#[command(name = "bench")]
pub(crate) struct Args {
    /// Path to the pulse binary under test
    #[arg(long, default_value = "./pulse")]
    pub(crate) bin: PathBuf,

    /// Write results as json to this file
    #[arg(long)]
    pub(crate) json: Option<PathBuf>,

    /// Write results as a markdown table to this file
    #[arg(long)]
    pub(crate) md: Option<PathBuf>,

    /// Replace the section between the bench markers in this file
    #[arg(long)]
    pub(crate) readme: Option<PathBuf>,

    /// Link to the CI run, which marks the report as measured on a shared runner
    #[arg(long, env = "BENCH_RUN_URL")]
    pub(crate) run_url: Option<String>,

    /// Commit being measured, shown next to the CI run link
    #[arg(long, env = "GITHUB_SHA")]
    pub(crate) commit: Option<String>,
}
