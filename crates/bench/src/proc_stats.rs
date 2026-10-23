use std::time::Duration;

use anyhow::Context;

// /proc reports cpu time in clock ticks, which is 100 per second on every linux we target
const CLOCK_TICKS_PER_SECOND: u64 = 100;

#[derive(Debug, Default)]
pub(crate) struct ProcStats {
    pub(crate) rss_kb: u64,
    pub(crate) cpu: Duration,
}

// linux only; elsewhere the cpu and memory columns read zero
pub(crate) async fn read(pid: u32) -> anyhow::Result<ProcStats> {
    if !cfg!(target_os = "linux") {
        return Ok(ProcStats::default());
    }

    let status = tokio::fs::read_to_string(format!("/proc/{pid}/status"))
        .await
        .context("read proc status")?;
    let stat = tokio::fs::read_to_string(format!("/proc/{pid}/stat"))
        .await
        .context("read proc stat")?;
    Ok(ProcStats {
        rss_kb: parse_rss_kb(&status)?,
        cpu: parse_cpu(&stat)?,
    })
}

fn parse_rss_kb(status: &str) -> anyhow::Result<u64> {
    let line = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .context("VmRSS missing from proc status")?;
    let kb = line
        .split_whitespace()
        .next()
        .context("VmRSS has no value")?;
    kb.parse().context("parse VmRSS")
}

// fields after the parenthesised command name, which may itself contain spaces
fn parse_cpu(stat: &str) -> anyhow::Result<Duration> {
    let (_, fields) = stat
        .rsplit_once(')')
        .context("proc stat has no command name")?;
    let mut fields = fields.split_whitespace().skip(11);
    let mut next_ticks = |name| -> anyhow::Result<u64> {
        let field = fields
            .next()
            .with_context(|| format!("{name} missing from proc stat"))?;
        field.parse().with_context(|| format!("parse {name}"))
    };
    let ticks = next_ticks("utime")?.saturating_add(next_ticks("stime")?);

    Ok(Duration::from_millis(
        ticks.saturating_mul(1000) / CLOCK_TICKS_PER_SECOND,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_proc_files() {
        let status = "Name:\tpulse\nVmPeak:\t  2000 kB\nVmRSS:\t  1234 kB\n";
        let stat = "42 (pul se) S 1 42 42 0 -1 4194560 100 0 0 0 250 50 0 0 20 0 4 0 1 1 1";

        assert_eq!(parse_rss_kb(status).unwrap(), 1234, "rss");
        assert_eq!(
            parse_cpu(stat).unwrap(),
            Duration::from_secs(3),
            "user plus system ticks"
        );
        assert!(parse_cpu("42 (pulse) S 1").is_err(), "truncated stat");
    }
}
