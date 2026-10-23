#![expect(clippy::print_stdout, clippy::print_stderr, reason = "cli output")]

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail};
use tokio::io::AsyncWriteExt;

use crate::cli::Args;
use crate::scenario::{Outcome, SCENARIOS};

const START_MARKER: &str = "<!-- bench:start -->";
const END_MARKER: &str = "<!-- bench:end -->";
const RSS_MIN_SUBSCRIBERS: usize = 1000;

pub(crate) fn progress(scenario: &str) {
    eprintln!("running {scenario:?}...");
}

pub(crate) async fn write(args: &Args, outcomes: &[Outcome]) -> anyhow::Result<()> {
    let cpuinfo = tokio::fs::read_to_string("/proc/cpuinfo")
        .await
        .context("read /proc/cpuinfo")?;
    let table = markdown(args, outcomes, cpu_model(&cpuinfo), SystemTime::now());
    let report = format!("## Pulse benchmarks\n\n{table}");
    print!("{report}");

    if let Some(path) = &args.readme {
        update_readme(path, &table).await?;
    }
    if let Some(path) = &args.md {
        write_atomic(path, &report).await?;
    }
    if let Some(path) = &args.json {
        write_atomic(path, &serde_json::to_string_pretty(outcomes)?).await?;
    }
    Ok(())
}

// shared runners land on different cpu models, which explains most run to run swings
fn markdown(args: &Args, outcomes: &[Outcome], cpu_model: Option<&str>, now: SystemTime) -> String {
    let runner_note = args.run_url.as_deref().map_or_else(String::new, |url| {
        let commit = args.commit.as_deref().unwrap_or_default();
        let commit = commit.get(..7).unwrap_or(commit);
        format!(
            "> Measured on a shared GitHub Actions runner. Expect lower numbers than dedicated hardware \
             and swings between runs. [Run]({url}) for commit `{commit}`.\n\n"
        )
    });
    let cpus = std::thread::available_parallelism().map_or(0, std::num::NonZero::get);
    let cpu_model = cpu_model.map_or_else(String::new, |model| format!("{model} with "));
    let payload = SCENARIOS
        .first()
        .map_or(0, |scenario| scenario.payload_bytes);
    let rows: String = outcomes.iter().map(row).collect::<Vec<_>>().concat();

    format!(
        "{runner_note}\
         - **Host:** {cpu_model}{cpus} CPUs on {os}/{arch}\n\
         - **Payload:** {payload} bytes\n\
         - **Measured:** {date} with the server and load generator on the same host\n\n\
         | Scenario | Subscribers | Publishers | Publishes/s | Deliveries/s | Dropped | Server CPU / delivery | RSS / conn |\n\
         |---|---:|---:|---:|---:|---:|---:|---:|\n\
         {rows}",
        os = std::env::consts::OS,
        arch = go_arch(std::env::consts::ARCH),
        date = utc_date(now),
    )
}

// x86 reports "model name"; arm kernels usually omit it, so the header leaves it out
fn cpu_model(cpuinfo: &str) -> Option<&str> {
    cpuinfo.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        (key.trim() == "model name").then(|| value.trim())
    })
}

fn row(outcome: &Outcome) -> String {
    let scenario = &outcome.scenario;
    let rss = if scenario.subscribers >= RSS_MIN_SUBSCRIBERS {
        format!("{:.1} KB", outcome.rss_per_conn_kb)
    } else {
        "-".to_owned()
    };

    format!(
        "| {} | {} | {} | {} | {} | {} | {:.2} \u{b5}s | {rss} |\n",
        scenario.name,
        scenario.subscribers,
        scenario.publishers,
        human(outcome.publishes_per_sec),
        human(outcome.deliveries_per_sec),
        outcome.expected.saturating_sub(outcome.delivered),
        outcome.cpu_us_per_delivery,
    )
}

// keeps the table comparable with results recorded by the old go benchmark
fn go_arch(arch: &str) -> &str {
    match arch {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        other => other,
    }
}

fn human(value: f64) -> String {
    if value >= 1e6 {
        format!("{:.2}M", value / 1e6)
    } else if value >= 1e3 {
        format!("{:.1}k", value / 1e3)
    } else {
        format!("{value:.0}")
    }
}

// civil date from days since the unix epoch, after Howard Hinnant's days_from_civil inverse
fn utc_date(now: SystemTime) -> String {
    let secs = now
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs());
    let days = i64::try_from(secs / 86_400).unwrap_or(0) + 719_468;
    let era = days.div_euclid(146_097);
    let day_of_era = days.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * shifted_month + 2) / 5 + 1;
    let month = if shifted_month < 10 {
        shifted_month + 3
    } else {
        shifted_month - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);

    format!("{year:04}-{month:02}-{day:02}")
}

async fn update_readme(path: &Path, table: &str) -> anyhow::Result<()> {
    let readme = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("read {}", path.display()))?;
    let (Some((before, rest)), true) =
        (readme.split_once(START_MARKER), readme.contains(END_MARKER))
    else {
        bail!(
            "{} is missing the {START_MARKER} / {END_MARKER} markers",
            path.display()
        );
    };
    let Some((_, after)) = rest.split_once(END_MARKER) else {
        bail!(
            "{END_MARKER} must come after {START_MARKER} in {}",
            path.display()
        );
    };

    write_atomic(
        path,
        &format!("{before}{START_MARKER}\n\n{table}\n{END_MARKER}{after}"),
    )
    .await
}

async fn write_atomic(path: &Path, contents: &str) -> anyhow::Result<()> {
    let temp = path.with_extension("tmp");
    let mut file = tokio::fs::File::create(&temp)
        .await
        .with_context(|| format!("create {}", temp.display()))?;
    file.write_all(contents.as_bytes())
        .await
        .with_context(|| format!("write {}", temp.display()))?;
    file.sync_all()
        .await
        .with_context(|| format!("sync {}", temp.display()))?;
    tokio::fs::rename(&temp, path)
        .await
        .with_context(|| format!("replace {}", path.display()))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn utc_dates() {
        let cases = [
            (0, "1970-01-01"),
            (1_353_088_179, "2012-11-16"),
            (951_782_400, "2000-02-29"),
            (4_107_542_400, "2100-03-01"),
        ];

        for (secs, expected) in cases {
            assert_eq!(
                utc_date(UNIX_EPOCH + Duration::from_secs(secs)),
                expected,
                "date of {secs}"
            );
        }
    }

    #[test]
    fn cpu_models() {
        let x86 = "processor\t: 0\nvendor_id\t: AuthenticAMD\nmodel name\t: AMD EPYC 7763 64-Core Processor\n";
        let arm = "processor\t: 0\nBogoMIPS\t: 50.00\nCPU part\t: 0xd0c\n";

        assert_eq!(
            cpu_model(x86),
            Some("AMD EPYC 7763 64-Core Processor"),
            "x86 model name"
        );
        assert_eq!(cpu_model(arm), None, "arm has no model name");
    }

    #[test]
    fn human_numbers() {
        let cases = [(12.4, "12"), (22_900.0, "22.9k"), (2_620_000.0, "2.62M")];

        for (value, expected) in cases {
            assert_eq!(human(value), expected, "human {value}");
        }
    }
}
