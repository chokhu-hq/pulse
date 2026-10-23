use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::Serialize;
use tokio::time::{Instant, interval, sleep};

use crate::proc_stats;
use crate::publishers;
use crate::report;
use crate::server::Server;
use crate::subscribers;

// lets connection buffers settle before memory is sampled
const SETTLE_TIME: Duration = Duration::from_millis(500);
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(30);
const STALL_TIMEOUT: Duration = Duration::from_secs(1);
const DELIVERY_POLL_INTERVAL: Duration = Duration::from_millis(1);

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct Scenario {
    pub(crate) name: &'static str,
    pub(crate) subscribers: usize,
    pub(crate) events: usize,
    pub(crate) publishers: usize,
    pub(crate) payload_bytes: usize,
}

pub(crate) const SCENARIOS: [Scenario; 4] = [
    Scenario {
        name: "publish throughput",
        subscribers: 1,
        events: 100_000,
        publishers: 256,
        payload_bytes: 200,
    },
    Scenario {
        name: "fan-out 100 subscribers",
        subscribers: 100,
        events: 10_000,
        publishers: 32,
        payload_bytes: 200,
    },
    Scenario {
        name: "fan-out 1000 subscribers",
        subscribers: 1000,
        events: 2000,
        publishers: 16,
        payload_bytes: 200,
    },
    Scenario {
        name: "fan-out 5000 subscribers",
        subscribers: 5000,
        events: 500,
        publishers: 16,
        payload_bytes: 200,
    },
];

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct Outcome {
    #[serde(flatten)]
    pub(crate) scenario: Scenario,
    pub(crate) publishes_per_sec: f64,
    pub(crate) deliveries_per_sec: f64,
    pub(crate) delivered: u64,
    pub(crate) expected: u64,
    #[serde(rename = "server_cpu_us_per_delivery")]
    pub(crate) cpu_us_per_delivery: f64,
    pub(crate) rss_per_conn_kb: f64,
}

pub(crate) async fn run_all(bin: &Path) -> anyhow::Result<Vec<Outcome>> {
    let mut outcomes = Vec::with_capacity(SCENARIOS.len());
    for scenario in SCENARIOS {
        report::progress(scenario.name);
        outcomes.push(run(bin, scenario).await?);
    }
    Ok(outcomes)
}

async fn run(bin: &Path, scenario: Scenario) -> anyhow::Result<Outcome> {
    let server = Server::start(bin).await?;
    let pid = server.pid()?;
    let idle = proc_stats::read(pid).await?;

    let received = Arc::new(AtomicU64::new(0));
    let readers = subscribers::subscribe_all(server.addr, scenario.subscribers, &received).await?;
    sleep(SETTLE_TIME).await;
    let subscribed = proc_stats::read(pid).await?;

    let body = publishers::event_body(scenario.payload_bytes)?;
    let start = Instant::now();
    publishers::publish_all(server.addr, body, scenario.events, scenario.publishers).await?;
    let publish_time = start.elapsed();

    let expected = to_u64(scenario.subscribers).saturating_mul(to_u64(scenario.events));
    let delivery_time = wait_delivered(&received, expected)
        .await
        .duration_since(start);
    let finished = proc_stats::read(pid).await?;
    drop(readers);
    server.stop().await?;

    let delivered = received.load(Ordering::Relaxed);
    let cpu = finished.cpu.saturating_sub(subscribed.cpu).as_secs_f64();
    let rss = subscribed.rss_kb.saturating_sub(idle.rss_kb);
    Ok(Outcome {
        scenario,
        publishes_per_sec: to_f64(to_u64(scenario.events)) / publish_time.as_secs_f64(),
        deliveries_per_sec: to_f64(delivered) / delivery_time.as_secs_f64(),
        delivered,
        expected,
        cpu_us_per_delivery: if delivered == 0 {
            0.0
        } else {
            cpu * 1e6 / to_f64(delivered)
        },
        rss_per_conn_kb: to_f64(rss) / to_f64(to_u64(scenario.subscribers)),
    })
}

// returns when the last delivery was seen, so waiting out dropped messages is not counted
async fn wait_delivered(received: &AtomicU64, expected: u64) -> Instant {
    let deadline = Instant::now() + DELIVERY_TIMEOUT;
    let mut poll = interval(DELIVERY_POLL_INTERVAL);
    let mut last = received.load(Ordering::Relaxed);
    let mut last_progress = Instant::now();

    while last < expected && Instant::now() < deadline && last_progress.elapsed() < STALL_TIMEOUT {
        poll.tick().await;
        let current = received.load(Ordering::Relaxed);
        if current != last {
            last = current;
            last_progress = Instant::now();
        }
    }
    last_progress
}

fn to_u64(n: usize) -> u64 {
    u64::try_from(n).unwrap_or(u64::MAX)
}

// benchmark counts stay far below u32::MAX, so saturating there loses nothing in practice
fn to_f64(n: u64) -> f64 {
    u32::try_from(n).map_or_else(|_| f64::from(u32::MAX), f64::from)
}
