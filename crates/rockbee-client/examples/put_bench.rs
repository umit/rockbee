use futures_util::stream::{FuturesUnordered, StreamExt};
use rockbee_client::Client;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::env;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_ADDRESS: &str = "127.0.0.1:7100";
const DEFAULT_SECONDS: u64 = 60;
const DEFAULT_CONCURRENCY: usize = 128;
const DEFAULT_VALUE_BYTES: usize = 1024;
const MAX_ERRORS: u64 = 100;
const MAX_LATENCY_SAMPLES: usize = 4096;
const MAX_ACKNOWLEDGED_SAMPLES: usize = 32;

struct Config {
    address: String,
    duration: Duration,
    concurrency: usize,
    prefix: String,
    value_bytes: usize,
}

struct Stats {
    attempts: u64,
    acknowledged_before_deadline: u64,
    acknowledged_after_deadline: u64,
    errors: ErrorCounts,
    on_window_latencies_us: Reservoir,
    drained_latencies_us: Reservoir,
    acknowledged_samples: Vec<AcknowledgedSample>,
    error_messages: Vec<String>,
}

#[derive(Default)]
struct ErrorCounts {
    server: u64,
    admission: u64,
    transport_protocol: u64,
    timeout: u64,
}

struct Reservoir {
    seen: u64,
    state: u64,
    values: Vec<u64>,
    max: Option<u64>,
}

struct AcknowledgedSample {
    key: String,
    value_sha256: String,
}

struct Completion {
    key: String,
    started: Instant,
    result: rockbee_client::Result<()>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = parse_args(env::args().skip(1))?;
    let value = Arc::new(vec![b'x'; config.value_bytes]);
    let value_sha256 = hex_sha256(&value);
    let client = Client::connect(&config.address).await?;
    let started = Instant::now();
    let deadline = started + config.duration;
    let mut total = Stats::new(1);
    let mut sequence = 0_u64;
    let mut in_flight = FuturesUnordered::new();
    loop {
        while in_flight.len() < config.concurrency
            && Instant::now() < deadline
            && total.errors.total() < MAX_ERRORS
        {
            let key = format!("{}:{sequence}", config.prefix);
            sequence += 1;
            total.attempts += 1;
            in_flight.push(issue_put(client.clone(), key, value.clone()));
        }

        let Some(completion) = in_flight.next().await else {
            break;
        };
        total.record(completion, deadline, &value_sha256);
    }
    let finished = Instant::now();
    assert_eq!(total.attempts, total.completed_attempts());
    let process = process_usage();
    let acknowledged_samples: Vec<_> = total
        .acknowledged_samples
        .iter()
        .map(|sample| json!({"key": sample.key, "value_sha256": sample.value_sha256}))
        .collect();
    println!(
        "{}",
        serde_json::to_string(&json!({
            "benchmark": "rockbee-client-async-put",
            "client_put_api": "eager_handle",
            "issue_timing": "Client::put is called before its handle enters FuturesUnordered; latency starts at that call",
            "address": config.address,
            "prefix": config.prefix,
            "requested_window_seconds": config.duration.as_secs_f64(),
            "actual_window_seconds": deadline.duration_since(started).as_secs_f64(),
            "monotonic_ns_from_start": {
                "start": 0_u128,
                "deadline": deadline.duration_since(started).as_nanos(),
                "end": finished.duration_since(started).as_nanos(),
            },
            "drain_seconds": finished.saturating_duration_since(deadline).as_secs_f64(),
            "concurrency": config.concurrency,
            "producer_pattern": "one producer with a bounded FuturesUnordered pipeline",
            "pipeline_capacity": config.concurrency,
            "value_bytes": config.value_bytes,
            "attempts": total.attempts,
            "issued_attempts": total.attempts,
            "queued_attempts": total.attempts,
            "queued_attempts_scope": "PutHandle values tracked by the benchmark, not confirmed wire submissions",
            "completed_attempts": total.completed_attempts(),
            "ack_before_deadline": total.acknowledged_before_deadline,
            "ack_after_deadline_drained": total.acknowledged_after_deadline,
            "errors": total.errors.total(),
            "error_categories": total.errors_json(),
            "error_messages": total.error_messages,
            "ack_rate_per_second": total.acknowledged_before_deadline as f64 / config.duration.as_secs_f64(),
            "latency_us": {
                "on_window": total.on_window_latencies_us.summary(),
                "late_drained": total.drained_latencies_us.summary(),
            },
            "acknowledged_samples": acknowledged_samples,
            "process_usage": process,
            "error_limit": MAX_ERRORS,
        }))?
    );
    if total.errors.total() != 0 {
        return Err(format!("benchmark reported {} failed PUTs", total.errors.total()).into());
    }
    Ok(())
}

fn issue_put(
    client: Client,
    key: String,
    value: Arc<Vec<u8>>,
) -> impl Future<Output = Completion> + Send + 'static {
    let started = Instant::now();
    // Calling put here, rather than inside async move, starts the SDK work now.
    let handle = client.put(key.as_bytes(), &value, None);
    async move {
        let result = handle.await;
        Completion {
            key,
            started,
            result: result.map_err(Into::into),
        }
    }
}

impl Stats {
    fn new(seed: u64) -> Self {
        Self {
            attempts: 0,
            acknowledged_before_deadline: 0,
            acknowledged_after_deadline: 0,
            errors: ErrorCounts::default(),
            on_window_latencies_us: Reservoir::new(seed),
            drained_latencies_us: Reservoir::new(seed ^ 0x9e37_79b9_7f4a_7c15),
            acknowledged_samples: Vec::new(),
            error_messages: Vec::new(),
        }
    }

    fn record(&mut self, completion: Completion, deadline: Instant, value_sha256: &str) {
        match completion.result {
            Ok(()) => {
                let completed = Instant::now();
                let latency_us = completed
                    .saturating_duration_since(completion.started)
                    .as_micros()
                    .try_into()
                    .unwrap_or(u64::MAX);
                if completed < deadline {
                    self.acknowledged_before_deadline += 1;
                    self.on_window_latencies_us.record(latency_us);
                } else {
                    self.acknowledged_after_deadline += 1;
                    self.drained_latencies_us.record(latency_us);
                }
                if self.acknowledged_samples.len() < MAX_ACKNOWLEDGED_SAMPLES {
                    self.acknowledged_samples.push(AcknowledgedSample {
                        key: completion.key,
                        value_sha256: value_sha256.to_owned(),
                    });
                }
            }
            Err(error) => {
                let message = error.to_string();
                self.errors.record(&message);
                if self.error_messages.len() < 32 {
                    self.error_messages.push(message);
                }
            }
        }
    }

    fn completed_attempts(&self) -> u64 {
        self.acknowledged_before_deadline + self.acknowledged_after_deadline + self.errors.total()
    }

    fn errors_json(&self) -> serde_json::Value {
        json!({
            "server": self.errors.server,
            "admission": self.errors.admission,
            "transport_or_protocol": self.errors.transport_protocol,
            "timeout": self.errors.timeout,
        })
    }
}

impl ErrorCounts {
    fn total(&self) -> u64 {
        self.server + self.admission + self.transport_protocol + self.timeout
    }

    fn record(&mut self, message: &str) {
        if message.contains("timed out") || message.contains("deadline") {
            self.timeout += 1;
        } else if message.contains("pending limit") || message.contains("admission") {
            self.admission += 1;
        } else if message.contains("upstream")
            || message.contains("link")
            || message.contains("frame")
        {
            self.transport_protocol += 1;
        } else {
            self.server += 1;
        }
    }
}

impl Reservoir {
    fn new(seed: u64) -> Self {
        Self {
            seen: 0,
            state: seed.max(1),
            values: Vec::with_capacity(MAX_LATENCY_SAMPLES),
            max: None,
        }
    }

    fn record(&mut self, latency_us: u64) {
        self.seen += 1;
        self.max = Some(self.max.map_or(latency_us, |max| max.max(latency_us)));
        if self.values.len() < MAX_LATENCY_SAMPLES {
            self.values.push(latency_us);
            return;
        }
        self.state ^= self.state << 13;
        self.state ^= self.state >> 7;
        self.state ^= self.state << 17;
        let index = self.state % self.seen;
        if index < MAX_LATENCY_SAMPLES as u64 {
            self.values[index as usize] = latency_us;
        }
    }

    fn summary(&self) -> serde_json::Value {
        let mut values = self.values.clone();
        values.sort_unstable();
        json!({
            "acknowledgements": self.seen,
            "sample_count": values.len(),
            "sampling": "bounded reservoir",
            "p50": percentile(&values, 50),
            "p95": percentile(&values, 95),
            "p99": percentile(&values, 99),
            "max": self.max,
        })
    }
}

fn percentile(values: &[u64], percentile: usize) -> Option<u64> {
    (!values.is_empty()).then(|| values[(values.len() - 1) * percentile / 100])
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Config, String> {
    let mut address = DEFAULT_ADDRESS.to_owned();
    let mut seconds = DEFAULT_SECONDS;
    let mut concurrency = DEFAULT_CONCURRENCY;
    let mut prefix = None;
    let mut value_bytes = DEFAULT_VALUE_BYTES;
    let mut args = args.into_iter();
    while let Some(flag) = args.next() {
        let value = args.next().ok_or_else(|| usage().to_owned())?;
        match flag.as_str() {
            "--addr" => address = value,
            "--duration-seconds" => seconds = parse_positive(&value, "duration")?,
            "--concurrency" => concurrency = parse_positive(&value, "concurrency")?,
            "--prefix" => prefix = Some(value),
            "--value-bytes" => value_bytes = parse_positive(&value, "value bytes")?,
            _ => return Err(usage().to_owned()),
        }
    }
    let prefix = prefix.unwrap_or_else(default_prefix);
    Ok(Config {
        address,
        duration: Duration::from_secs(seconds),
        concurrency,
        prefix,
        value_bytes,
    })
}

fn parse_positive<T: std::str::FromStr + PartialOrd + From<u8>>(
    value: &str,
    name: &str,
) -> Result<T, String> {
    let value: T = value
        .parse()
        .map_err(|_| format!("invalid {name}: {value}"))?;
    (value > T::from(0))
        .then_some(value)
        .ok_or_else(|| format!("{name} must be positive"))
}

fn default_prefix() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("async-put-{}-{millis}", std::process::id())
}

fn hex_sha256(value: &[u8]) -> String {
    let digest = Sha256::digest(value);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn process_usage() -> serde_json::Value {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if result != 0 {
        return json!({"available": false});
    }
    let usage = unsafe { usage.assume_init() };
    let user_seconds = usage.ru_utime.tv_sec as f64 + usage.ru_utime.tv_usec as f64 / 1_000_000.0;
    let system_seconds = usage.ru_stime.tv_sec as f64 + usage.ru_stime.tv_usec as f64 / 1_000_000.0;
    #[cfg(target_os = "macos")]
    let peak_rss_bytes = usage.ru_maxrss as u64;
    #[cfg(not(target_os = "macos"))]
    let peak_rss_bytes = usage.ru_maxrss as u64 * 1024;
    json!({
        "available": true,
        "scope": "benchmark_client_process",
        "user_cpu_seconds": user_seconds,
        "system_cpu_seconds": system_seconds,
        "peak_rss_bytes": peak_rss_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accounting_separates_on_window_late_and_failed_puts() {
        let mut stats = Stats::new(1);
        stats.attempts = 3;
        let started = Instant::now();
        stats.record(
            Completion {
                key: "on-time".into(),
                started,
                result: Ok(()),
            },
            started + Duration::from_secs(60),
            "digest",
        );
        stats.record(
            Completion {
                key: "late".into(),
                started,
                result: Ok(()),
            },
            started,
            "digest",
        );
        stats.record(
            Completion {
                key: "failed".into(),
                started,
                result: Err("read-only server".into()),
            },
            started,
            "digest",
        );
        assert_eq!(stats.acknowledged_before_deadline, 1);
        assert_eq!(stats.acknowledged_after_deadline, 1);
        assert_eq!(stats.errors.total(), 1);
        assert_eq!(stats.completed_attempts(), stats.attempts);
        assert_eq!(stats.on_window_latencies_us.seen, 1);
        assert_eq!(stats.drained_latencies_us.seen, 1);
        assert_eq!(stats.acknowledged_samples.len(), 2);
        assert!(
            stats
                .acknowledged_samples
                .iter()
                .all(|sample| sample.key != "failed")
        );
    }

    #[test]
    fn latency_memory_stays_bounded_while_total_and_max_cover_all_acks() {
        let mut samples = Reservoir::new(1);
        for latency in 0..10_000 {
            samples.record(latency);
        }
        let summary = samples.summary();
        assert_eq!(summary["acknowledgements"], 10_000);
        assert_eq!(summary["sample_count"], MAX_LATENCY_SAMPLES);
        assert_eq!(summary["max"], 9_999);
        assert_eq!(samples.values.len(), MAX_LATENCY_SAMPLES);
    }
}

fn usage() -> &'static str {
    "Usage: cargo run -p rockbee-client --example put_bench -- [--addr HOST:PORT] [--duration-seconds SECONDS] [--concurrency WORKERS] [--prefix PREFIX] [--value-bytes BYTES]"
}
