use futures_util::stream::{FuturesUnordered, StreamExt};
use rockbee_client::RockBeeClient;
use rockbee_protocol::MAX_FRAME;
use serde_json::json;
use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_ADDRESS: &str = "127.0.0.1:7100";
const DEFAULT_SECONDS: u64 = 60;
const DEFAULT_CONCURRENCY: usize = 128;
const DEFAULT_KEYS: usize = 1024;
const DEFAULT_VALUE_BYTES: usize = 1024;
const MAX_KEYS: usize = 65_536;
const MAX_VALUE_BYTES: usize = MAX_FRAME - 512;
const MAX_FIXTURE_BYTES: usize = 64 * 1024 * 1024;
const MAX_NAMESPACE_BYTES: usize = 256;
const MAX_ERRORS: u64 = 100;
const MAX_LATENCY_SAMPLES: usize = 4096;

struct Config {
    address: String,
    duration: Duration,
    concurrency: usize,
    keys: usize,
    value_bytes: usize,
    namespace: String,
    mode: Mode,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Prepare,
    VerifyOnly,
    MeasureExisting,
}
struct Fixture {
    key: Vec<u8>,
    value: Vec<u8>,
}
struct GetCompletion {
    fixture: Arc<Fixture>,
    started: Instant,
    finished: Instant,
    result: rockbee_client::Result<Option<Vec<u8>>>,
}
struct Stats {
    attempts: u64,
    before_deadline: u64,
    late_drained: u64,
    missing: u64,
    mismatched: u64,
    transport: u64,
    latency_us: Reservoir,
    errors: Vec<String>,
}
struct Reservoir {
    seen: u64,
    state: u64,
    values: Vec<u64>,
    max: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = parse_args(env::args().skip(1))?;
    run(config).await
}

async fn run(config: Config) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let fixtures = fixtures(&config.namespace, config.keys, config.value_bytes)?;
    let client = RockBeeClient::connect(&config.address).await?;
    if config.mode == Mode::VerifyOnly {
        let started = Instant::now();
        verify_existing(&client, &fixtures, config.concurrency).await?;
        println!(
            "{}",
            serde_json::to_string(&json!({
                "audit": "rockbee-client-fixture-verify",
                "mode": "verify_only",
                "namespace": config.namespace,
                "fixture_keys": config.keys,
                "fixture_value_bytes": config.value_bytes,
                "concurrency": config.concurrency,
                "verified_count": config.keys,
                "verification": "all_fixtures_exact_bytes",
                "errors": 0,
                "total_bytes": config.keys * config.value_bytes,
                "elapsed_seconds": started.elapsed().as_secs_f64(),
                "claims": [
                    "GET-only exact-byte fixture verification; peer role is established by the runner",
                    "not a cold-latency, cache-hit, or compaction proof"
                ],
            }))?
        );
        return Ok(());
    }
    if config.mode == Mode::MeasureExisting {
        let preflight_started = Instant::now();
        if let Err(error) = verify_existing(&client, &fixtures, config.concurrency).await {
            eprintln!(
                "{}",
                json!({
                    "event": "phase_failed",
                    "phase": "existing_fixture_preflight",
                    "namespace": config.namespace,
                    "error": error.to_string(),
                })
            );
            return Err(error);
        }
        let preflight_seconds = preflight_started.elapsed().as_secs_f64();
        eprintln!(
            "{}",
            json!({
                "event": "existing_fixture_verified",
                "namespace": config.namespace,
                "fixture_keys": config.keys,
                "fixture_value_bytes": config.value_bytes,
                "verified_count": config.keys,
                "concurrency": config.concurrency,
            })
        );
        return print_existing_fixture_measurement(&client, &fixtures, &config, preflight_seconds)
            .await;
    }
    eprintln!(
        "{}",
        json!({
            "event": "fixture_preparation_started",
            "namespace": config.namespace,
            "fixture_keys": config.keys,
            "fixture_value_bytes": config.value_bytes,
            "concurrency": config.concurrency,
        })
    );
    let preparation_started = Instant::now();
    if let Err(error) = prepare(&client, &fixtures, config.concurrency).await {
        eprintln!(
            "{}",
            json!({
                "event": "phase_failed",
                "phase": "fixture_preparation",
                "namespace": config.namespace,
                "error": error.to_string(),
            })
        );
        return Err(error);
    }
    let preparation_seconds = preparation_started.elapsed().as_secs_f64();
    eprintln!(
        "{}",
        json!({
            "event": "durable_fixture_puts_completed",
            "namespace": config.namespace,
            "fixture_keys": config.keys,
            "fixture_value_bytes": config.value_bytes,
            "concurrency": config.concurrency,
            "durable_acks": config.keys,
        })
    );
    let convergence_started = Instant::now();
    if let Err(error) = converge(&client, &fixtures, config.concurrency).await {
        eprintln!(
            "{}",
            json!({
                "event": "phase_failed",
                "phase": "fixture_convergence",
                "namespace": config.namespace,
                "error": error.to_string(),
            })
        );
        return Err(error);
    }
    let convergence_seconds = convergence_started.elapsed().as_secs_f64();
    let warmup_started = Instant::now();
    if !matches_all(&client, &fixtures, config.concurrency).await? {
        return Err("fixture warm GET did not return exact bytes".into());
    }
    let warmup_seconds = warmup_started.elapsed().as_secs_f64();

    let timed = timed_get(&client, &fixtures, &config).await?;
    let TimedRun {
        stats,
        started,
        finished,
    } = timed;
    let stop_reason = if stats.failures() >= MAX_ERRORS {
        "error_limit"
    } else {
        "scheduled_deadline"
    };
    assert_eq!(stats.attempts, stats.completed());
    println!(
        "{}",
        serde_json::to_string(&json!({
            "benchmark": "rockbee-client-warm-get",
            "workload": "warm_small_working_set",
            "claims": ["bounded closed-loop GETs over prepared exact-byte fixtures", "no S3, cache-hit, cold-path, or PostgreSQL advantage claim"],
            "address": config.address,
            "namespace": config.namespace,
            "preparation_seconds": preparation_seconds,
            "convergence_seconds": convergence_seconds,
            "warmup_seconds": warmup_seconds,
            "scheduled_window_seconds": config.duration.as_secs_f64(),
            "timed_loop_elapsed_seconds": finished.saturating_duration_since(started).as_secs_f64(),
            "stop_reason": stop_reason,
            "drain_seconds": finished.saturating_duration_since(started.checked_add(config.duration).ok_or("validated duration became unrepresentable")?).as_secs_f64(),
            "concurrency": config.concurrency,
            "fixture_keys": config.keys,
            "fixture_value_bytes": config.value_bytes,
            "fixture_total_value_bytes": config.keys * config.value_bytes,
            "fixture_preparation": "durable PUT handles awaited through the same public RockBeeClient before the timed window",
            "warmup": "every fixture key returned its exact expected bytes before timing; records may reside in reader WAL memtables",
            "producer_pattern": "one closed-loop bounded FuturesUnordered pipeline",
            "attempts": stats.attempts,
            "completed_attempts": stats.completed(),
            "get_before_deadline": stats.before_deadline,
            "get_after_deadline_drained": stats.late_drained,
            "missing": stats.missing,
            "mismatched": stats.mismatched,
            "transport_errors": stats.transport,
            "errors": stats.failures(),
            "error_messages": stats.errors,
            "get_rate_per_second": stats.before_deadline as f64 / config.duration.as_secs_f64(),
            "latency_us": stats.latency_us.summary(),
            "error_limit": MAX_ERRORS,
        }))?
    );
    if stats.failures() != 0 {
        return Err(format!("benchmark reported {} timed GET errors", stats.failures()).into());
    }
    Ok(())
}

struct TimedRun {
    stats: Stats,
    started: Instant,
    finished: Instant,
}

async fn timed_get(
    client: &RockBeeClient,
    fixtures: &[Arc<Fixture>],
    config: &Config,
) -> Result<TimedRun, Box<dyn std::error::Error + Send + Sync>> {
    eprintln!(
        "{}",
        json!({"event": "timed_warm_get_window_started", "namespace": config.namespace, "address": config.address})
    );
    let started = Instant::now();
    let deadline = started
        .checked_add(config.duration)
        .ok_or("validated duration became unrepresentable")?;
    let mut stats = Stats::new();
    let mut next = 0;
    let mut in_flight = FuturesUnordered::new();
    loop {
        while in_flight.len() < config.concurrency
            && Instant::now() < deadline
            && stats.failures() < MAX_ERRORS
        {
            let fixture = Arc::clone(&fixtures[next % fixtures.len()]);
            next += 1;
            stats.attempts += 1;
            in_flight.push(issue_get(client.clone(), fixture));
        }
        let Some(completion) = in_flight.next().await else {
            break;
        };
        stats.record(completion, deadline);
    }
    Ok(TimedRun {
        stats,
        started,
        finished: Instant::now(),
    })
}

async fn print_existing_fixture_measurement(
    client: &RockBeeClient,
    fixtures: &[Arc<Fixture>],
    config: &Config,
    preflight_seconds: f64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let TimedRun {
        stats,
        started,
        finished,
    } = timed_get(client, fixtures, config).await?;
    let stop_reason = if stats.failures() >= MAX_ERRORS {
        "error_limit"
    } else {
        "scheduled_deadline"
    };
    assert_eq!(stats.attempts, stats.completed());
    println!(
        "{}",
        serde_json::to_string(&json!({
            "benchmark": "rockbee-client-existing-fixture-warm-get",
            "workload": "warm_small_working_set",
            "claims": ["bounded closed-loop GETs over existing exact-byte fixtures", "no S3, cache-hit, cold-path, or PostgreSQL advantage claim"],
            "address": config.address,
            "namespace": config.namespace,
            "fixture_source": "existing_exact_verified",
            "preflight_seconds": preflight_seconds,
            "scheduled_window_seconds": config.duration.as_secs_f64(),
            "timed_loop_elapsed_seconds": finished.saturating_duration_since(started).as_secs_f64(),
            "stop_reason": stop_reason,
            "drain_seconds": finished.saturating_duration_since(started.checked_add(config.duration).ok_or("validated duration became unrepresentable")?).as_secs_f64(),
            "concurrency": config.concurrency,
            "fixture_keys": config.keys,
            "fixture_value_bytes": config.value_bytes,
            "fixture_total_value_bytes": config.keys * config.value_bytes,
            "fixture_preparation": "GET-only exact-byte verification before the timed window; no durable writes were prepared by this invocation",
            "warmup": "every existing fixture key returned its exact expected bytes before timing",
            "producer_pattern": "one closed-loop bounded FuturesUnordered pipeline",
            "attempts": stats.attempts,
            "completed_attempts": stats.completed(),
            "get_before_deadline": stats.before_deadline,
            "get_after_deadline_drained": stats.late_drained,
            "missing": stats.missing,
            "mismatched": stats.mismatched,
            "transport_errors": stats.transport,
            "errors": stats.failures(),
            "error_messages": stats.errors,
            "get_rate_per_second": stats.before_deadline as f64 / config.duration.as_secs_f64(),
            "latency_us": stats.latency_us.summary(),
            "error_limit": MAX_ERRORS,
        }))?
    );
    if stats.failures() != 0 {
        return Err(format!("benchmark reported {} timed GET errors", stats.failures()).into());
    }
    Ok(())
}

async fn prepare(
    client: &RockBeeClient,
    fixtures: &[Arc<Fixture>],
    concurrency: usize,
) -> rockbee_client::Result<()> {
    let mut next = 0;
    let mut in_flight = FuturesUnordered::new();
    while next < fixtures.len() || !in_flight.is_empty() {
        while next < fixtures.len() && in_flight.len() < concurrency {
            in_flight.push(put_fixture(client.clone(), Arc::clone(&fixtures[next])));
            next += 1;
        }
        in_flight
            .next()
            .await
            .expect("fixture pipeline is non-empty")?;
    }
    Ok(())
}

async fn put_fixture(client: RockBeeClient, fixture: Arc<Fixture>) -> rockbee_client::Result<()> {
    client
        .put(&fixture.key, &fixture.value, None)
        .await
        .map_err(Into::into)
}

async fn converge(
    client: &RockBeeClient,
    fixtures: &[Arc<Fixture>],
    concurrency: usize,
) -> rockbee_client::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err("fixtures did not converge within 60 seconds".into());
        }
        if tokio::time::timeout(remaining, matches_all(client, fixtures, concurrency)).await?? {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn verify_existing(
    client: &RockBeeClient,
    fixtures: &[Arc<Fixture>],
    concurrency: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let missing = tokio::time::timeout(
        Duration::from_secs(60),
        verify_exact_pass(client, fixtures, concurrency),
    )
    .await
    .map_err(|_| "fixture verification exceeded 60 seconds")??;
    if missing == 0 {
        Ok(())
    } else {
        Err(format!("{missing} fixture keys were missing").into())
    }
}

async fn verify_exact_pass(
    client: &RockBeeClient,
    fixtures: &[Arc<Fixture>],
    concurrency: usize,
) -> rockbee_client::Result<usize> {
    let mut next = 0;
    let mut missing = 0;
    let mut in_flight = FuturesUnordered::new();
    while next < fixtures.len() || !in_flight.is_empty() {
        while next < fixtures.len() && in_flight.len() < concurrency {
            in_flight.push(verify_fixture(client.clone(), Arc::clone(&fixtures[next])));
            next += 1;
        }
        if in_flight
            .next()
            .await
            .expect("verification pipeline is non-empty")?
        {
            missing += 1;
        }
    }
    Ok(missing)
}

async fn verify_fixture(
    client: RockBeeClient,
    fixture: Arc<Fixture>,
) -> rockbee_client::Result<bool> {
    match client.get(&fixture.key).await? {
        None => Ok(true),
        Some(value) if value == fixture.value => Ok(false),
        Some(_) => Err(format!(
            "fixture returned nonmatching bytes for key {:02x?}",
            fixture.key
        )
        .into()),
    }
}

async fn matches_all(
    client: &RockBeeClient,
    fixtures: &[Arc<Fixture>],
    concurrency: usize,
) -> rockbee_client::Result<bool> {
    let mut next = 0;
    let mut matched = true;
    let mut in_flight = FuturesUnordered::new();
    while next < fixtures.len() || !in_flight.is_empty() {
        while next < fixtures.len() && in_flight.len() < concurrency {
            in_flight.push(matches_fixture(client.clone(), Arc::clone(&fixtures[next])));
            next += 1;
        }
        matched &= in_flight
            .next()
            .await
            .expect("verification pipeline is non-empty")?;
    }
    Ok(matched)
}

async fn matches_fixture(
    client: RockBeeClient,
    fixture: Arc<Fixture>,
) -> rockbee_client::Result<bool> {
    Ok(client.get(&fixture.key).await?.as_deref() == Some(fixture.value.as_slice()))
}

async fn issue_get(client: RockBeeClient, fixture: Arc<Fixture>) -> GetCompletion {
    let started = Instant::now();
    let result = client.get(&fixture.key).await;
    let finished = Instant::now();
    GetCompletion {
        fixture,
        started,
        finished,
        result,
    }
}

impl Stats {
    fn new() -> Self {
        Self {
            attempts: 0,
            before_deadline: 0,
            late_drained: 0,
            missing: 0,
            mismatched: 0,
            transport: 0,
            latency_us: Reservoir::new(),
            errors: Vec::new(),
        }
    }
    fn failures(&self) -> u64 {
        self.missing + self.mismatched + self.transport
    }
    fn completed(&self) -> u64 {
        self.before_deadline + self.late_drained + self.failures()
    }
    fn record(&mut self, completion: GetCompletion, deadline: Instant) {
        match completion.result {
            Ok(None) => self.missing += 1,
            Ok(Some(value)) if value != completion.fixture.value => self.mismatched += 1,
            Ok(Some(_)) => {
                let latency = completion
                    .finished
                    .saturating_duration_since(completion.started)
                    .as_micros()
                    .try_into()
                    .unwrap_or(u64::MAX);
                self.latency_us.record(latency);
                if completion.finished < deadline {
                    self.before_deadline += 1
                } else {
                    self.late_drained += 1
                }
            }
            Err(error) => {
                self.transport += 1;
                if self.errors.len() < 32 {
                    self.errors.push(error.to_string())
                }
            }
        }
    }
}

impl Reservoir {
    fn new() -> Self {
        Self {
            seen: 0,
            state: 1,
            values: Vec::with_capacity(MAX_LATENCY_SAMPLES),
            max: None,
        }
    }
    fn record(&mut self, value: u64) {
        self.seen += 1;
        self.max = Some(self.max.map_or(value, |max| max.max(value)));
        if self.values.len() < MAX_LATENCY_SAMPLES {
            self.values.push(value);
            return;
        }
        self.state ^= self.state << 13;
        self.state ^= self.state >> 7;
        self.state ^= self.state << 17;
        let index = self.state % self.seen;
        if index < MAX_LATENCY_SAMPLES as u64 {
            self.values[index as usize] = value
        }
    }
    fn summary(&self) -> serde_json::Value {
        let mut values = self.values.clone();
        values.sort_unstable();
        json!({"successes": self.seen, "sample_count": values.len(), "sampling": "bounded reservoir", "p50": percentile(&values, 50), "p95": percentile(&values, 95), "p99": percentile(&values, 99), "max": self.max})
    }
}

fn percentile(values: &[u64], percentile: usize) -> Option<u64> {
    (!values.is_empty()).then(|| values[(values.len() - 1) * percentile / 100])
}

fn fixtures(namespace: &str, keys: usize, value_bytes: usize) -> Result<Vec<Arc<Fixture>>, String> {
    let mut result = Vec::with_capacity(keys);
    for index in 0..keys {
        let mut value = vec![b'x'; value_bytes];
        if value_bytes >= 8 {
            value[..8].copy_from_slice(&(index as u64).to_be_bytes())
        }
        result.push(Arc::new(Fixture {
            key: format!("{namespace}:{index}").into_bytes(),
            value,
        }));
    }
    Ok(result)
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Config, String> {
    let mut address = DEFAULT_ADDRESS.to_owned();
    let mut seconds = DEFAULT_SECONDS;
    let mut concurrency = DEFAULT_CONCURRENCY;
    let mut keys = DEFAULT_KEYS;
    let mut value_bytes = DEFAULT_VALUE_BYTES;
    let mut mode = Mode::Prepare;
    let mut namespace = None;
    let mut args = args.into_iter();
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--verify-only" => {
                if mode != Mode::Prepare {
                    return Err("--verify-only may be supplied once".into());
                }
                mode = Mode::VerifyOnly;
            }
            "--measure-existing" => {
                if mode != Mode::Prepare {
                    return Err("--measure-existing may be supplied once".into());
                }
                mode = Mode::MeasureExisting;
            }
            "--addr" | "--duration-seconds" | "--concurrency" | "--keys" | "--value-bytes"
            | "--namespace" => {
                let value = args.next().ok_or_else(|| usage().to_owned())?;
                match flag.as_str() {
                    "--addr" => address = value,
                    "--duration-seconds" => seconds = positive(&value, "duration")?,
                    "--concurrency" => concurrency = positive(&value, "concurrency")?,
                    "--keys" => keys = positive(&value, "keys")?,
                    "--value-bytes" => value_bytes = positive(&value, "value bytes")?,
                    "--namespace" => {
                        if namespace.replace(value).is_some() {
                            return Err("--namespace may be supplied once".into());
                        }
                    }
                    _ => unreachable!("matched known option"),
                }
            }
            _ => return Err(usage().to_owned()),
        }
    }
    if address.is_empty() || concurrency > DEFAULT_CONCURRENCY {
        return Err("address must be non-empty and concurrency must be 1..=128".into());
    }
    if keys > MAX_KEYS || value_bytes > MAX_VALUE_BYTES {
        return Err("keys or value bytes exceed benchmark limits".into());
    }
    let bytes = keys
        .checked_mul(value_bytes)
        .ok_or("fixture bytes overflow")?;
    if bytes > MAX_FIXTURE_BYTES {
        return Err(format!("fixture bytes must be at most {MAX_FIXTURE_BYTES}"));
    }
    let duration = Duration::from_secs(seconds);
    if Instant::now().checked_add(duration).is_none() {
        return Err("duration is not representable by Instant".into());
    }
    let namespace = match (mode, namespace) {
        (Mode::VerifyOnly | Mode::MeasureExisting, Some(namespace)) => namespace,
        (Mode::VerifyOnly, None) => {
            return Err("--verify-only requires --namespace <existing>".into());
        }
        (Mode::MeasureExisting, None) => {
            return Err("--measure-existing requires --namespace <existing>".into());
        }
        (Mode::Prepare, Some(_)) => {
            return Err(
                "--namespace is only valid with --verify-only or --measure-existing".into(),
            );
        }
        (Mode::Prepare, None) => unique_namespace(),
    };
    if namespace.is_empty() {
        return Err("namespace must be non-empty".into());
    }
    if namespace.len() > MAX_NAMESPACE_BYTES {
        return Err(format!(
            "namespace must be at most {MAX_NAMESPACE_BYTES} UTF-8 bytes"
        ));
    }
    Ok(Config {
        address,
        duration,
        concurrency,
        keys,
        value_bytes,
        namespace,
        mode,
    })
}

fn positive<T: std::str::FromStr + PartialOrd + From<u8>>(
    value: &str,
    name: &str,
) -> Result<T, String> {
    let parsed = value
        .parse()
        .map_err(|_| format!("invalid {name}: {value}"))?;
    (parsed > T::from(0))
        .then_some(parsed)
        .ok_or_else(|| format!("{name} must be positive"))
}

fn unique_namespace() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("rockbee-get-bench-{}-{nanos}", std::process::id())
}

fn usage() -> &'static str {
    "Usage: cargo run -p rockbee-client --example get_bench -- [--addr HOST:PORT] [--duration-seconds SECONDS] [--concurrency 1..128] [--keys COUNT] [--value-bytes BYTES] [--verify-only --namespace EXISTING | --measure-existing --namespace EXISTING]"
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockbee_protocol::transport::{FrameReader, read_frame, write_frame};
    use rockbee_protocol::{Request, RequestEnvelope, Response, ResponseEnvelope};
    use tokio::net::TcpListener;

    fn parse_error(args: impl IntoIterator<Item = String>) -> String {
        match parse_args(args) {
            Ok(_) => panic!("inputs must fail"),
            Err(error) => error,
        }
    }

    fn completion(
        fixture: Arc<Fixture>,
        finished: Instant,
        result: rockbee_client::Result<Option<Vec<u8>>>,
    ) -> GetCompletion {
        GetCompletion {
            fixture,
            started: finished,
            finished,
            result,
        }
    }

    #[test]
    fn rejects_fixture_inputs_that_exceed_the_bounded_preparation_budget() {
        let error = parse_error([
            "--keys".to_owned(),
            "65536".to_owned(),
            "--value-bytes".to_owned(),
            "65536".to_owned(),
        ]);
        assert!(error.contains("fixture bytes"));
    }

    #[test]
    fn rejects_a_duration_that_cannot_be_added_to_instant() {
        let error = parse_error(["--duration-seconds".to_owned(), u64::MAX.to_string()]);
        assert!(error.contains("duration"));
    }

    #[test]
    fn verify_only_requires_and_preserves_an_explicit_existing_namespace() {
        let config = parse_args([
            "--verify-only".to_owned(),
            "--namespace".to_owned(),
            "existing-fixture".to_owned(),
            "--keys".to_owned(),
            "2".to_owned(),
        ])
        .unwrap();
        assert_eq!(config.mode, Mode::VerifyOnly);
        assert_eq!(config.namespace, "existing-fixture");
        assert!(parse_args(["--verify-only".to_owned()]).is_err());
        assert!(parse_args(["--namespace".to_owned(), "existing".to_owned()]).is_err());
        assert!(
            parse_args([
                "--verify-only".to_owned(),
                "--verify-only".to_owned(),
                "--namespace".to_owned(),
                "existing".to_owned(),
            ])
            .is_err()
        );
        assert!(
            parse_args([
                "--verify-only".to_owned(),
                "--namespace".to_owned(),
                "".to_owned(),
            ])
            .is_err()
        );
        assert!(
            parse_args([
                "--verify-only".to_owned(),
                "--namespace".to_owned(),
                "x".repeat(MAX_NAMESPACE_BYTES + 1),
            ])
            .is_err()
        );
    }

    #[test]
    fn measure_existing_requires_and_preserves_an_explicit_existing_namespace() {
        let config = parse_args([
            "--measure-existing".to_owned(),
            "--namespace".to_owned(),
            "existing-fixture".to_owned(),
            "--keys".to_owned(),
            "2".to_owned(),
        ]);
        let config = config.expect("existing-fixture measurement should parse");
        assert_eq!(config.mode, Mode::MeasureExisting);
        assert_eq!(config.namespace, "existing-fixture");
        assert!(parse_args(["--measure-existing".to_owned()]).is_err());
        assert!(
            parse_args([
                "--measure-existing".to_owned(),
                "--verify-only".to_owned(),
                "--namespace".to_owned(),
                "existing".to_owned(),
            ])
            .is_err()
        );
    }

    async fn get_server(response: Response) -> (String, tokio::task::JoinHandle<Request>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (input, mut output) = stream.into_split();
            let mut input = FrameReader::new(input);
            let frame = read_frame(&mut input).await.unwrap().unwrap();
            let request = RequestEnvelope::decode(&frame).unwrap();
            write_frame(
                &mut output,
                &ResponseEnvelope {
                    request_id: request.request_id,
                    response,
                },
            )
            .await
            .unwrap();
            request.request
        });
        (address, task)
    }

    async fn eof_server() -> (String, tokio::task::JoinHandle<Request>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            // Consume the request before closing, so unread input cannot turn
            // the intended EOF into a connection reset on some platforms.
            let mut input = FrameReader::new(stream);
            let frame = read_frame(&mut input).await.unwrap().unwrap();
            RequestEnvelope::decode(&frame).unwrap().request
        });
        (address, task)
    }

    async fn exact_get_server(
        fixtures: Vec<Arc<Fixture>>,
    ) -> (String, tokio::task::JoinHandle<Vec<Request>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (input, mut output) = stream.into_split();
            let mut input = FrameReader::new(input);
            let mut requests = Vec::with_capacity(fixtures.len());
            for _ in 0..fixtures.len() {
                let frame = read_frame(&mut input).await.unwrap().unwrap();
                requests.push(RequestEnvelope::decode(&frame).unwrap());
            }
            let responses = requests
                .iter()
                .map(|request| match &request.request {
                    Request::Get { key } => Response::Value {
                        value: Some(
                            fixtures
                                .iter()
                                .find(|fixture| fixture.key == *key)
                                .expect("verification requested an expected fixture key")
                                .value
                                .clone(),
                        ),
                    },
                    request => panic!("verify-only sent non-GET request: {request:?}"),
                })
                .collect::<Vec<_>>();
            for (request, response) in requests.iter().zip(responses).rev() {
                write_frame(
                    &mut output,
                    &ResponseEnvelope {
                        request_id: request.request_id,
                        response,
                    },
                )
                .await
                .unwrap();
            }
            requests
                .into_iter()
                .map(|request| request.request)
                .collect()
        });
        (address, task)
    }

    async fn measuring_get_server(
        fixtures: Vec<Arc<Fixture>>,
    ) -> (String, tokio::task::JoinHandle<Vec<Request>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (input, mut output) = stream.into_split();
            let mut input = FrameReader::new(input);
            let mut requests = Vec::new();
            loop {
                let frame = read_frame(&mut input).await.unwrap();
                let Some(frame) = frame else { break };
                let request = RequestEnvelope::decode(&frame).unwrap();
                let Request::Get { key } = &request.request else {
                    panic!(
                        "measure-existing sent non-GET request: {:?}",
                        request.request
                    );
                };
                let fixture = fixtures
                    .iter()
                    .find(|fixture| fixture.key == *key)
                    .expect("measurement requested an expected fixture key");
                write_frame(
                    &mut output,
                    &ResponseEnvelope {
                        request_id: request.request_id,
                        response: Response::Value {
                            value: Some(fixture.value.clone()),
                        },
                    },
                )
                .await
                .unwrap();
                requests.push(request.request);
            }
            requests
        });
        (address, task)
    }

    #[tokio::test]
    async fn verify_only_issues_get_and_accepts_only_exact_fixture_bytes() {
        let fixture = fixtures("existing", 1, 8).unwrap();
        let (address, server) = get_server(Response::Value {
            value: Some(fixture[0].value.clone()),
        })
        .await;
        let client = RockBeeClient::connect(&address).await.unwrap();
        verify_existing(&client, &fixture, 1).await.unwrap();
        assert_eq!(
            server.await.unwrap(),
            Request::Get {
                key: fixture[0].key.clone()
            }
        );
    }

    #[tokio::test]
    async fn verify_only_checks_every_fixture_key_with_out_of_order_responses() {
        let fixtures = fixtures("existing", 3, 8).unwrap();
        let (address, server) = exact_get_server(fixtures.clone()).await;
        let client = RockBeeClient::connect(&address).await.unwrap();
        tokio::time::timeout(
            Duration::from_secs(1),
            verify_existing(&client, &fixtures, 3),
        )
        .await
        .expect("every fixture GET must be issued without a test-scale wait")
        .unwrap();
        let requests = server.await.unwrap();
        assert_eq!(requests.len(), fixtures.len());
        for fixture in fixtures {
            assert!(requests.contains(&Request::Get {
                key: fixture.key.clone()
            }));
        }
    }

    #[tokio::test]
    async fn verify_only_rejects_a_wrong_present_value() {
        let fixture = fixtures("existing", 1, 8).unwrap();
        let (address, server) = get_server(Response::Value {
            value: Some(vec![1; 8]),
        })
        .await;
        let client = RockBeeClient::connect(&address).await.unwrap();
        let error = verify_existing(&client, &fixture, 1).await.unwrap_err();
        assert!(error.to_string().contains("nonmatching bytes"));
        assert!(matches!(server.await.unwrap(), Request::Get { .. }));
    }

    #[tokio::test]
    async fn verify_only_rejects_a_missing_fixture() {
        let fixture = fixtures("existing", 1, 8).unwrap();
        let (address, server) = get_server(Response::Value { value: None }).await;
        let client = RockBeeClient::connect(&address).await.unwrap();
        let error = verify_existing(&client, &fixture, 1).await.unwrap_err();
        assert!(error.to_string().contains("1 fixture keys were missing"));
        assert!(matches!(server.await.unwrap(), Request::Get { .. }));
    }

    #[tokio::test]
    async fn measure_existing_stops_before_timing_on_a_nonexact_preflight() {
        let fixture = fixtures("existing", 1, 8).unwrap();
        let (address, server) = get_server(Response::Value {
            value: Some(vec![1; 8]),
        })
        .await;
        let config = Config {
            address,
            duration: Duration::from_secs(1),
            concurrency: 1,
            keys: 1,
            value_bytes: 8,
            namespace: "existing".to_owned(),
            mode: Mode::MeasureExisting,
        };
        let error = tokio::time::timeout(Duration::from_secs(1), run(config))
            .await
            .expect("nonexact preflight must fail without a timed window")
            .unwrap_err();
        assert!(error.to_string().contains("nonmatching bytes"));
        assert_eq!(
            server.await.unwrap(),
            Request::Get {
                key: fixture[0].key.clone()
            }
        );
    }

    #[tokio::test]
    async fn measure_existing_stops_before_timing_on_a_missing_preflight() {
        let fixture = fixtures("existing", 1, 8).unwrap();
        let (address, server) = get_server(Response::Value { value: None }).await;
        let config = Config {
            address,
            duration: Duration::from_secs(1),
            concurrency: 1,
            keys: 1,
            value_bytes: 8,
            namespace: "existing".to_owned(),
            mode: Mode::MeasureExisting,
        };
        let error = tokio::time::timeout(Duration::from_secs(1), run(config))
            .await
            .expect("missing preflight must fail without a timed window")
            .unwrap_err();
        assert!(error.to_string().contains("1 fixture keys were missing"));
        assert_eq!(
            server.await.unwrap(),
            Request::Get {
                key: fixture[0].key.clone()
            }
        );
    }

    #[tokio::test]
    async fn measure_existing_stops_before_timing_on_a_transport_preflight() {
        let (address, server) = eof_server().await;
        let config = Config {
            address,
            duration: Duration::from_secs(1),
            concurrency: 1,
            keys: 1,
            value_bytes: 8,
            namespace: "existing".to_owned(),
            mode: Mode::MeasureExisting,
        };
        let error = tokio::time::timeout(Duration::from_secs(1), run(config))
            .await
            .expect("transport preflight must fail without a timed window")
            .unwrap_err();
        assert!(
            error.to_string().contains("upstream EOF"),
            "preflight must preserve its transport error, not return a timed-loop error: {error}"
        );
        assert_eq!(
            server.await.unwrap(),
            Request::Get {
                key: b"existing:0".to_vec()
            }
        );
    }

    #[tokio::test]
    async fn measure_existing_entry_flow_uses_only_exact_gets() {
        let fixtures = fixtures("existing", 2, 8).unwrap();
        let (address, server) = measuring_get_server(fixtures.clone()).await;
        let config = Config {
            address,
            duration: Duration::from_millis(20),
            concurrency: 2,
            keys: fixtures.len(),
            value_bytes: 8,
            namespace: "existing".to_owned(),
            mode: Mode::MeasureExisting,
        };
        tokio::time::timeout(Duration::from_secs(2), run(config))
            .await
            .expect("measure-existing must finish within the test bound")
            .unwrap();
        let requests = tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("server must observe client close")
            .unwrap();
        assert!(requests.len() > fixtures.len());
        assert!(
            requests
                .iter()
                .all(|request| matches!(request, Request::Get { .. }))
        );
        for fixture in fixtures {
            assert!(requests.contains(&Request::Get {
                key: fixture.key.clone()
            }));
        }
    }

    #[test]
    fn fixture_values_embed_their_key_index_when_the_value_has_room() {
        let fixtures = fixtures("bench", 2, 8).unwrap();
        assert_ne!(fixtures[0].value, fixtures[1].value);
        assert_eq!(fixtures[1].value, 1_u64.to_be_bytes());
    }

    #[test]
    fn accounting_distinguishes_missing_mismatched_and_transport_results() {
        let deadline = Instant::now() + Duration::from_secs(1);
        let fixture = Arc::new(Fixture {
            key: b"k".to_vec(),
            value: vec![1],
        });
        let mut stats = Stats::new();
        let early = deadline - Duration::from_millis(1);
        stats.record(
            completion(Arc::clone(&fixture), early, Ok(Some(vec![1]))),
            deadline,
        );
        stats.record(
            completion(Arc::clone(&fixture), deadline, Ok(None)),
            deadline,
        );
        stats.record(
            completion(Arc::clone(&fixture), deadline, Ok(Some(vec![2]))),
            deadline,
        );
        stats.record(
            completion(fixture, deadline, Err("link retired".into())),
            deadline,
        );
        assert_eq!((stats.before_deadline, stats.late_drained), (1, 0));
        assert_eq!(
            (stats.missing, stats.mismatched, stats.transport),
            (1, 1, 1)
        );
        assert_eq!(stats.completed(), 4);
    }
}
