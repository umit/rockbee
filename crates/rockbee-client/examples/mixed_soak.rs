//! Bounded correctness soak for one public Client through an uncached gateway.
//!
//! It exercises one writer and the gateway's configured reader(s). Reader
//! observations are intentionally eventual: a mutable key may show an earlier
//! acknowledged version or `None`, but never another lane's bytes, a future
//! version, or malformed bytes. This is not a throughput benchmark.

use rockbee_client::Client;
use serde_json::json;
use std::env;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::task::JoinSet;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

const DEFAULT_ADDRESS: &str = "127.0.0.1:7100";
const DEFAULT_SECONDS: u64 = 60;
const DEFAULT_LANES: usize = 8;
const MAX_LANES: usize = 32;
const MAX_SECONDS: u64 = 86_400;
const VALUE_BYTES: usize = 1024;
const CONVERGENCE_BOUND: Duration = Duration::from_secs(60);
const OPERATION_BOUND: Duration = Duration::from_secs(6);
const POLL_INTERVAL: Duration = Duration::from_millis(200);
const PROGRESS_INTERVAL: Duration = Duration::from_secs(30);
const MAGIC: &[u8; 4] = b"MSK1";

struct Config {
    address: String,
    duration: Duration,
    lanes: usize,
    namespace: String,
}

#[derive(Default)]
struct Counts {
    seed_put_attempts: AtomicU64,
    seed_put_acks: AtomicU64,
    put_attempts: AtomicU64,
    put_acks: AtomicU64,
    delete_attempts: AtomicU64,
    delete_acks: AtomicU64,
    final_put_attempts: AtomicU64,
    final_put_acks: AtomicU64,
    completed_cycles: AtomicU64,
    mutable_present_current: AtomicU64,
    mutable_present_old: AtomicU64,
    mutable_missing: AtomicU64,
    sentinel_exact: AtomicU64,
    errors: AtomicU64,
}

struct LaneWork {
    lane: u32,
    namespace: String,
    mutable_key: Vec<u8>,
    sentinel_key: Vec<u8>,
    sentinel_value: Vec<u8>,
}

#[derive(Clone, Copy)]
enum Observation {
    Current,
    Old,
    Missing,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = parse_args(env::args().skip(1))?;
    eprintln!("mixed_soak namespace={}", config.namespace);
    if let Err(error) = run(config).await {
        eprintln!(
            "{}",
            json!({"event": "mixed_soak_failed", "error": error.to_string()})
        );
        return Err(error);
    }
    Ok(())
}

async fn run(config: Config) -> Result<()> {
    let client = Client::connect(&config.address).await?;
    let counts = Arc::new(Counts::default());
    let sentinel_key = key(&config.namespace, b"sentinel", 0);
    let sentinel_value = payload(&config.namespace, u32::MAX, 0)?;
    let mutable_keys: Vec<_> = (0..config.lanes)
        .map(|lane| key(&config.namespace, b"mutable", lane as u32))
        .collect();

    seed(
        &client,
        &sentinel_key,
        &sentinel_value,
        &mutable_keys,
        &config.namespace,
        &counts,
    )
    .await?;
    wait_initial(
        &client,
        &sentinel_key,
        &sentinel_value,
        &mutable_keys,
        &config.namespace,
    )
    .await?;

    eprintln!(
        "mixed_soak workload_started namespace={} lanes={}",
        config.namespace, config.lanes
    );
    let started = Instant::now();
    let deadline = started
        .checked_add(config.duration)
        .ok_or("validated soak duration became unrepresentable")?;
    let stop = Arc::new(AtomicBool::new(false));
    let mut lanes = JoinSet::new();
    for (lane, mutable_key) in mutable_keys.iter().cloned().enumerate() {
        lanes.spawn(lane_loop(
            client.clone(),
            LaneWork {
                lane: lane as u32,
                namespace: config.namespace.clone(),
                mutable_key,
                sentinel_key: sentinel_key.clone(),
                sentinel_value: sentinel_value.clone(),
            },
            deadline,
            Arc::clone(&counts),
            Arc::clone(&stop),
        ));
    }
    let mut ticker = tokio::time::interval(PROGRESS_INTERVAL);
    ticker.tick().await;
    let mut lane_last_versions = vec![0_u64; config.lanes];
    let mut error_messages = Vec::new();
    while !lanes.is_empty() {
        tokio::select! {
            completed = lanes.join_next() => {
                let Some(completed) = completed else { break; };
                match completed {
                    Ok(Ok((lane, last_version))) => lane_last_versions[lane] = last_version,
                    Ok(Err(error)) => {
                        record_error(&counts, &mut error_messages, error.to_string());
                        stop.store(true, Ordering::Release);
                    }
                    Err(error) => {
                        record_error(&counts, &mut error_messages, format!("lane task failed: {error}"));
                        stop.store(true, Ordering::Release);
                    }
                }
            }
            _ = ticker.tick() => progress(&config.namespace, started, &counts),
        }
    }
    let workload_finished = Instant::now();
    if counts.errors.load(Ordering::Acquire) != 0 {
        return Err(format!(
            "mixed soak recorded {} errors: {}",
            counts.errors.load(Ordering::Acquire),
            error_messages.join("; ")
        )
        .into());
    }

    for lane in 0..config.lanes {
        if lane % 2 == 0 {
            let next = lane_last_versions[lane]
                .checked_add(1)
                .ok_or("lane version overflow")?;
            counts.final_put_attempts.fetch_add(1, Ordering::Relaxed);
            put(
                &client,
                &mutable_keys[lane],
                &payload(&config.namespace, lane as u32, next)?,
            )
            .await?;
            counts.final_put_acks.fetch_add(1, Ordering::Relaxed);
            lane_last_versions[lane] = next;
        }
    }
    let final_started = Instant::now();
    wait_final(
        &client,
        &sentinel_key,
        &sentinel_value,
        &mutable_keys,
        &config.namespace,
        &lane_last_versions,
    )
    .await?;

    let expected: Vec<_> = (0..config.lanes)
        .map(|lane| {
            let value = if lane % 2 == 0 {
                Some(
                    payload(&config.namespace, lane as u32, lane_last_versions[lane])
                        .expect("validated payload"),
                )
            } else {
                None
            };
            json!({"lane": lane, "key_bytes": mutable_keys[lane], "value_bytes": value})
        })
        .collect();
    println!(
        "{}",
        serde_json::to_string(&json!({
            "audit": "rockbee-client-mixed-soak",
            "scope": "one retained public Client and clones through an uncached gateway; reader observations are eventual",
            "namespace": config.namespace,
            "address": config.address,
            "lanes": config.lanes,
            "value_bytes": VALUE_BYTES,
            "scheduled_seconds": config.duration.as_secs_f64(),
            "workload_elapsed_seconds": workload_finished.saturating_duration_since(started).as_secs_f64(),
            "drain_seconds": workload_finished.saturating_duration_since(deadline).as_secs_f64(),
            "final_convergence_seconds": final_started.elapsed().as_secs_f64(),
            "seed_put_attempts": counts.seed_put_attempts.load(Ordering::Relaxed),
            "seed_put_acks": counts.seed_put_acks.load(Ordering::Relaxed),
            "put_attempts": counts.put_attempts.load(Ordering::Relaxed),
            "put_acks": counts.put_acks.load(Ordering::Relaxed),
            "delete_attempts": counts.delete_attempts.load(Ordering::Relaxed),
            "delete_acks": counts.delete_acks.load(Ordering::Relaxed),
            "final_put_attempts": counts.final_put_attempts.load(Ordering::Relaxed),
            "final_put_acks": counts.final_put_acks.load(Ordering::Relaxed),
            "completed_cycles": counts.completed_cycles.load(Ordering::Relaxed),
            "mutable_present_current": counts.mutable_present_current.load(Ordering::Relaxed),
            "mutable_present_old": counts.mutable_present_old.load(Ordering::Relaxed),
            "mutable_missing": counts.mutable_missing.load(Ordering::Relaxed),
            "sentinel_exact": counts.sentinel_exact.load(Ordering::Relaxed),
            "errors": counts.errors.load(Ordering::Relaxed),
            "error_messages": error_messages,
            "mutable_stale_oracle": {
                "minimum_version": 0,
                "maximum_versions": lane_last_versions,
                "format": "MSK1 | namespace_len:u8 | namespace UTF-8 bytes | lane:u32le | version:u64le | deterministic fill byte[index]=(index ^ version ^ rotate_left(lane,11)) & 255",
            },
            "final_fixture": {
                "sentinel_key_bytes": sentinel_key,
                "sentinel_value_bytes": sentinel_value,
                "mutable": expected,
            },
            "claims": [
                "bounded correctness soak with exact byte oracles",
                "not a throughput, latency, cache-hit, or reader-freshness bound"
            ],
        }))?
    );
    Ok(())
}

async fn seed(
    client: &Client,
    sentinel_key: &[u8],
    sentinel_value: &[u8],
    mutable_keys: &[Vec<u8>],
    namespace: &str,
    counts: &Counts,
) -> Result<()> {
    counts.seed_put_attempts.fetch_add(1, Ordering::Relaxed);
    put(client, sentinel_key, sentinel_value).await?;
    counts.seed_put_acks.fetch_add(1, Ordering::Relaxed);
    for (lane, key) in mutable_keys.iter().enumerate() {
        counts.seed_put_attempts.fetch_add(1, Ordering::Relaxed);
        put(client, key, &payload(namespace, lane as u32, 0)?).await?;
        counts.seed_put_acks.fetch_add(1, Ordering::Relaxed);
    }
    Ok(())
}

async fn lane_loop(
    client: Client,
    work: LaneWork,
    deadline: Instant,
    counts: Arc<Counts>,
    stop: Arc<AtomicBool>,
) -> Result<(usize, u64)> {
    let mut last_version = 0_u64;
    while Instant::now() < deadline && !stop.load(Ordering::Acquire) {
        let next = last_version.checked_add(1).ok_or("lane version overflow")?;
        counts.put_attempts.fetch_add(1, Ordering::Relaxed);
        put(
            &client,
            &work.mutable_key,
            &payload(&work.namespace, work.lane, next)?,
        )
        .await?;
        counts.put_acks.fetch_add(1, Ordering::Relaxed);
        last_version = next;

        record_mutable(
            get(&client, &work.mutable_key).await?,
            &work.namespace,
            work.lane,
            last_version,
            &counts,
        )?;
        let sentinel = get(&client, &work.sentinel_key).await?;
        if sentinel.as_deref() != Some(work.sentinel_value.as_slice()) {
            return Err("immutable sentinel returned missing or wrong bytes".into());
        }
        counts.sentinel_exact.fetch_add(1, Ordering::Relaxed);

        counts.delete_attempts.fetch_add(1, Ordering::Relaxed);
        delete(&client, &work.mutable_key).await?;
        counts.delete_acks.fetch_add(1, Ordering::Relaxed);
        record_mutable(
            get(&client, &work.mutable_key).await?,
            &work.namespace,
            work.lane,
            last_version,
            &counts,
        )?;
        counts.completed_cycles.fetch_add(1, Ordering::Relaxed);
    }
    Ok((work.lane as usize, last_version))
}

async fn wait_initial(
    client: &Client,
    sentinel_key: &[u8],
    sentinel_value: &[u8],
    mutable_keys: &[Vec<u8>],
    namespace: &str,
) -> Result<()> {
    let deadline = Instant::now() + CONVERGENCE_BOUND;
    loop {
        let sentinel = convergence_get(client, sentinel_key, deadline, "initial").await?;
        if !initial_sentinel_ready(sentinel.as_deref(), sentinel_value)? {
            wait_or_timeout("initial", deadline).await?;
            continue;
        }
        let mut complete = true;
        for (lane, key) in mutable_keys.iter().enumerate() {
            let expected = payload(namespace, lane as u32, 0)?;
            let observed = convergence_get(client, key, deadline, "initial").await?;
            match observed.as_deref() {
                None => complete = false,
                Some(value) if value == expected.as_slice() => {}
                Some(value) => {
                    classify_mutable(Some(value), namespace, lane as u32, 0)?;
                    return Err(
                        format!("initial lane {lane} returned a non-version-zero value").into(),
                    );
                }
            }
        }
        if complete {
            return Ok(());
        }
        wait_or_timeout("initial", deadline).await?;
    }
}

fn initial_sentinel_ready(observed: Option<&[u8]>, expected: &[u8]) -> Result<bool> {
    match observed {
        None => Ok(false),
        Some(value) if value == expected => Ok(true),
        Some(_) => Err("initial sentinel returned wrong bytes".into()),
    }
}

async fn wait_final(
    client: &Client,
    sentinel_key: &[u8],
    sentinel_value: &[u8],
    mutable_keys: &[Vec<u8>],
    namespace: &str,
    versions: &[u64],
) -> Result<()> {
    let deadline = Instant::now() + CONVERGENCE_BOUND;
    loop {
        let sentinel = convergence_get(client, sentinel_key, deadline, "final").await?;
        if sentinel.as_deref() != Some(sentinel_value) {
            return Err("final sentinel returned missing or wrong bytes".into());
        }
        let mut complete = true;
        for (lane, key) in mutable_keys.iter().enumerate() {
            let expected = if lane % 2 == 0 {
                Some(payload(namespace, lane as u32, versions[lane])?)
            } else {
                None
            };
            let observed = convergence_get(client, key, deadline, "final").await?;
            if observed.as_deref() != expected.as_deref() {
                classify_mutable(observed.as_deref(), namespace, lane as u32, versions[lane])?;
                complete = false;
            }
        }
        if complete {
            return Ok(());
        }
        wait_or_timeout("final", deadline).await?;
    }
}

async fn wait_or_timeout(phase: &str, deadline: Instant) -> Result<()> {
    if Instant::now() >= deadline {
        return Err(format!("{phase} reader convergence exceeded sixty seconds").into());
    }
    tokio::time::sleep(POLL_INTERVAL).await;
    Ok(())
}

async fn convergence_get(
    client: &Client,
    key: &[u8],
    deadline: Instant,
    phase: &str,
) -> Result<Option<Vec<u8>>> {
    let budget = convergence_budget(deadline, Instant::now())?;
    let result = tokio::time::timeout(budget, client.get(key))
        .await
        .map_err(|_| format!("{phase} reader convergence GET exceeded its remaining deadline"))??;
    if Instant::now() > deadline {
        return Err(format!("{phase} reader convergence exceeded sixty seconds").into());
    }
    Ok(result)
}

fn convergence_budget(deadline: Instant, now: Instant) -> Result<Duration> {
    if now >= deadline {
        return Err("reader convergence exceeded sixty seconds".into());
    }
    Ok(deadline.duration_since(now).min(OPERATION_BOUND))
}

async fn put(client: &Client, key: &[u8], value: &[u8]) -> Result<()> {
    tokio::time::timeout(OPERATION_BOUND, client.put(key, value, None))
        .await
        .map_err(|_| "PUT exceeded six seconds")??;
    Ok(())
}

async fn delete(client: &Client, key: &[u8]) -> Result<()> {
    tokio::time::timeout(OPERATION_BOUND, client.delete(key))
        .await
        .map_err(|_| "DELETE exceeded six seconds")??;
    Ok(())
}

async fn get(client: &Client, key: &[u8]) -> Result<Option<Vec<u8>>> {
    tokio::time::timeout(OPERATION_BOUND, client.get(key))
        .await
        .map_err(|_| "GET exceeded six seconds")?
}

fn record_mutable(
    value: Option<Vec<u8>>,
    namespace: &str,
    lane: u32,
    last_version: u64,
    counts: &Counts,
) -> Result<()> {
    match classify_mutable(value.as_deref(), namespace, lane, last_version)? {
        Observation::Current => counts
            .mutable_present_current
            .fetch_add(1, Ordering::Relaxed),
        Observation::Old => counts.mutable_present_old.fetch_add(1, Ordering::Relaxed),
        Observation::Missing => counts.mutable_missing.fetch_add(1, Ordering::Relaxed),
    };
    Ok(())
}

fn classify_mutable(
    observed: Option<&[u8]>,
    namespace: &str,
    lane: u32,
    last_version: u64,
) -> Result<Observation> {
    let Some(observed) = observed else {
        return Ok(Observation::Missing);
    };
    let version = decode_version(observed, namespace, lane)?;
    if version > last_version {
        return Err(format!(
            "mutable lane {lane} returned future version {version} > {last_version}"
        )
        .into());
    }
    if version == last_version {
        Ok(Observation::Current)
    } else {
        Ok(Observation::Old)
    }
}

fn decode_version(value: &[u8], namespace: &str, lane: u32) -> Result<u64> {
    if value.len() != VALUE_BYTES || !value.starts_with(MAGIC) {
        return Err("mutable value was truncated or has an unknown format".into());
    }
    let namespace_bytes = namespace.as_bytes();
    let namespace_len = *value.get(4).ok_or("missing namespace length")? as usize;
    if namespace_len != namespace_bytes.len()
        || value.get(5..5 + namespace_len) != Some(namespace_bytes)
    {
        return Err("mutable value belongs to another namespace".into());
    }
    let lane_start = 5 + namespace_len;
    let lane_end = lane_start + 4;
    let version_end = lane_end + 8;
    let found_lane = u32::from_le_bytes(
        value
            .get(lane_start..lane_end)
            .ok_or("missing lane")?
            .try_into()?,
    );
    if found_lane != lane {
        return Err(format!("mutable value belongs to lane {found_lane}, expected {lane}").into());
    }
    let version = u64::from_le_bytes(
        value
            .get(lane_end..version_end)
            .ok_or("missing version")?
            .try_into()?,
    );
    if value != payload(namespace, lane, version)?.as_slice() {
        return Err("mutable value failed its exact-byte check".into());
    }
    Ok(version)
}

fn key(namespace: &str, kind: &[u8], lane: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(namespace.len() + kind.len() + 8);
    key.extend_from_slice(namespace.as_bytes());
    key.push(b'/');
    key.extend_from_slice(kind);
    key.push(b'/');
    key.extend_from_slice(&lane.to_le_bytes());
    key
}

fn payload(namespace: &str, lane: u32, version: u64) -> Result<Vec<u8>> {
    let namespace = namespace.as_bytes();
    if namespace.len() > u8::MAX as usize || 5 + namespace.len() + 12 > VALUE_BYTES {
        return Err("generated namespace does not fit the fixed payload header".into());
    }
    let mut value = Vec::with_capacity(VALUE_BYTES);
    value.extend_from_slice(MAGIC);
    value.push(namespace.len() as u8);
    value.extend_from_slice(namespace);
    value.extend_from_slice(&lane.to_le_bytes());
    value.extend_from_slice(&version.to_le_bytes());
    while value.len() < VALUE_BYTES {
        let index = value.len() as u64;
        value.push(((index ^ version ^ u64::from(lane).rotate_left(11)) & 0xff) as u8);
    }
    Ok(value)
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Config> {
    let mut address = DEFAULT_ADDRESS.to_owned();
    let mut seconds = DEFAULT_SECONDS;
    let mut lanes = DEFAULT_LANES;
    let mut args = args.into_iter();
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--addr" => address = next_option(&mut args)?,
            "--duration-seconds" => seconds = next_option(&mut args)?.parse()?,
            "--lanes" => lanes = next_option(&mut args)?.parse()?,
            _ => return Err("usage: mixed_soak [--addr HOST:PORT] [--duration-seconds 1..86400] [--lanes 1..32]".into()),
        }
    }
    validate_config(address, seconds, lanes)
}

fn next_option<I: Iterator<Item = String>>(args: &mut I) -> Result<String> {
    args.next().ok_or_else(|| "missing option value".into())
}

fn validate_config(address: String, seconds: u64, lanes: usize) -> Result<Config> {
    if address.is_empty()
        || !(1..=MAX_SECONDS).contains(&seconds)
        || !(1..=MAX_LANES).contains(&lanes)
    {
        return Err("invalid address, duration, or lane count".into());
    }
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    Ok(Config {
        address,
        duration: Duration::from_secs(seconds),
        lanes,
        namespace: format!("mixed-soak-{}-{nanos}", std::process::id()),
    })
}

fn record_error(counts: &Counts, messages: &mut Vec<String>, message: String) {
    counts.errors.fetch_add(1, Ordering::Relaxed);
    if messages.len() < 32 {
        messages.push(message);
    }
}

fn progress(namespace: &str, started: Instant, counts: &Counts) {
    eprintln!(
        "{}",
        json!({
            "event": "mixed_soak_progress",
            "namespace": namespace,
            "elapsed_seconds": started.elapsed().as_secs_f64(),
            "put_acks": counts.put_acks.load(Ordering::Relaxed),
            "put_attempts": counts.put_attempts.load(Ordering::Relaxed),
            "delete_acks": counts.delete_acks.load(Ordering::Relaxed),
            "delete_attempts": counts.delete_attempts.load(Ordering::Relaxed),
            "completed_cycles": counts.completed_cycles.load(Ordering::Relaxed),
            "mutable_present_current": counts.mutable_present_current.load(Ordering::Relaxed),
            "mutable_present_old": counts.mutable_present_old.load(Ordering::Relaxed),
            "mutable_missing": counts.mutable_missing.load(Ordering::Relaxed),
            "sentinel_exact": counts.sentinel_exact.load(Ordering::Relaxed),
            "errors": counts.errors.load(Ordering::Relaxed),
        })
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mutable_oracle_accepts_old_and_missing_bytes() {
        let namespace = "unit";
        assert!(matches!(
            classify_mutable(None, namespace, 2, 4).unwrap(),
            Observation::Missing
        ));
        assert!(matches!(
            classify_mutable(Some(&payload(namespace, 2, 3).unwrap()), namespace, 2, 4).unwrap(),
            Observation::Old
        ));
        assert!(matches!(
            classify_mutable(Some(&payload(namespace, 2, 4).unwrap()), namespace, 2, 4).unwrap(),
            Observation::Current
        ));
    }

    #[test]
    fn mutable_oracle_rejects_cross_lane_future_wrong_namespace_truncated_and_bitflipped_values() {
        let value = payload("unit", 2, 4).unwrap();
        assert!(classify_mutable(Some(&payload("unit", 3, 4).unwrap()), "unit", 2, 4).is_err());
        assert!(classify_mutable(Some(&payload("other", 2, 4).unwrap()), "unit", 2, 4).is_err());
        assert!(classify_mutable(Some(&payload("unit", 2, 5).unwrap()), "unit", 2, 4).is_err());
        assert!(classify_mutable(Some(&value[..VALUE_BYTES - 1]), "unit", 2, 4).is_err());
        let mut flipped = value;
        flipped[VALUE_BYTES - 1] ^= 1;
        assert!(classify_mutable(Some(&flipped), "unit", 2, 4).is_err());
    }

    #[test]
    fn config_bounds_are_checked() {
        assert!(validate_config("127.0.0.1:7100".into(), 1, 1).is_ok());
        assert!(validate_config("x".into(), 0, 1).is_err());
        assert!(validate_config("x".into(), MAX_SECONDS + 1, 1).is_err());
        assert!(validate_config("x".into(), 1, MAX_LANES + 1).is_err());
    }

    #[test]
    fn convergence_budget_rejects_an_exhausted_deadline() {
        let now = Instant::now();
        assert!(convergence_budget(now, now).is_err());
    }

    #[test]
    fn initial_sentinel_allows_only_missing_or_exact_bytes() {
        assert!(!initial_sentinel_ready(None, b"sentinel").unwrap());
        assert!(initial_sentinel_ready(Some(b"sentinel"), b"sentinel").unwrap());
        assert!(initial_sentinel_ready(Some(b"wrong"), b"sentinel").is_err());
    }
}
