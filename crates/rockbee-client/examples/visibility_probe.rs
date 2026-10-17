//! Low-load ACK-to-reader visibility measurement through the public RockBeeClient.
//! Run against already-started binaries; this program does not manage services.
//!
//! With no `--reader` arguments, GETs go through the gateway and measure its
//! round-robin reader path. Supplying `--reader HOST:PORT` measures every named
//! reader directly after each mutation is acknowledged by the gateway.
use rockbee_client::{Result, RockBeeClient};
use serde_json::{Value, json};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep, timeout_at};

const POLL: Duration = Duration::from_millis(20);
const SPACING: Duration = Duration::from_millis(613);
const LIMIT: Duration = Duration::from_secs(30);
const USAGE: &str = "Usage: visibility_probe GATEWAY_HOST:PORT [SAMPLES_PER_OPERATION=20] [--reader READER_HOST:PORT]...";

#[derive(Debug)]
struct Config {
    gateway: String,
    samples: usize,
    readers: Vec<String>,
}

#[derive(Clone, Copy)]
enum TargetKind {
    Gateway,
    Reader,
}

impl TargetKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Gateway => "gateway_round_robin",
            Self::Reader => "reader",
        }
    }
}

#[derive(Clone)]
struct Target {
    endpoint: String,
    kind: TargetKind,
    client: RockBeeClient,
}

fn parse_config(args: impl IntoIterator<Item = String>) -> Result<Config> {
    let mut args = args.into_iter();
    let gateway = args.next().ok_or(USAGE)?;
    let mut samples = 20;
    let mut has_samples = false;
    let mut readers = Vec::new();

    while let Some(argument) = args.next() {
        if argument == "--reader" {
            let reader = args.next().ok_or("--reader requires HOST:PORT")?;
            if reader.starts_with('-') {
                return Err("--reader requires HOST:PORT".into());
            }
            readers.push(reader);
        } else if !has_samples {
            samples = argument.parse()?;
            has_samples = true;
        } else {
            return Err(format!("unexpected argument `{argument}`; {USAGE}").into());
        }
    }

    if !(1..=64).contains(&samples) {
        return Err("samples must be in 1..=64".into());
    }
    Ok(Config {
        gateway,
        samples,
        readers,
    })
}

async fn connect_targets(config: &Config, gateway: &RockBeeClient) -> (Vec<Target>, Vec<Value>) {
    if config.readers.is_empty() {
        return (
            vec![Target {
                endpoint: config.gateway.clone(),
                kind: TargetKind::Gateway,
                client: gateway.clone(),
            }],
            Vec::new(),
        );
    }

    let mut targets = Vec::with_capacity(config.readers.len());
    let mut failures = Vec::new();
    for endpoint in &config.readers {
        match RockBeeClient::connect(endpoint).await {
            Ok(client) => targets.push(Target {
                endpoint: endpoint.clone(),
                kind: TargetKind::Reader,
                client,
            }),
            Err(error) => failures.push(json!({
                "endpoint": endpoint,
                "target": TargetKind::Reader.as_str(),
                "status": "failed",
                "error": error.to_string(),
            })),
        }
    }
    (targets, failures)
}

fn failure(target: &Target, error: impl ToString, attempts: u64, ack: Instant) -> Value {
    json!({
        "endpoint": target.endpoint,
        "target": target.kind.as_str(),
        "status": "failed",
        "error": error.to_string(),
        "get_attempts": attempts,
        "observed_after_ack_ms": ack.elapsed().as_secs_f64() * 1000.0,
    })
}

async fn observe_target(
    target: Target,
    key: Vec<u8>,
    expected: Option<Vec<u8>>,
    ack: Instant,
) -> Value {
    let deadline = ack + LIMIT;
    let mut attempts = 0_u64;
    loop {
        match timeout_at(deadline, target.client.get(&key)).await {
            Ok(Ok(actual)) if actual == expected => {
                return json!({
                    "endpoint": target.endpoint,
                    "target": target.kind.as_str(),
                    "status": "ok",
                    "get_attempts": attempts + 1,
                    "first_match_after_ack_ms": ack.elapsed().as_secs_f64() * 1000.0,
                });
            }
            Ok(Ok(_)) => attempts += 1,
            Ok(Err(error)) => return failure(&target, error, attempts + 1, ack),
            Err(_) => {
                return failure(
                    &target,
                    "reader did not converge before deadline",
                    attempts,
                    ack,
                );
            }
        }
        sleep(POLL).await;
    }
}

async fn observe_targets(
    targets: &[Target],
    key: &[u8],
    expected: Option<Vec<u8>>,
    ack: Instant,
) -> Vec<Value> {
    let mut pending = JoinSet::new();
    for target in targets {
        pending.spawn(observe_target(
            target.clone(),
            key.to_vec(),
            expected.clone(),
            ack,
        ));
    }

    let mut observations = Vec::with_capacity(targets.len());
    while let Some(result) = pending.join_next().await {
        match result {
            Ok(observation) => observations.push(observation),
            Err(error) => observations.push(json!({
                "endpoint": "unknown",
                "target": "unknown",
                "status": "failed",
                "error": format!("observation task failed: {error}"),
            })),
        }
    }
    observations
}

fn observations_succeeded(observations: &[Value]) -> bool {
    !observations.is_empty()
        && observations
            .iter()
            .all(|observation| observation["status"] == "ok")
}

async fn mutate_and_observe(
    gateway: &RockBeeClient,
    targets: &[Target],
    key: &[u8],
    value: &[u8],
    delete: bool,
) -> Value {
    let started = Instant::now();
    let mutation = if delete {
        gateway.delete(key).await.map_err(|error| error.to_string())
    } else {
        gateway
            .put(key, value, None)
            .await
            .map_err(|error| error.to_string())
    };
    let operation = if delete { "delete" } else { "put" };
    if let Err(error) = mutation {
        return json!({
            "operation": operation,
            "key": String::from_utf8_lossy(key),
            "status": "failed",
            "error": error,
            "readers": [],
        });
    }

    let ack = Instant::now();
    let expected = if delete { None } else { Some(value.to_vec()) };
    let readers = observe_targets(targets, key, expected, ack).await;
    let status = if observations_succeeded(&readers) {
        "ok"
    } else {
        "failed"
    };
    json!({
        "operation": operation,
        "key": String::from_utf8_lossy(key),
        "status": status,
        "durable_ack_ms": (ack - started).as_secs_f64() * 1000.0,
        "readers": readers,
    })
}

fn task_failure(operation: &str, key: &[u8], error: tokio::task::JoinError) -> Value {
    json!({
        "operation": operation,
        "key": String::from_utf8_lossy(key),
        "status": "failed",
        "error": format!("{operation} task failed: {error}"),
        "readers": [],
    })
}

fn complete_operations(
    operation: &str,
    fixtures: &[(Vec<u8>, Vec<u8>)],
    operations: Vec<Option<Value>>,
) -> Vec<Value> {
    operations
        .into_iter()
        .enumerate()
        .map(|(index, operation_result)| {
            operation_result.unwrap_or_else(|| {
                json!({
                    "operation": operation,
                    "key": String::from_utf8_lossy(&fixtures[index].0),
                    "status": "failed",
                    "error": format!("{operation} task did not return a result"),
                    "readers": [],
                })
            })
        })
        .collect()
}

async fn wave(
    gateway: &RockBeeClient,
    targets: &[Target],
    fixtures: &[(Vec<u8>, Vec<u8>)],
) -> (Vec<Value>, Vec<Value>) {
    let mut pending_puts = Vec::with_capacity(fixtures.len());
    for (index, (key, value)) in fixtures.iter().enumerate() {
        let gateway = gateway.clone();
        let targets = targets.to_vec();
        let key = key.clone();
        let value = value.clone();
        pending_puts.push((
            index,
            tokio::spawn(async move {
                mutate_and_observe(&gateway, &targets, &key, &value, false).await
            }),
        ));
        sleep(SPACING).await;
    }

    // Complete the whole staggered PUT wave first. This keeps lagging readers
    // from serialising the sample while ensuring a DELETE never races ahead of
    // visibility of its own PUT on a required target.
    let mut puts: Vec<Option<Value>> = (0..fixtures.len()).map(|_| None).collect();
    for (index, pending) in pending_puts {
        puts[index] = Some(match pending.await {
            Ok(put) => put,
            Err(error) => task_failure("put", &fixtures[index].0, error),
        });
    }
    let puts = complete_operations("put", fixtures, puts);

    let mut deletes: Vec<Option<Value>> = (0..fixtures.len()).map(|_| None).collect();
    let mut pending_deletes = Vec::with_capacity(fixtures.len());
    for (index, ((key, value), put)) in fixtures.iter().zip(&puts).enumerate() {
        if put["status"] == "ok" {
            let gateway = gateway.clone();
            let targets = targets.to_vec();
            let key = key.clone();
            let value = value.clone();
            pending_deletes.push((
                index,
                tokio::spawn(async move {
                    mutate_and_observe(&gateway, &targets, &key, &value, true).await
                }),
            ));
            sleep(SPACING).await;
        } else {
            deletes[index] = Some(json!({
                "operation": "delete",
                "key": String::from_utf8_lossy(key),
                "status": "skipped",
                "reason": "PUT was not observed by every required target",
                "readers": [],
            }));
        }
    }
    for (index, pending) in pending_deletes {
        deletes[index] = Some(match pending.await {
            Ok(delete) => delete,
            Err(error) => task_failure("delete", &fixtures[index].0, error),
        });
    }
    let deletes = complete_operations("delete", fixtures, deletes);
    (puts, deletes)
}

async fn shutdown_all(gateway: &RockBeeClient, targets: &[Target]) {
    let mut pending = JoinSet::new();
    pending.spawn({
        let gateway = gateway.clone();
        async move { gateway.shutdown().await }
    });
    for target in targets {
        // In gateway-only mode this intentionally calls shutdown on a clone of
        // the same RockBeeClient. RockBeeClient::shutdown is idempotent and concurrent-safe.
        let client = target.client.clone();
        pending.spawn(async move { client.shutdown().await });
    }
    while pending.join_next().await.is_some() {}
}

fn all_operations_succeeded(puts: &[Value], deletes: &[Value]) -> bool {
    puts.iter()
        .chain(deletes)
        .all(|operation| operation["status"] == "ok")
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = parse_config(std::env::args().skip(1).collect::<Vec<_>>())?;
    let gateway = RockBeeClient::connect(&config.gateway).await?;
    let started = Instant::now();
    let (targets, setup_failures) = connect_targets(&config, &gateway).await;
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let fixtures: Vec<_> = (0..config.samples)
        .map(|i| {
            (
                format!("visibility-{}-{nonce}-{i}", std::process::id()).into_bytes(),
                format!("value-{i}\0binary\u{00ff}").into_bytes(),
            )
        })
        .collect();

    let (puts, deletes) = if setup_failures.is_empty() {
        wave(&gateway, &targets, &fixtures).await
    } else {
        (Vec::new(), Vec::new())
    };
    shutdown_all(&gateway, &targets).await;

    let succeeded = setup_failures.is_empty() && all_operations_succeeded(&puts, &deletes);
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "status": if succeeded { "ok" } else { "failed" },
            "gateway": config.gateway,
            "targets": targets.iter().map(|target| json!({
                "endpoint": target.endpoint,
                "target": target.kind.as_str(),
            })).collect::<Vec<_>>(),
            "setup_failures": setup_failures,
            "samples_per_operation": config.samples,
            "poll_interval_ms": POLL.as_millis(),
            "mutation_spacing_ms": SPACING.as_millis(),
            "convergence_timeout_ms": LIMIT.as_millis(),
            "elapsed_seconds": started.elapsed().as_secs_f64(),
            "note": "Low-load staggered sample, not a throughput benchmark. First observed match includes GET RTT and polling delay; no strict staleness bound is claimed. Without --reader, the gateway round-robin path is measured and does not establish convergence on every reader.",
            "puts": puts,
            "deletes": deletes,
        }))?
    );
    if succeeded {
        Ok(())
    } else {
        Err("one or more required targets did not observe every mutation".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockbee_protocol::transport::{FrameReader, read_frame, write_frame};
    use rockbee_protocol::{Request, RequestEnvelope, Response, ResponseEnvelope};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use tokio::net::TcpListener;

    async fn bind_lagging_reader(
        value: Vec<u8>,
        misses_before_visible: usize,
        deleted: Arc<AtomicBool>,
        put_observed: Arc<AtomicBool>,
    ) -> Result<(String, tokio::task::JoinHandle<Result<()>>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?.to_string();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let (input, mut output) = stream.into_split();
            let mut input = FrameReader::new(input);
            let attempts = AtomicUsize::new(0);
            while let Some(frame) = read_frame(&mut input).await? {
                let request = RequestEnvelope::decode(&frame)?;
                let response = match request.request {
                    Request::Get { .. } if deleted.load(Ordering::SeqCst) => {
                        Response::Value { value: None }
                    }
                    Request::Get { .. } => {
                        let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                        if attempt <= misses_before_visible {
                            Response::Value { value: None }
                        } else {
                            put_observed.store(true, Ordering::SeqCst);
                            Response::Value {
                                value: Some(value.clone()),
                            }
                        }
                    }
                    _ => return Err("reader mock received a non-GET request".into()),
                };
                write_frame(
                    &mut output,
                    &ResponseEnvelope {
                        request_id: request.request_id,
                        response,
                    },
                )
                .await?;
            }
            Ok(())
        });
        Ok((address, server))
    }

    async fn bind_gateway(
        deleted: Arc<AtomicBool>,
        put_observed: Arc<AtomicBool>,
        delete_before_put_observed: Arc<AtomicBool>,
    ) -> Result<(String, tokio::task::JoinHandle<Result<()>>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?.to_string();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let (input, mut output) = stream.into_split();
            let mut input = FrameReader::new(input);
            while let Some(frame) = read_frame(&mut input).await? {
                let request = RequestEnvelope::decode(&frame)?;
                match request.request {
                    Request::Put { .. } | Request::PutWithTtl { .. } => {}
                    Request::Delete { .. } => {
                        if !put_observed.load(Ordering::SeqCst) {
                            delete_before_put_observed.store(true, Ordering::SeqCst);
                        }
                        deleted.store(true, Ordering::SeqCst);
                    }
                    _ => return Err("gateway mock received a non-mutation request".into()),
                }
                write_frame(
                    &mut output,
                    &ResponseEnvelope {
                        request_id: request.request_id,
                        response: Response::Ok,
                    },
                )
                .await?;
            }
            Ok(())
        });
        Ok((address, server))
    }

    #[test]
    fn accepts_existing_positional_invocation() {
        let config = parse_config(["gateway:7100".into(), "3".into()]).unwrap();
        assert_eq!(config.gateway, "gateway:7100");
        assert_eq!(config.samples, 3);
        assert!(config.readers.is_empty());
    }

    #[test]
    fn accepts_repeated_direct_readers() {
        let config = parse_config([
            "gateway:7100".into(),
            "2".into(),
            "--reader".into(),
            "reader-a:7102".into(),
            "--reader".into(),
            "reader-b:7103".into(),
        ])
        .unwrap();
        assert_eq!(config.samples, 2);
        assert_eq!(config.readers, ["reader-a:7102", "reader-b:7103"]);
    }

    #[test]
    fn failed_or_skipped_operations_fail_the_run() {
        let put = json!({"status": "ok"});
        let delete = json!({"status": "skipped"});
        assert!(!all_operations_succeeded(&[put], &[delete]));
    }

    #[tokio::test]
    async fn waits_for_a_lagging_reader_before_reporting_visibility() -> Result<()> {
        let deleted = Arc::new(AtomicBool::new(false));
        let observed = Arc::new(AtomicBool::new(false));
        let (address, server) = bind_lagging_reader(
            b"value".to_vec(),
            2,
            Arc::clone(&deleted),
            Arc::clone(&observed),
        )
        .await?;
        let client = RockBeeClient::connect(&address).await?;
        let target = Target {
            endpoint: address,
            kind: TargetKind::Reader,
            client: client.clone(),
        };
        let acknowledged = Instant::now();
        let observation = observe_target(
            target,
            b"key".to_vec(),
            Some(b"value".to_vec()),
            acknowledged,
        )
        .await;

        assert_eq!(observation["status"], "ok");
        assert_eq!(observation["get_attempts"], 3);
        assert!(
            observation["first_match_after_ack_ms"]
                .as_f64()
                .unwrap_or(0.0)
                >= 2.0 * POLL.as_secs_f64() * 1000.0
        );
        assert!(observed.load(Ordering::SeqCst));
        client.shutdown().await;
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn wave_never_deletes_before_the_lagging_reader_saw_the_put() -> Result<()> {
        let deleted = Arc::new(AtomicBool::new(false));
        let put_observed = Arc::new(AtomicBool::new(false));
        let delete_before_put_observed = Arc::new(AtomicBool::new(false));
        let (reader_address, reader_server) = bind_lagging_reader(
            b"value".to_vec(),
            2,
            Arc::clone(&deleted),
            Arc::clone(&put_observed),
        )
        .await?;
        let (gateway_address, gateway_server) = bind_gateway(
            Arc::clone(&deleted),
            Arc::clone(&put_observed),
            Arc::clone(&delete_before_put_observed),
        )
        .await?;
        let gateway = RockBeeClient::connect(&gateway_address).await?;
        let reader = RockBeeClient::connect(&reader_address).await?;
        let targets = vec![Target {
            endpoint: reader_address,
            kind: TargetKind::Reader,
            client: reader,
        }];

        let (puts, deletes) =
            wave(&gateway, &targets, &[(b"key".to_vec(), b"value".to_vec())]).await;

        assert_eq!(puts[0]["status"], "ok");
        assert_eq!(deletes[0]["status"], "ok");
        assert!(put_observed.load(Ordering::SeqCst));
        assert!(!delete_before_put_observed.load(Ordering::SeqCst));
        shutdown_all(&gateway, &targets).await;
        gateway_server.await??;
        reader_server.await??;
        Ok(())
    }
}
