//! Opt-in warm closed-loop correctness soak. This is not a capacity benchmark.
//!
//! Workers share one public connection by default, or use an explicit number of
//! independent connections to one gateway. They own disjoint keys and never retry
//! failed mutations or reads. The owner joins workers before closing resources.

mod diagnostics;
mod nginx;

use std::{
    fs::{self, File},
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use hdrhistogram::Histogram;
use rockbee_raft_client::{ClientConfig, ReadError, RockBeeRaftClient};
use rockbee_raft_gateway::{GatewayConfig, GatewayRuntime, SeedNode};
use rockbee_raft_protocol::{
    MutationOutcome, ShardConfig,
    transport::{AdmitError, LinkConfig, server::ServerConfig},
};
use tokio::{
    net::TcpListener,
    signal::unix::{SignalKind, signal},
    sync::{Barrier, watch},
    task::JoinSet,
    time::{Instant, sleep, sleep_until, timeout},
};

use super::ProcessCluster;

const PAYLOAD_BYTES: usize = 1024;
const GROUPS: usize = 4;
const MAX_WORKERS: u64 = 100;
const DRAIN_SECONDS: u64 = 35;
const MAX_LATENCY_MICROS: u64 = 60_000_000;
const OP_NAMES: [&str; 6] = [
    "put",
    "get_after_put",
    "update",
    "get_after_update",
    "delete",
    "get_absence",
];
const OUTCOME_NAMES: [&str; 8] = [
    "success",
    "local_admission",
    "not_sent",
    "rejected",
    "unknown",
    "read_transport",
    "read_remote",
    "wrong_value",
];

#[derive(Clone, Copy)]
struct SoakConfig {
    seconds: u64,
    concurrency: usize,
    clients: usize,
    gateway_count: usize,
    seed: u64,
    gateway_server: ServerConfig,
    node_budget_mib: u64,
}

impl SoakConfig {
    fn from_environment() -> Result<Self, String> {
        let seconds = bounded_setting(env_setting("ROCKBEE_SOAK_SECONDS")?.as_deref(), 60, 3600)?;
        let concurrency = bounded_setting(
            env_setting("ROCKBEE_SOAK_CONCURRENCY")?.as_deref(),
            1,
            MAX_WORKERS,
        )?;
        let clients =
            client_connections(env_setting("ROCKBEE_SOAK_CLIENTS")?.as_deref(), concurrency)?;
        let response_mib = bounded_setting(
            env_setting("ROCKBEE_SOAK_GATEWAY_RESPONSE_MIB")?.as_deref(),
            64,
            1024,
        )?;
        let gateway_server = ServerConfig {
            max_connections: clients.max(ServerConfig::default().max_connections),
            response_bytes: usize::try_from(response_mib * 1024 * 1024)
                .map_err(|error| error.to_string())?,
            ..ServerConfig::default()
        };
        gateway_server
            .validate()
            .map_err(|error| error.to_string())?;
        let node_budget_mib = bounded_setting(
            env_setting("ROCKBEE_SOAK_NODE_BUDGET_MIB")?.as_deref(),
            64,
            1024,
        )?;
        let node_budget_bytes =
            usize::try_from(node_budget_mib * 1024 * 1024).map_err(|error| error.to_string())?;
        ServerConfig {
            request_bytes: node_budget_bytes,
            response_bytes: node_budget_bytes,
            ..ServerConfig::default()
        }
        .validate()
        .map_err(|error| error.to_string())?;
        let seed = env_setting("ROCKBEE_SOAK_SEED")?.map_or(Ok(20261019), |value| {
            value.parse::<u64>().map_err(|error| error.to_string())
        })?;
        Ok(Self {
            seconds,
            concurrency: usize::try_from(concurrency).unwrap(),
            clients,
            gateway_count: 1,
            seed,
            gateway_server,
            node_budget_mib,
        })
    }
}

fn client_connections(value: Option<&str>, workers: u64) -> Result<usize, String> {
    let clients = bounded_setting(value, 1, MAX_WORKERS)?;
    if clients > workers {
        return Err("client connections cannot exceed worker concurrency".to_owned());
    }
    Ok(usize::try_from(clients).unwrap())
}

fn env_setting(name: &str) -> Result<Option<String>, String> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(format!("{name}: {error}")),
    }
}

fn bounded_setting(value: Option<&str>, default: u64, maximum: u64) -> Result<u64, String> {
    let parsed = value.map_or(Ok(default), |value| {
        value.parse::<u64>().map_err(|error| error.to_string())
    })?;
    if !(1..=maximum).contains(&parsed) {
        return Err(format!("setting must be in 1..={maximum}, got {parsed}"));
    }
    Ok(parsed)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StopReason {
    Running,
    Duration,
    Signal,
    Validation,
}

fn request_stop(stop: &watch::Sender<StopReason>, reason: StopReason) {
    stop.send_if_modified(|current| {
        if *current == StopReason::Running {
            *current = reason;
            true
        } else {
            false
        }
    });
}

#[derive(Clone, Copy, Debug)]
#[repr(usize)]
enum Outcome {
    Success,
    LocalAdmission,
    NotSent,
    Rejected,
    Unknown,
    ReadTransport,
    ReadRemote,
    WrongValue,
}

#[derive(Debug)]
struct FailedOperation {
    outcome: Outcome,
    detail: String,
}

type OperationResult = Result<(), FailedOperation>;

fn committed(result: Result<MutationOutcome, AdmitError>) -> OperationResult {
    match result {
        Ok(MutationOutcome::Committed) => Ok(()),
        Err(error) => Err(FailedOperation {
            outcome: Outcome::LocalAdmission,
            detail: error.to_string(),
        }),
        Ok(other) => {
            let outcome = match &other {
                MutationOutcome::NotSent(_) => Outcome::NotSent,
                MutationOutcome::Rejected(_) => Outcome::Rejected,
                MutationOutcome::Unknown(_) => Outcome::Unknown,
                MutationOutcome::Committed => unreachable!(),
            };
            Err(FailedOperation {
                outcome,
                detail: format!("{other:?}"),
            })
        }
    }
}

fn exact_value(
    result: Result<Option<Bytes>, ReadError>,
    expected: Option<&Bytes>,
) -> OperationResult {
    match result {
        Ok(value) if value.as_ref() == expected => Ok(()),
        Ok(value) => Err(FailedOperation {
            outcome: Outcome::WrongValue,
            detail: format!(
                "expected {} bytes, received {} bytes (None means absent)",
                expected.map_or_else(|| "None".to_owned(), |value| value.len().to_string()),
                value
                    .as_ref()
                    .map_or_else(|| "None".to_owned(), |value| value.len().to_string())
            ),
        }),
        Err(error) => {
            let outcome = match &error {
                ReadError::Admission(_) => Outcome::LocalAdmission,
                ReadError::Transport(_) => Outcome::ReadTransport,
                ReadError::Remote(_) => Outcome::ReadRemote,
            };
            Err(FailedOperation {
                outcome,
                detail: error.to_string(),
            })
        }
    }
}

/// Every operation begins timing before this function invokes eager admission.
async fn execute_step(
    client: &RockBeeRaftClient,
    step: usize,
    key: &Bytes,
    first: &Bytes,
    second: &Bytes,
) -> OperationResult {
    match step {
        0 | 2 => {
            let value = if step == 0 { first } else { second };
            let result = match client.put(key.clone(), value.clone()) {
                Ok(handle) => Ok(handle.await),
                Err(error) => Err(error),
            };
            committed(result)
        }
        1 => exact_value(client.get(key.clone()).await, Some(first)),
        3 => exact_value(client.get(key.clone()).await, Some(second)),
        4 => {
            let result = match client.delete(key.clone()) {
                Ok(handle) => Ok(handle.await),
                Err(error) => Err(error),
            };
            committed(result)
        }
        5 => exact_value(client.get(key.clone()).await, None),
        _ => unreachable!("the six-step cycle has a fixed operation set"),
    }
}

#[derive(Default)]
struct CycleAccounting {
    completed_cycles: u64,
    successful_operations: u64,
    partial_cycles: u64,
}

impl CycleAccounting {
    fn finish_cycle(&mut self, successes: u64) {
        self.successful_operations += successes;
        if successes == 6 {
            self.completed_cycles += 1;
        } else {
            self.partial_cycles += 1;
        }
    }
}

struct OperationMetrics {
    latency: Histogram<u64>,
    outcomes: [u64; 8],
    censored: u64,
    latency_out_of_range: u64,
}

impl OperationMetrics {
    fn new() -> Self {
        let mut latency = Histogram::new_with_bounds(1, MAX_LATENCY_MICROS, 3).unwrap();
        latency.auto(false);
        Self {
            latency,
            outcomes: [0; 8],
            censored: 0,
            latency_out_of_range: 0,
        }
    }

    fn record(&mut self, elapsed: Duration, outcome: Outcome) -> bool {
        self.outcomes[outcome as usize] += 1;
        // HDR rounds its physical capacity upward. Enforce our advertised bound
        // explicitly; record zero microseconds honestly at 1-us resolution.
        let micros = u64::try_from(elapsed.as_micros());
        if let Ok(micros) = micros
            && micros <= MAX_LATENCY_MICROS
            && self.latency.record(micros).is_ok()
        {
            return true;
        }
        self.latency_out_of_range += 1;
        false
    }
}

struct WorkerMetrics {
    operations: [OperationMetrics; 6],
    accounting: CycleAccounting,
    group_cycles: [u64; GROUPS],
    per_second_successes: Vec<u64>,
    active_successes: Option<u64>,
    in_flight: Option<usize>,
    first_failure: Option<String>,
}

impl WorkerMetrics {
    fn new(seconds: u64) -> Self {
        Self {
            operations: std::array::from_fn(|_| OperationMetrics::new()),
            accounting: CycleAccounting::default(),
            group_cycles: [0; GROUPS],
            per_second_successes: vec![0; usize::try_from(seconds + DRAIN_SECONDS + 2).unwrap()],
            active_successes: None,
            in_flight: None,
            first_failure: None,
        }
    }

    fn record_completion(
        &mut self,
        step: usize,
        latency: Duration,
        since_start: Duration,
        result: &OperationResult,
    ) -> bool {
        self.in_flight = None;
        let outcome = result
            .as_ref()
            .map_or_else(|error| error.outcome, |_| Outcome::Success);
        let bounded = self.operations[step].record(latency, outcome);
        if result.is_ok() {
            *self.active_successes.as_mut().unwrap() += 1;
            let bucket = usize::try_from(since_start.as_secs()).unwrap();
            if let Some(count) = self.per_second_successes.get_mut(bucket) {
                *count += 1;
            } else {
                self.first_failure.get_or_insert_with(|| {
                    "completion outside bounded measurement buckets".to_owned()
                });
                return false;
            }
        } else if let Err(error) = result {
            self.first_failure.get_or_insert_with(|| {
                format!("{}: {:?}: {}", OP_NAMES[step], error.outcome, error.detail)
            });
        }
        if !bounded {
            self.first_failure
                .get_or_insert_with(|| "latency exceeded fixed HDR range".to_owned());
        }
        bounded && result.is_ok()
    }

    fn finish_cycle(&mut self, group: usize) {
        let successes = self.active_successes.take().unwrap();
        self.accounting.finish_cycle(successes);
        if successes == 6 {
            self.group_cycles[group] += 1;
        }
    }

    fn finalize_joined_worker(&mut self) {
        if let Some(step) = self.in_flight.take() {
            self.operations[step].censored += 1;
        }
        if let Some(successes) = self.active_successes.take() {
            self.accounting.finish_cycle(successes);
        }
    }
}

fn worker_keys(seed: u64, worker: usize) -> [Bytes; GROUPS] {
    let shards = ShardConfig::new(4, 0).unwrap();
    std::array::from_fn(|group| {
        (0..10_000)
            .find_map(|nonce| {
                let key = Bytes::from(format!("soak/{seed}/{worker}/{group}/{nonce}"));
                (shards.group_for_key(&key).unwrap().get() == u64::try_from(group + 1).unwrap())
                    .then_some(key)
            })
            .expect("bounded deterministic search must cover each data group")
    })
}

fn payload(seed: u64, worker: usize, cycle: u64, version: u64) -> Bytes {
    let mut value = [0x5a; PAYLOAD_BYTES];
    for (chunk, number) in value[..32].as_chunks_mut::<8>().0.iter_mut().zip([
        seed,
        u64::try_from(worker).unwrap(),
        cycle,
        version,
    ]) {
        chunk.copy_from_slice(&number.to_be_bytes());
    }
    Bytes::copy_from_slice(&value)
}

struct Worker {
    diagnostics: Option<diagnostics::Recorder>,
    id: usize,
    config: SoakConfig,
    client: RockBeeRaftClient,
    metrics: Arc<Mutex<WorkerMetrics>>,
    stop: watch::Sender<StopReason>,
}

impl Worker {
    async fn run(self, ready: Arc<Barrier>, mut start: watch::Receiver<Option<Instant>>) {
        let keys = worker_keys(self.config.seed, self.id);
        ready.wait().await;
        start.wait_for(Option::is_some).await.unwrap();
        let started = (*start.borrow()).unwrap();
        let deadline = started + Duration::from_secs(self.config.seconds);
        let mut cycle = 0_u64;
        while *self.stop.borrow() == StopReason::Running && Instant::now() < deadline {
            let group = usize::try_from(cycle % 4).unwrap();
            let first = payload(self.config.seed, self.id, cycle, 1);
            let second = payload(self.config.seed, self.id, cycle, 2);
            self.metrics.lock().unwrap().active_successes = Some(0);
            for step in 0..6 {
                self.metrics.lock().unwrap().in_flight = Some(step);
                let operation_started = Instant::now();
                let result = execute_step(&self.client, step, &keys[group], &first, &second).await;
                let completed = Instant::now();
                if let Some(recorder) = &self.diagnostics {
                    recorder.operation(
                        self.id,
                        group,
                        step,
                        (operation_started - started)..(completed - started),
                        &result,
                    );
                }
                let success = self.metrics.lock().unwrap().record_completion(
                    step,
                    completed - operation_started,
                    completed - started,
                    &result,
                );
                if !success {
                    request_stop(&self.stop, StopReason::Validation);
                    break; // Never issue a dependent request after a failed step.
                }
            }
            self.metrics.lock().unwrap().finish_cycle(group);
            cycle += 1;
        }
        if Instant::now() >= deadline {
            request_stop(&self.stop, StopReason::Duration);
        }
    }
}

/// Returns only after every task has completed or has been aborted AND joined.
async fn drain_workers(workers: &mut JoinSet<()>, bound: Duration) -> bool {
    let deadline = Instant::now() + bound;
    let mut clean = true;
    while !workers.is_empty() {
        match tokio::time::timeout_at(deadline, workers.join_next()).await {
            Ok(Some(result)) => {
                clean &= result.is_ok();
            }
            Ok(None) => break,
            Err(_) => {
                clean = false;
                workers.abort_all();
                while workers.join_next().await.is_some() {}
            }
        }
    }
    clean
}

async fn warm_all_groups(
    client: &RockBeeRaftClient,
    worker_id: usize,
    config: SoakConfig,
    stop: &watch::Sender<StopReason>,
) -> Result<(), String> {
    let keys = worker_keys(config.seed, worker_id);
    let first = payload(config.seed, worker_id, u64::MAX, 1);
    let second = payload(config.seed, worker_id, u64::MAX, 2);
    for key in keys {
        if *stop.borrow() != StopReason::Running {
            return Err("stopped during warmup".to_owned());
        }
        // Election readiness is outside measurement. This only retries read
        // errors, never a successful absence/value and never a mutation.
        if wait_for_read_authority(client, key.clone(), stop)
            .await?
            .is_some()
        {
            return Err("fresh warmup key unexpectedly exists".to_owned());
        }
        for (step, name) in OP_NAMES.iter().enumerate() {
            execute_step(client, step, &key, &first, &second)
                .await
                .map_err(|error| format!("warmup {name}: {error:?}"))?;
        }
    }
    Ok(())
}

async fn wait_for_read_authority(
    client: &RockBeeRaftClient,
    key: Bytes,
    stop: &watch::Sender<StopReason>,
) -> Result<Option<Bytes>, String> {
    timeout(Duration::from_secs(25), async {
        loop {
            if *stop.borrow() != StopReason::Running {
                return Err("stopped while waiting for warmup read authority".to_owned());
            }
            if let Ok(value) = client.get(key.clone()).await {
                return Ok(value);
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| "warmup read authority deadline exceeded".to_owned())?
}

struct Measurement {
    elapsed: Duration,
    workers_joined_cleanly: bool,
    metrics: Vec<Arc<Mutex<WorkerMetrics>>>,
}

async fn measure(
    clients: &[RockBeeRaftClient],
    config: SoakConfig,
    stop: &watch::Sender<StopReason>,
    cluster: &ProcessCluster,
    report_dir: &Path,
) -> Measurement {
    let mut workers = JoinSet::new();
    let mut metrics = Vec::with_capacity(config.concurrency);
    let ready = Arc::new(Barrier::new(config.concurrency + 1));
    let (start_tx, start_rx) = watch::channel(None);
    let diagnostics = diagnostics::Diagnostics::start(cluster, start_rx.clone())
        .await
        .expect("diagnostic links must connect before workers are spawned");
    for id in 0..config.concurrency {
        let worker_metrics = Arc::new(Mutex::new(WorkerMetrics::new(config.seconds)));
        let worker = Worker {
            diagnostics: diagnostics
                .as_ref()
                .map(|observer| observer.recorder.clone()),
            id,
            config,
            client: clients[id % clients.len()].clone(),
            metrics: Arc::clone(&worker_metrics),
            stop: stop.clone(),
        };
        workers.spawn(worker.run(Arc::clone(&ready), start_rx.clone()));
        metrics.push(worker_metrics);
    }
    ready.wait().await;
    let started = Instant::now();
    start_tx.send(Some(started)).unwrap();
    // Optional externally controlled cancellation checkpoint; file creation is
    // inside the owner scope and cannot drop the workers on an I/O failure.
    if let Ok(Some(path)) = env_setting("ROCKBEE_SOAK_READY_FILE") {
        let pids = cluster
            .nodes
            .iter()
            .map(|node| node.child.id().to_string())
            .collect::<Vec<_>>()
            .join("\n");
        if let Err(error) = fs::write(path, format!("report={}\n{pids}\n", report_dir.display())) {
            eprintln!("soak ready checkpoint: {error}");
            request_stop(stop, StopReason::Validation);
        }
    }
    let mut stop_rx = stop.subscribe();
    let mut worker_exit_clean = true;
    if *stop_rx.borrow() == StopReason::Running {
        tokio::select! {
            _ = sleep_until(started + Duration::from_secs(config.seconds)) => request_stop(stop, StopReason::Duration),
            _ = stop_rx.changed() => {},
            result = workers.join_next() => {
                worker_exit_clean = result.is_some_and(|result| result.is_ok());
                if *stop.borrow() == StopReason::Running { request_stop(stop, StopReason::Validation); }
            }
        }
    }
    let workers_joined_cleanly =
        drain_workers(&mut workers, Duration::from_secs(DRAIN_SECONDS)).await && worker_exit_clean;
    let elapsed = started.elapsed();
    if let Some(diagnostics) = diagnostics {
        diagnostics.finish(report_dir).await;
    }
    for metrics in &metrics {
        metrics.lock().unwrap().finalize_joined_worker();
    }
    Measurement {
        elapsed,
        workers_joined_cleanly,
        metrics,
    }
}

fn create_report_directory() -> PathBuf {
    let parent = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../reports/soak");
    fs::create_dir_all(&parent).unwrap();
    tempfile::Builder::new()
        .prefix("stable-")
        .tempdir_in(parent)
        .unwrap()
        .keep()
}

fn write_report(
    report_dir: &Path,
    config: SoakConfig,
    warmup: Duration,
    measurement: Option<&Measurement>,
    reason: StopReason,
    shutdown: &[String],
    setup_error: Option<&str>,
) -> std::io::Result<bool> {
    let mut totals = WorkerMetrics::new(config.seconds);
    if let Some(measurement) = measurement {
        for metrics in &measurement.metrics {
            let metrics = metrics.lock().unwrap();
            for (total, operation) in totals.operations.iter_mut().zip(&metrics.operations) {
                total.latency.add(&operation.latency).unwrap();
                for (total, count) in total.outcomes.iter_mut().zip(operation.outcomes) {
                    *total += count;
                }
                total.censored += operation.censored;
                total.latency_out_of_range += operation.latency_out_of_range;
            }
            totals.accounting.completed_cycles += metrics.accounting.completed_cycles;
            totals.accounting.partial_cycles += metrics.accounting.partial_cycles;
            totals.accounting.successful_operations += metrics.accounting.successful_operations;
            for (total, count) in totals.group_cycles.iter_mut().zip(metrics.group_cycles) {
                *total += count;
            }
            for (total, count) in totals
                .per_second_successes
                .iter_mut()
                .zip(&metrics.per_second_successes)
            {
                *total += count;
            }
            if totals.first_failure.is_none() {
                totals.first_failure.clone_from(&metrics.first_failure);
            }
        }
    }
    let elapsed = measurement.map_or(0.0, |measurement| measurement.elapsed.as_secs_f64());
    let errors: u64 = totals
        .operations
        .iter()
        .map(|operation| {
            operation.outcomes[1..].iter().sum::<u64>()
                + operation.censored
                + operation.latency_out_of_range
        })
        .sum();
    let success = setup_error.is_none()
        && totals.first_failure.is_none()
        && reason == StopReason::Duration
        && errors == 0
        && totals.accounting.partial_cycles == 0
        && totals.group_cycles.iter().all(|count| *count > 0)
        && measurement.is_some_and(|measurement| measurement.workers_joined_cleanly)
        && shutdown.iter().all(|status| status.ends_with("=ok"));
    let mut summary = File::create(report_dir.join("summary.csv"))?;
    write!(
        summary,
        "operation,completed,hdr_samples,censored,latency_out_of_range,p50_us,p95_us,p99_us,max_us"
    )?;
    for name in OUTCOME_NAMES {
        write!(summary, ",{name}")?;
    }
    writeln!(summary)?;
    for (name, operation) in OP_NAMES.iter().zip(&totals.operations) {
        write!(
            summary,
            "{name},{},{},{},{},{},{},{},{}",
            operation.outcomes.iter().sum::<u64>(),
            operation.latency.len(),
            operation.censored,
            operation.latency_out_of_range,
            operation.latency.value_at_quantile(0.50),
            operation.latency.value_at_quantile(0.95),
            operation.latency.value_at_quantile(0.99),
            operation.latency.max()
        )?;
        for count in operation.outcomes {
            write!(summary, ",{count}")?;
        }
        writeln!(summary)?;
    }
    let mut per_second = File::create(report_dir.join("goodput.csv"))?;
    writeln!(
        per_second,
        "second,interval_seconds,successful_operations,operations_per_second"
    )?;
    for (second, count) in totals.per_second_successes.iter().enumerate() {
        let interval = (elapsed - second as f64).clamp(0.0, 1.0);
        if interval == 0.0 {
            break;
        }
        writeln!(
            per_second,
            "{second},{interval:.9},{count},{:.3}",
            *count as f64 / interval
        )?;
    }
    let mut manifest = File::create(report_dir.join("manifest.txt"))?;
    writeln!(
        manifest,
        "status={}\nworkload=warm closed-loop; no maximum-capacity claim\nprofile={}\narch={}\nos={}\nconcurrency={}\npayload_bytes={}\ndata_groups=4\nseed={}\nhash_seed=0\nconfigured_seconds={}\nwarmup_seconds={:.9}\nactual_elapsed_seconds={elapsed:.9}\ndrain_included=true\nstop_reason={reason:?}\nworkers_joined_before_resource_close=true\nworkers_joined_cleanly={}\ncompleted_cycles={}\npartial_cycles={}\nsuccessful_operations={}\noperations_per_second={:.3}\ncycles_per_second={:.3}\ngroup_completed_cycles={:?}\nerrors={errors}\nhdr=7.6.0; microseconds; 3 significant figures; 0..=60000000; auto_resize=false\nlatency_population=all completed attempts; censored attempts have no fabricated sample\nrss_cpu=not sampled\nsetup_error={:?}\nfirst_failure={:?}",
        if success { "PASS" } else { "FAIL" },
        if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
        std::env::consts::ARCH,
        std::env::consts::OS,
        config.concurrency,
        PAYLOAD_BYTES,
        config.seed,
        config.seconds,
        warmup.as_secs_f64(),
        measurement.is_some_and(|measurement| measurement.workers_joined_cleanly),
        totals.accounting.completed_cycles,
        totals.accounting.partial_cycles,
        totals.accounting.successful_operations,
        if elapsed > 0.0 {
            totals.accounting.successful_operations as f64 / elapsed
        } else {
            0.0
        },
        if elapsed > 0.0 {
            totals.accounting.completed_cycles as f64 / elapsed
        } else {
            0.0
        },
        totals.group_cycles,
        setup_error,
        totals.first_failure
    )?;
    for status in shutdown {
        writeln!(manifest, "shutdown_{status}")?;
    }
    writeln!(
        manifest,
        "client_connections={}\ntotal_workers={}\ngateways={}\nnode_processes=3\nworker_connection_mapping=worker_id % client_connections",
        config.clients, config.concurrency, config.gateway_count,
    )?;
    writeln!(
        manifest,
        "gateway_max_connections={}\ngateway_max_outstanding={}\ngateway_request_bytes={}\ngateway_response_bytes={}",
        config.gateway_server.max_connections,
        config.gateway_server.max_outstanding,
        config.gateway_server.request_bytes,
        config.gateway_server.response_bytes,
    )?;
    writeln!(
        manifest,
        "node_request_budget_mib={}\nnode_response_budget_mib={}",
        config.node_budget_mib, config.node_budget_mib,
    )?;
    Ok(success)
}

/// Bind once so every independently connected public client targets one gateway.
async fn start_gateway(
    cluster: &ProcessCluster,
    config: SoakConfig,
) -> (GatewayRuntime, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let gateway = GatewayRuntime::start_with_listener(
        GatewayConfig {
            cluster_id: cluster.directory.cluster_id,
            expected_shard_config: cluster.directory.shard_config,
            seeds: cluster
                .directory
                .nodes
                .iter()
                .map(|node| SeedNode {
                    node_id: node.node_id,
                    address: node.data_address,
                })
                .collect(),
            listen_address: address,
            backend_link: LinkConfig::default(),
            server: config.gateway_server,
            startup_timeout: Duration::from_secs(10),
        },
        listener,
    )
    .await
    .unwrap();
    (gateway, address)
}

/// The caller retains every successful connection even if a later connect or
/// warmup fails, and closes that entire collection through the common exit path.
async fn connect_and_warm_clients(
    clients: &mut Vec<RockBeeRaftClient>,
    address: SocketAddr,
    config: SoakConfig,
    stop: &watch::Sender<StopReason>,
) -> Result<(), String> {
    for id in 0..config.clients {
        if *stop.borrow() != StopReason::Running {
            return Err("stopped during client connection setup".to_owned());
        }
        let client = RockBeeRaftClient::connect(address, ClientConfig::default())
            .await
            .map_err(|error| format!("client {id} connection failed: {error}"))?;
        clients.push(client);
    }
    for (id, client) in clients.iter().enumerate() {
        warm_all_groups(client, id, config, stop)
            .await
            .map_err(|error| format!("client {id}: {error}"))?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual opt-in: stable three-node warm closed-loop soak, defaults to 60 seconds"]
async fn stable_three_node_soak() {
    // The harness owns the in-process client and gateway logger. Libraries
    // emit events only; child node applications initialize their own logger.
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_micros()
        .is_test(true)
        .try_init();
    let config = SoakConfig::from_environment().unwrap();
    // Register both handlers before launching any owned child process.
    let mut interrupt = signal(SignalKind::interrupt()).unwrap();
    let mut terminate = signal(SignalKind::terminate()).unwrap();
    let (stop, _) = watch::channel(StopReason::Running);
    let signal_stop = stop.clone();
    let signal_task = tokio::spawn(async move {
        tokio::select! { _ = interrupt.recv() => {}, _ = terminate.recv() => {} }
        request_stop(&signal_stop, StopReason::Signal);
    });
    let report_dir = create_report_directory();
    eprintln!("SOAK_REPORT={}", report_dir.display());
    let mut cluster =
        ProcessCluster::start_with_server_budget(Some(report_dir.clone()), config.node_budget_mib);
    cluster.wait_until_directory_is_published().await;
    let (mut gateway, address) = start_gateway(&cluster, config).await;
    let mut clients = Vec::with_capacity(config.clients);
    let warmup_started = Instant::now();
    let setup = connect_and_warm_clients(&mut clients, address, config, &stop).await;
    let warmup = warmup_started.elapsed();
    let measurement = if setup.is_ok() && *stop.borrow() == StopReason::Running {
        Some(measure(&clients, config, &stop, &cluster, &report_dir).await)
    } else {
        request_stop(&stop, StopReason::Validation);
        None
    };
    // No worker can still reference the connection or gateway past this point.
    let mut shutdown = Vec::with_capacity(clients.len() + 4);
    for (id, client) in clients.iter().enumerate() {
        shutdown.push(format!(
            "client_{id}={}",
            if timeout(Duration::from_secs(10), client.close())
                .await
                .is_ok()
            {
                "ok"
            } else {
                "timeout"
            }
        ));
    }
    shutdown.push(format!(
        "gateway={}",
        if timeout(Duration::from_secs(10), gateway.close())
            .await
            .is_ok()
        {
            "ok"
        } else {
            "timeout"
        }
    ));
    for node in &mut cluster.nodes {
        let result = node.try_stop_gracefully().await;
        shutdown.push(format!(
            "node_{}={}",
            node.child.id(),
            result.map_or_else(|error| error, |()| "ok".to_owned())
        ));
        node.reap();
    }
    signal_task.abort();
    let _ = signal_task.await;
    let passed = write_report(
        &report_dir,
        config,
        warmup,
        measurement.as_ref(),
        *stop.borrow(),
        &shutdown,
        setup.as_ref().err().map(String::as_str),
    )
    .unwrap();
    drop(cluster); // Copies all owned logs before removing the temporary DBs.
    assert!(
        passed,
        "stable soak validation failed; report: {}",
        report_dir.display()
    );
}

#[test]
fn settings_reject_unbounded_or_invalid_loads() {
    assert_eq!(bounded_setting(None, 60, 3600).unwrap(), 60);
    assert_eq!(bounded_setting(Some("2"), 60, 3600).unwrap(), 2);
    for invalid in ["0", "3601", "-1", "invalid"] {
        assert!(bounded_setting(Some(invalid), 60, 3600).is_err());
    }
    assert!(bounded_setting(Some("65"), 1, 64).is_err());
}

#[test]
fn client_settings_distinguish_connections_from_workers() {
    assert_eq!(client_connections(None, 8).unwrap(), 1);
    for count in [1, 2, 3, 64, 100] {
        assert_eq!(
            client_connections(Some(&count.to_string()), count).unwrap(),
            usize::try_from(count).unwrap()
        );
    }
    assert_eq!(client_connections(Some("2"), 8).unwrap(), 2);
    for invalid in ["0", "101", "-1", "invalid"] {
        assert!(client_connections(Some(invalid), 100).is_err());
    }
    assert!(client_connections(Some("3"), 2).is_err());
}

#[test]
fn report_fails_when_accounting_detects_an_error_outside_outcome_counts() {
    let directory = tempfile::tempdir().unwrap();
    let config = SoakConfig {
        seconds: 1,
        concurrency: 1,
        clients: 1,
        gateway_count: 1,
        seed: 42,
        gateway_server: ServerConfig::default(),
        node_budget_mib: 64,
    };
    let mut metrics = WorkerMetrics::new(1);
    metrics.group_cycles = [1; GROUPS];
    metrics.first_failure = Some("completion outside bounded measurement buckets".to_owned());
    let measurement = Measurement {
        elapsed: Duration::from_secs(1),
        workers_joined_cleanly: true,
        metrics: vec![Arc::new(Mutex::new(metrics))],
    };
    assert!(
        !write_report(
            directory.path(),
            config,
            Duration::ZERO,
            Some(&measurement),
            StopReason::Duration,
            &[],
            None
        )
        .unwrap()
    );
}

#[test]
fn partial_cycles_keep_successful_prefix_without_inventing_completions() {
    let mut accounting = CycleAccounting::default();
    accounting.finish_cycle(6);
    accounting.finish_cycle(3);
    assert_eq!(accounting.completed_cycles, 1);
    assert_eq!(accounting.successful_operations, 9);
    assert_eq!(accounting.partial_cycles, 1);
}

#[test]
fn completion_accounting_keeps_failures_and_censored_work_visible() {
    let mut metrics = WorkerMetrics::new(1);
    metrics.active_successes = Some(0);
    assert!(metrics.record_completion(
        0,
        Duration::from_micros(12),
        Duration::from_millis(20),
        &Ok(())
    ));
    let mismatch = exact_value(
        Ok(Some(Bytes::from_static(b"stale"))),
        Some(&Bytes::from_static(b"new")),
    );
    assert!(!metrics.record_completion(
        1,
        Duration::from_micros(20),
        Duration::from_millis(30),
        &mismatch
    ));
    metrics.finish_cycle(0);
    metrics.active_successes = Some(0);
    metrics.in_flight = Some(0);
    metrics.finalize_joined_worker();
    assert_eq!(metrics.accounting.successful_operations, 1);
    assert_eq!(metrics.accounting.partial_cycles, 2);
    assert_eq!(metrics.per_second_successes[0], 1);
    assert_eq!(
        metrics.operations[1].outcomes[Outcome::WrongValue as usize],
        1
    );
    assert_eq!(metrics.operations[0].censored, 1);
    assert!(!metrics.operations[2].record(Duration::from_secs(61), Outcome::Success));
    assert_eq!(metrics.operations[2].latency_out_of_range, 1);
    assert_eq!(metrics.operations[2].latency.len(), 0);
}

#[test]
fn deterministic_keys_cover_four_groups_and_payloads_detect_stale_versions() {
    let shards = ShardConfig::new(4, 0).unwrap();
    let first = worker_keys(42, 0);
    let second = worker_keys(42, 1);
    for (group, key) in first.iter().enumerate() {
        assert_eq!(
            shards.group_for_key(key).unwrap().get(),
            u64::try_from(group + 1).unwrap()
        );
        assert!(!second.contains(key));
    }
    let value = payload(42, 0, 7, 1);
    assert_eq!(value.len(), PAYLOAD_BYTES);
    assert_ne!(value, payload(42, 0, 7, 2));
    assert_ne!(value, payload(42, 0, 8, 1));
    assert_ne!(value, payload(42, 1, 7, 1));
}

#[tokio::test]
async fn drain_aborts_and_joins_workers_before_cleanup_can_start() {
    use std::sync::atomic::{AtomicBool, Ordering};
    struct WorkerLifetime(Arc<AtomicBool>);
    impl Drop for WorkerLifetime {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let dropped = Arc::new(AtomicBool::new(false));
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let mut workers = JoinSet::new();
    let worker_dropped = Arc::clone(&dropped);
    workers.spawn(async move {
        let _lifetime = WorkerLifetime(worker_dropped);
        let _ = started_tx.send(());
        std::future::pending::<()>().await;
    });
    started_rx.await.unwrap();
    assert!(!drain_workers(&mut workers, Duration::from_millis(1)).await);
    assert!(workers.is_empty());
    assert!(dropped.load(Ordering::SeqCst));
}
