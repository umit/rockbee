//! Criterion measurement for one public client over one warm gateway path.
//!
//! The benchmark owns a local writer, reader, and gateway topology.

mod support;

use criterion::{BenchmarkId, Criterion, SamplingMode, Throughput};
use futures_util::stream::{FuturesUnordered, StreamExt};
use rockbee_client::RockBeeClient;
use std::env;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

const FIXTURE_COUNT: usize = 1_024;
const VALUE_BYTES: usize = 1_024;
const IN_FLIGHT: usize = 128;
const WARMUP: Duration = Duration::from_secs(10);
const MEASUREMENT: Duration = Duration::from_secs(60);
const SAMPLES: usize = 50;
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(60);
const CONVERGENCE_POLL: Duration = Duration::from_millis(50);

struct Fixture {
    key: Vec<u8>,
    value: Vec<u8>,
}

fn bench_warm_get(criterion: &mut Criterion, topology: &support::LocalTopology) {
    eprintln!(
        "warm_get reports: {}",
        topology.report_directory().display()
    );
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap_or_else(|error| {
            topology.benchmark_failure(format!("could not start Tokio runtime: {error}"))
        });
    let client = runtime
        .block_on(RockBeeClient::connect(topology.address()))
        .unwrap_or_else(|error| {
            topology.benchmark_failure(format!(
                "could not connect to the local gateway at {}: {error}",
                topology.address()
            ))
        });
    let fixtures = fixtures();

    if let Err(error) = runtime.block_on(prepare_and_converge(&client, &fixtures)) {
        runtime.block_on(client.shutdown());
        topology.benchmark_failure(format!("fixture setup failed: {error}"));
    }

    let mut group = criterion.benchmark_group("rockbee-client/warm-get");
    group.warm_up_time(WARMUP);
    group.measurement_time(MEASUREMENT);
    group.sample_size(SAMPLES);
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(IN_FLIGHT as u64));

    let benchmark = catch_unwind(AssertUnwindSafe(|| {
        group.bench_with_input(
            BenchmarkId::new("one-client-128-in-flight", FIXTURE_COUNT),
            &(),
            |bencher, _| {
                bencher.to_async(&runtime).iter_custom(|iterations| {
                    let client = client.clone();
                    let fixtures = Arc::clone(&fixtures);
                    async move {
                        run_continuous_gets(client, fixtures, iterations)
                            .await
                            .unwrap_or_else(|error| topology.benchmark_failure(error))
                    }
                });
            },
        );
        group.finish();
    }));

    runtime.block_on(client.shutdown());
    if let Err(payload) = benchmark {
        resume_unwind(payload);
    }
}

async fn prepare_and_converge(
    client: &RockBeeClient,
    fixtures: &[Arc<Fixture>],
) -> rockbee_client::Result<()> {
    prepare(client, fixtures).await?;
    tokio::time::timeout(CONVERGENCE_TIMEOUT, wait_until_exact(client, fixtures))
        .await
        .map_err(|_| "fixtures did not converge to the reader within 60 seconds")?
}

async fn wait_until_exact(
    client: &RockBeeClient,
    fixtures: &[Arc<Fixture>],
) -> rockbee_client::Result<()> {
    loop {
        if all_exact(client, fixtures).await? {
            return Ok(());
        }
        tokio::time::sleep(CONVERGENCE_POLL).await;
    }
}

async fn prepare(client: &RockBeeClient, fixtures: &[Arc<Fixture>]) -> rockbee_client::Result<()> {
    let mut next = 0;
    let mut in_flight = FuturesUnordered::new();
    while next < fixtures.len() || !in_flight.is_empty() {
        while next < fixtures.len() && in_flight.len() < IN_FLIGHT {
            let fixture = &fixtures[next];
            in_flight.push(client.put(&fixture.key, &fixture.value, None));
            next += 1;
        }
        in_flight
            .next()
            .await
            .expect("fixture PUT pipeline is non-empty")?;
    }
    Ok(())
}

async fn all_exact(
    client: &RockBeeClient,
    fixtures: &[Arc<Fixture>],
) -> rockbee_client::Result<bool> {
    let mut next = 0;
    let mut matched = true;
    let mut in_flight = FuturesUnordered::new();
    while next < fixtures.len() || !in_flight.is_empty() {
        while next < fixtures.len() && in_flight.len() < IN_FLIGHT {
            in_flight.push(get_exact(client.clone(), Arc::clone(&fixtures[next])));
            next += 1;
        }
        matched &= in_flight
            .next()
            .await
            .expect("fixture GET pipeline is non-empty")?;
    }
    Ok(matched)
}

async fn run_continuous_gets(
    client: RockBeeClient,
    fixtures: Arc<Vec<Arc<Fixture>>>,
    iterations: u64,
) -> Result<Duration, String> {
    // Criterion divides this duration by `iterations`. One iteration is 128
    // requests, so its reported time is amortized pipeline work, not an
    // individual request-latency percentile.
    let total = iterations
        .checked_mul(IN_FLIGHT as u64)
        .ok_or_else(|| "Criterion iteration count overflowed the request count".to_owned())?;
    let started = Instant::now();
    let mut issued = 0_u64;
    let mut completed = 0_u64;
    let mut next_fixture = 0_usize;
    let mut in_flight = FuturesUnordered::new();

    while issued < total || !in_flight.is_empty() {
        while issued < total && in_flight.len() < IN_FLIGHT {
            let fixture = Arc::clone(&fixtures[next_fixture]);
            next_fixture = (next_fixture + 1) % fixtures.len();
            issued += 1;
            in_flight.push(get_exact(client.clone(), fixture));
        }
        let exact = in_flight
            .next()
            .await
            .expect("continuous GET pipeline is non-empty")
            .map_err(|error| error.to_string())?;
        if !exact {
            return Err("GET returned a missing fixture during measurement".to_owned());
        }
        completed += 1;
    }

    if completed != total {
        return Err(format!("completed {completed} GETs after issuing {total}"));
    }
    Ok(started.elapsed())
}

async fn get_exact(client: RockBeeClient, fixture: Arc<Fixture>) -> rockbee_client::Result<bool> {
    match client.get(&fixture.key).await? {
        Some(value) if value == fixture.value => Ok(true),
        Some(_) => Err("GET returned bytes different from the fixture".into()),
        None => Ok(false),
    }
}

fn fixtures() -> Arc<Vec<Arc<Fixture>>> {
    let namespace = format!(
        "criterion-warm-get-{}-{}",
        std::process::id(),
        Uuid::now_v7()
    );
    let mut fixtures = Vec::with_capacity(FIXTURE_COUNT);
    for index in 0..FIXTURE_COUNT {
        let mut value = vec![b'x'; VALUE_BYTES];
        value[..8].copy_from_slice(&(index as u64).to_be_bytes());
        fixtures.push(Arc::new(Fixture {
            key: format!("{namespace}:{index}").into_bytes(),
            value,
        }));
    }
    Arc::new(fixtures)
}

fn cargo_bench_invocation() -> bool {
    env::args().skip(1).any(|argument| argument == "--bench")
}

#[allow(clippy::panic)] // A failed request must invalidate Criterion's measurement.
fn benchmark_failure(message: String) -> ! {
    panic!("warm_get benchmark failed: {message}")
}

fn main() {
    if !cargo_bench_invocation() {
        eprintln!("warm_get Criterion benchmark skipped under cargo test");
        return;
    }
    // Criterion may exit for help or invalid arguments; parse before starting children.
    let criterion = Criterion::default().configure_from_args();
    let topology = support::LocalTopology::start().unwrap_or_else(|error| {
        benchmark_failure(format!("could not start local topology: {error}"))
    });
    let mut criterion = criterion.output_directory(&topology.report_directory().join("criterion"));
    bench_warm_get(&mut criterion, &topology);
    criterion.final_summary();
}
