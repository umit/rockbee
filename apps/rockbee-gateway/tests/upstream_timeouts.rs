#![cfg_attr(test, allow(clippy::unwrap_used))]
//! Regression coverage for timeout settings after they leave gateway TOML.

use rockbee_client::{ClientConfig, MutationOutcome, RockBeeClient};
use rockbee_gateway::{Gateway, Result, config::Config};
use rockbee_protocol::transport::{FrameReader, read_frame, write_frame};
use rockbee_protocol::{RequestEnvelope, Response, ResponseEnvelope};
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tempfile::NamedTempFile;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout};

const TEST_BOUND: Duration = Duration::from_secs(3);

#[derive(Clone)]
enum Reply {
    Delayed(Duration, Response),
    Stall,
}

struct Backend {
    address: String,
    calls: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

impl Drop for Backend {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn backend(reply: Reply) -> Backend {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&calls);
    let task = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (stream, _) = accepted.unwrap();
                    let seen = Arc::clone(&seen);
                    let reply = reply.clone();
                    connections.spawn(async move {
                        let (input, mut output) = stream.into_split();
                        let mut input = FrameReader::new(input);
                        while let Ok(Some(frame)) = read_frame(&mut input).await {
                            let request = RequestEnvelope::decode(&frame).unwrap();
                            seen.fetch_add(1, Ordering::SeqCst);
                            match &reply {
                                Reply::Stall => std::future::pending::<()>().await,
                                Reply::Delayed(delay, response) => {
                                    tokio::time::sleep(*delay).await;
                                    if write_frame(&mut output, &ResponseEnvelope {
                                        request_id: request.request_id,
                                        response: response.clone(),
                                    }).await.is_err() {
                                        return;
                                    }
                                }
                            }
                        }
                    });
                }
                _ = connections.join_next(), if !connections.is_empty() => {}
            }
        }
    });
    Backend {
        address,
        calls,
        task,
    }
}

struct RunningGateway {
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<()>>>,
}

impl Drop for RunningGateway {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl RunningGateway {
    async fn stop(mut self) -> Result<()> {
        self.stop
            .take()
            .expect("gateway stop sender is present")
            .send(())
            .map_err(|_| "gateway stop receiver dropped")?;
        let mut task = self.task.take().expect("gateway task is present");
        match timeout(TEST_BOUND, &mut task).await {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => Err(error.into()),
            Err(_) => {
                task.abort();
                let _ = task.await;
                Err("gateway did not stop within test bound".into())
            }
        }
    }
}

fn gateway_config(
    writer: &str,
    readers: &[String],
    request_timeout_ms: u64,
    reader_attempt_timeout_ms: u64,
) -> Result<Config> {
    let mut file = NamedTempFile::new()?;
    let readers = readers
        .iter()
        .map(|reader| format!("{reader:?}"))
        .collect::<Vec<_>>()
        .join(", ");
    writeln!(
        file,
        "addr = \"127.0.0.1:0\"\nwriter = {writer:?}\nreaders = [{readers}]\n\n[upstream]\nrequest_timeout_ms = {request_timeout_ms}\nreader_attempt_timeout_ms = {reader_attempt_timeout_ms}"
    )?;
    Config::parse([
        "--config".to_owned(),
        file.path().to_string_lossy().into_owned(),
    ])?
    .ok_or_else(|| "configuration unexpectedly requested help".into())
}

async fn start_gateway(config: Config) -> Result<(String, RunningGateway)> {
    let gateway = Gateway::bind_with_config(
        &config.address,
        config.writer,
        config.readers,
        config.upstream,
        config.rpc,
    )
    .await?;
    let address = gateway.local_addr()?.to_string();
    let (stop, stopped) = oneshot::channel();
    let task = tokio::spawn(gateway.run_until(async move {
        stopped.await?;
        Ok(())
    }));
    Ok((
        address,
        RunningGateway {
            stop: Some(stop),
            task: Some(task),
        },
    ))
}

async fn client(address: &str) -> Result<RockBeeClient> {
    RockBeeClient::connect_with_config(address, ClientConfig::new(TEST_BOUND, 8, 8)?).await
}

#[tokio::test]
async fn toml_reader_attempt_timeout_fails_over_before_the_default_attempt_window() -> Result<()> {
    let writer = backend(Reply::Stall).await;
    let slow_reader = backend(Reply::Stall).await;
    let healthy_reader = backend(Reply::Delayed(
        Duration::from_millis(250),
        Response::Value {
            value: Some(b"fresh".to_vec()),
        },
    ))
    .await;
    let config = gateway_config(
        &writer.address,
        &[slow_reader.address.clone(), healthy_reader.address.clone()],
        800,
        150,
    )?;
    let (address, gateway) = start_gateway(config).await?;
    let client = client(&address).await?;

    assert_eq!(client.get(b"key").await?, Some(b"fresh".to_vec()));
    assert_eq!(slow_reader.calls.load(Ordering::SeqCst), 1);
    assert_eq!(healthy_reader.calls.load(Ordering::SeqCst), 1);

    client.shutdown().await;
    gateway.stop().await
}

#[tokio::test]
async fn final_reader_only_receives_the_remaining_toml_request_budget() -> Result<()> {
    let writer = backend(Reply::Stall).await;
    let first_reader = backend(Reply::Delayed(
        Duration::from_millis(180),
        Response::Error {
            message: "lagging".into(),
            mutation_outcome: MutationOutcome::Unknown,
        },
    ))
    .await;
    let final_reader = backend(Reply::Delayed(
        Duration::from_millis(180),
        Response::Value {
            value: Some(b"too late".to_vec()),
        },
    ))
    .await;
    let config = gateway_config(
        &writer.address,
        &[first_reader.address.clone(), final_reader.address.clone()],
        280,
        240,
    )?;
    let (address, gateway) = start_gateway(config).await?;
    let client = client(&address).await?;

    assert!(client.get(b"key").await.is_err());
    assert_eq!(first_reader.calls.load(Ordering::SeqCst), 1);
    assert_eq!(final_reader.calls.load(Ordering::SeqCst), 1);

    client.shutdown().await;
    gateway.stop().await
}

#[tokio::test]
async fn reader_attempt_larger_than_request_budget_is_clamped_to_the_total_budget() -> Result<()> {
    let writer = backend(Reply::Stall).await;
    let stalled_reader = backend(Reply::Stall).await;
    let unused_reader = backend(Reply::Delayed(
        Duration::ZERO,
        Response::Value {
            value: Some(b"unexpected".to_vec()),
        },
    ))
    .await;
    let config = gateway_config(
        &writer.address,
        &[
            stalled_reader.address.clone(),
            unused_reader.address.clone(),
        ],
        180,
        900,
    )?;
    let (address, gateway) = start_gateway(config).await?;
    let client = client(&address).await?;

    assert!(client.get(b"key").await.is_err());
    assert_eq!(stalled_reader.calls.load(Ordering::SeqCst), 1);
    assert_eq!(unused_reader.calls.load(Ordering::SeqCst), 0);

    client.shutdown().await;
    gateway.stop().await
}

#[tokio::test]
async fn timed_out_toml_configured_mutation_is_unknown_and_is_not_replayed() -> Result<()> {
    let stalled_writer = backend(Reply::Stall).await;
    let reader = backend(Reply::Stall).await;
    let config = gateway_config(
        &stalled_writer.address,
        std::slice::from_ref(&reader.address),
        180,
        100,
    )?;
    let (address, gateway) = start_gateway(config).await?;
    let client = client(&address).await?;

    let started = Instant::now();
    let error = client.put(b"key", b"value", None).await.unwrap_err();
    assert!(started.elapsed() < Duration::from_millis(1500));
    assert_eq!(error.outcome(), MutationOutcome::Unknown);
    assert_eq!(stalled_writer.calls.load(Ordering::SeqCst), 1);

    client.shutdown().await;
    gateway.stop().await
}
