//! Opt-in real-process smoke test. It requires an already-running LocalStack bucket.

use rockbee_client::{MutationOutcome, RockBeeClient};
use rockbee_test_support::Result;
use serde_json::Value;
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use tokio::time::{Instant, sleep, sleep_until, timeout};

const READY_TIMEOUT: Duration = Duration::from_secs(30);
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(30);
const OP_TIMEOUT: Duration = Duration::from_secs(10);
static LOG_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

struct OwnedChild {
    child: Child,
    stderr_log: PathBuf,
}

impl OwnedChild {
    fn kill(&mut self) -> Result<()> {
        if self.child.try_wait()?.is_none() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait()?;
        Ok(())
    }
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

struct ReadyChild {
    process: OwnedChild,
    address: String,
}

fn environment(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn binary_dir() -> Result<PathBuf> {
    let directory = env::var("ROCKBEE_BIN_DIR")
        .map_err(|_| "ROCKBEE_BIN_DIR is required; run through make test-local")?;
    let directory = PathBuf::from(directory);
    if !directory.is_absolute() {
        return Err("ROCKBEE_BIN_DIR must be an absolute path".into());
    }
    Ok(directory)
}

fn binary(directory: &Path, name: &str) -> Result<PathBuf> {
    let path = directory.join(name);
    if !path.is_file() {
        return Err(format!("required binary does not exist: {}", path.display()).into());
    }
    Ok(path)
}

fn unique_prefix() -> Result<String> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "system clock is before the Unix epoch")?
        .as_millis();
    Ok(format!(
        "rockbee-localstack-{}-{millis}",
        std::process::id()
    ))
}

fn stderr_log(logs: &TempDir, label: &str) -> Result<(PathBuf, File)> {
    let sequence = LOG_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let path = logs.path().join(format!("{label}-{sequence}.stderr.log"));
    Ok((path.clone(), File::create(path)?))
}

fn readiness_error(
    process: &mut OwnedChild,
    message: impl std::fmt::Display,
) -> Box<dyn std::error::Error + Send + Sync> {
    let stderr = std::fs::read_to_string(&process.stderr_log).unwrap_or_else(|error| {
        format!("<could not read {}: {error}>", process.stderr_log.display())
    });
    let _ = process.kill();
    format!("{message}; stderr={stderr:?}").into()
}

fn start_process(
    executable: &Path,
    args: &[String],
    expected_role: &str,
    logs: &TempDir,
) -> Result<ReadyChild> {
    let (stderr_log, stderr) = stderr_log(logs, expected_role)?;
    let child = Command::new(executable)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::from(stderr))
        .spawn()?;
    let mut process = OwnedChild { child, stderr_log };
    let stdout = process
        .child
        .stdout
        .take()
        .ok_or("child stdout was not piped")?;
    let (send, receive) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let mut line = String::new();
        let result = BufReader::new(stdout).read_line(&mut line).map(|_| line);
        let _ = send.send(result);
    });
    let line = match receive.recv_timeout(READY_TIMEOUT) {
        Ok(Ok(line)) => line,
        Ok(Err(error)) => return Err(readiness_error(&mut process, error)),
        Err(_) => {
            return Err(readiness_error(
                &mut process,
                format!("{expected_role} did not print ready JSON in {READY_TIMEOUT:?}"),
            ));
        }
    };
    let address =
        parse_ready(&line, expected_role).map_err(|error| readiness_error(&mut process, error))?;
    Ok(ReadyChild { process, address })
}

fn parse_ready(line: &str, expected_role: &str) -> Result<String> {
    let ready: Value = serde_json::from_str(line)?;
    if ready.get("event") != Some(&Value::String("ready".into()))
        || ready.get("role") != Some(&Value::String(expected_role.into()))
    {
        return Err(format!("unexpected {expected_role} ready line: {line:?}").into());
    }
    let address = ready["addr"]
        .as_str()
        .ok_or("ready address missing")?
        .to_owned();
    let parsed: std::net::SocketAddr = address.parse()?;
    if !parsed.ip().is_loopback() || parsed.port() == 0 {
        return Err(format!("invalid ready address: {address}").into());
    }
    Ok(address)
}

fn start_node(
    executable: &Path,
    role: &str,
    endpoint: &str,
    bucket: &str,
    prefix: &str,
    logs: &TempDir,
) -> Result<ReadyChild> {
    let args = [
        "--addr",
        "127.0.0.1:0",
        "--backend",
        "localstack",
        "--endpoint",
        endpoint,
        "--bucket",
        bucket,
        "--prefix",
        prefix,
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<Vec<_>>();
    start_process(executable, &args, role, logs)
}

fn start_gateway(
    executable: &Path,
    writer: &str,
    reader: &str,
    logs: &TempDir,
) -> Result<ReadyChild> {
    let config = logs.path().join("gateway.toml");
    // These addresses come from the owned roles' parsed loopback readiness records.
    std::fs::write(
        &config,
        format!("addr = \"127.0.0.1:0\"\nwriter = \"{writer}\"\nreaders = [\"{reader}\"]\n"),
    )?;
    let args = vec!["--config".to_owned(), config.to_string_lossy().into_owned()];
    start_process(executable, &args, "gateway", logs)
}

async fn put(
    client: &RockBeeClient,
    key: &[u8],
    value: &[u8],
    ttl: Option<Duration>,
) -> Result<()> {
    timeout(OP_TIMEOUT, client.put(key, value, ttl))
        .await
        .map_err(|_| "PUT timed out")?
        .map_err(Into::into)
}

async fn get(client: &RockBeeClient, key: &[u8]) -> Result<Option<Vec<u8>>> {
    timeout(OP_TIMEOUT, client.get(key))
        .await
        .map_err(|_| "GET timed out")?
}

async fn delete(client: &RockBeeClient, key: &[u8]) -> Result<()> {
    timeout(OP_TIMEOUT, client.delete(key))
        .await
        .map_err(|_| "DELETE timed out")?
        .map_err(Into::into)
}

async fn eventually_value(
    client: &RockBeeClient,
    key: &[u8],
    expected: Option<&[u8]>,
) -> Result<()> {
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    loop {
        if get(client, key).await?.as_deref() == expected {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("reader did not converge for key {key:?}").into());
        }
        sleep(Duration::from_millis(50)).await;
    }
}

async fn run_localstack(logs: &TempDir) -> Result<()> {
    let binaries = binary_dir()?;
    let writer_binary = binary(&binaries, "rockbee-writer")?;
    let reader_binary = binary(&binaries, "rockbee-reader")?;
    let gateway_binary = binary(&binaries, "rockbee-gateway")?;
    let endpoint = environment("ROCKBEE_S3_ENDPOINT", "http://127.0.0.1:4566");
    let bucket = environment("ROCKBEE_S3_BUCKET", "rockbee-test");
    let prefix = unique_prefix()?;
    eprintln!("localstack prefix={prefix}; logs={}", logs.path().display());

    let writer = start_node(&writer_binary, "writer", &endpoint, &bucket, &prefix, logs)?;
    let writer_client = RockBeeClient::connect(&writer.address).await?;

    let zero_key = b"\0";
    let bytes_key = b"\0key\xff";
    let bytes_value = b"\x00value\xff";
    put(&writer_client, zero_key, b"\0", None).await?;
    put(&writer_client, bytes_key, bytes_value, None).await?;
    if get(&writer_client, zero_key).await?.as_deref() != Some(b"\0".as_slice())
        || get(&writer_client, bytes_key).await?.as_deref() != Some(bytes_value.as_slice())
    {
        return Err("writer did not preserve exact byte keys or values".into());
    }

    let reader = start_node(&reader_binary, "reader", &endpoint, &bucket, &prefix, logs)?;
    let reader_client = RockBeeClient::connect(&reader.address).await?;
    eventually_value(&reader_client, zero_key, Some(b"\0")).await?;
    eventually_value(&reader_client, bytes_key, Some(bytes_value)).await?;
    if let Err(error) = put(&reader_client, b"reader-put", b"denied", None).await {
        if !error.to_string().contains("read-only") {
            return Err(error);
        }
    } else {
        return Err("reader accepted PUT".into());
    }
    if let Err(error) = delete(&reader_client, bytes_key).await {
        if !error.to_string().contains("read-only") {
            return Err(error);
        }
    } else {
        return Err("reader accepted DELETE".into());
    }

    let gateway = start_gateway(&gateway_binary, &writer.address, &reader.address, logs)?;
    let gateway_client = RockBeeClient::connect(&gateway.address).await?;
    eventually_value(&gateway_client, bytes_key, Some(bytes_value)).await?;
    put(&gateway_client, b"gateway-put", b"original-value", None).await?;
    eventually_value(&gateway_client, b"gateway-put", Some(b"original-value")).await?;
    // Rejected empty values must not overwrite an acknowledged value, even
    // when the caller supplies a TTL. The connection remains usable for CRUD.
    for ttl in [None, Some(Duration::from_secs(30))] {
        let error = timeout(OP_TIMEOUT, gateway_client.put(b"gateway-put", b"", ttl))
            .await?
            .expect_err("client accepted an empty PUT value");
        assert_eq!(error.outcome(), MutationOutcome::NotSent);
    }
    assert_eq!(
        get(&writer_client, b"gateway-put").await?.as_deref(),
        Some(b"original-value".as_slice())
    );
    put(&gateway_client, b"gateway-put", b"replacement-value", None).await?;
    eventually_value(&reader_client, b"gateway-put", Some(b"replacement-value")).await?;
    delete(&gateway_client, bytes_key).await?;
    eventually_value(&gateway_client, bytes_key, None).await?;

    // First make reader and gateway observe an old non-expiring value. The TTL
    // replacement must become visible before its real thirty-second expiry.
    let ttl_key = b"expires-without-resurrection";
    put(&writer_client, ttl_key, b"old-forever", None).await?;
    eventually_value(&reader_client, ttl_key, Some(b"old-forever")).await?;
    eventually_value(&gateway_client, ttl_key, Some(b"old-forever")).await?;
    eprintln!("localstack phase=ttl-visible");
    put(
        &gateway_client,
        ttl_key,
        b"new-thirty-second-ttl",
        Some(Duration::from_secs(30)),
    )
    .await?;

    // Give another key a thirty-second expiry, then replace it with TTL zero
    // before readers have converged. Waiting past this key's own deadline
    // proves that zero clears, rather than inherits, the earlier expiry.
    let forever_key = b"ttl-zero-overwrites-prior-ttl";
    put(
        &writer_client,
        forever_key,
        b"old-ttl",
        Some(Duration::from_secs(30)),
    )
    .await?;
    let old_ttl_deadline = Instant::now() + Duration::from_secs(30);
    put(
        &writer_client,
        forever_key,
        b"ttl-zero-is-forever",
        Some(Duration::ZERO),
    )
    .await?;

    eventually_value(&reader_client, ttl_key, Some(b"new-thirty-second-ttl")).await?;
    eventually_value(&gateway_client, ttl_key, Some(b"new-thirty-second-ttl")).await?;
    // Poll both paths while the thirty-second TTL runs. This also keeps the
    // independently configured RPC connections active within their normal
    // read-timeout window; no timeout setting is relaxed for this smoke test.
    let (reader_expired, gateway_expired) = tokio::join!(
        eventually_value(&reader_client, ttl_key, None),
        eventually_value(&gateway_client, ttl_key, None),
    );
    reader_expired.map_err(|error| format!("reader expiry observation failed: {error}"))?;
    gateway_expired.map_err(|error| format!("gateway expiry observation failed: {error}"))?;
    sleep_until(old_ttl_deadline).await;
    eprintln!("localstack phase=ttl-expired");
    // The original writer connection was deliberately idle during the TTL
    // window. Reconnect for post-expiry storage and fencing assertions, so
    // this test does not depend on a separate idle-connection policy.
    let writer_client = RockBeeClient::connect(&writer.address).await?;
    if get(&writer_client, forever_key).await?.as_deref() != Some(b"ttl-zero-is-forever".as_slice())
    {
        return Err("TTL zero value retained its overwritten expiry".into());
    }

    // A second writer is allowed to become current. Its ownership must fence A;
    // after the fence is observed, B's durable value survives B's hard stop.
    eprintln!("localstack phase=writer-fencing");
    let mut writer_b = start_node(&writer_binary, "writer", &endpoint, &bucket, &prefix, logs)?;
    let writer_b_client = RockBeeClient::connect(&writer_b.address).await?;
    put(
        &writer_b_client,
        b"durable",
        b"ack-before-writer-b-kill",
        None,
    )
    .await?;
    let fence_deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    loop {
        match put(&writer_client, b"fence-probe", b"old-writer", None).await {
            Err(error) if error.to_string().contains("detected newer DB client") => break,
            Err(error) => {
                return Err(format!(
                    "old writer failed before reporting its newer-writer fence: {error}"
                )
                .into());
            }
            Ok(()) if Instant::now() < fence_deadline => sleep(Duration::from_millis(50)).await,
            Ok(()) => return Err("old writer was not fenced by the newer writer".into()),
        }
    }
    put(
        &writer_b_client,
        b"durable",
        b"ack-before-writer-b-kill",
        None,
    )
    .await?;
    writer_b.process.kill()?;
    let reopened_reader = start_node(&reader_binary, "reader", &endpoint, &bucket, &prefix, logs)?;
    let reopened_client = RockBeeClient::connect(&reopened_reader.address).await?;
    eventually_value(
        &reopened_client,
        b"durable",
        Some(b"ack-before-writer-b-kill"),
    )
    .await?;

    eprintln!("localstack phase=durability-reopen");
    drop(reopened_reader);
    drop(gateway);
    drop(reader);
    drop(writer);
    Ok(())
}

#[tokio::test]
#[ignore = "requires LocalStack; run make test-local"]
async fn localstack_writer_reader_gateway_bytes_ttl_and_restart() -> Result<()> {
    let logs = tempfile::tempdir()?;
    let result = run_localstack(&logs).await;
    if result.is_err() {
        let path = logs.keep();
        eprintln!("localstack retained failure logs at {}", path.display());
    }
    result
}

async fn run_reader_visibility(logs: &TempDir) -> Result<Value> {
    let binaries = binary_dir()?;
    let endpoint = environment("ROCKBEE_S3_ENDPOINT", "http://127.0.0.1:4566");
    let bucket = environment("ROCKBEE_S3_BUCKET", "rockbee-test");
    let prefix = format!("{}-visibility", unique_prefix()?);
    let writer = start_node(
        &binary(&binaries, "rockbee-writer")?,
        "writer",
        &endpoint,
        &bucket,
        &prefix,
        logs,
    )?;
    let mut readers = Vec::new();
    for poll_interval_ms in [100, 800] {
        let config = logs.path().join(format!("reader-{poll_interval_ms}.toml"));
        std::fs::write(
            &config,
            format!("[reader]\npoll_interval_ms = {poll_interval_ms}\n"),
        )?;
        let args = [
            "--addr",
            "127.0.0.1:0",
            "--backend",
            "localstack",
            "--endpoint",
            &endpoint,
            "--bucket",
            &bucket,
            "--prefix",
            &prefix,
            "--config",
            config.to_str().ok_or("config path is not UTF-8")?,
        ]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
        readers.push(start_process(
            &binary(&binaries, "rockbee-reader")?,
            &args,
            "reader",
            logs,
        )?);
    }
    let gateway_args = [
        "--addr",
        "127.0.0.1:0",
        "--writer",
        &writer.address,
        "--reader",
        &readers[0].address,
        "--reader",
        &readers[1].address,
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<Vec<_>>();
    let gateway = start_process(
        &binary(&binaries, "rockbee-gateway")?,
        &gateway_args,
        "gateway",
        logs,
    )?;
    let stdout = logs.path().join("visibility.json");
    let (stderr_log, stderr) = stderr_log(logs, "visibility")?;
    let child = Command::new(binary(&binaries, "examples/visibility_probe")?)
        .args([
            &gateway.address,
            "2",
            "--reader",
            &readers[0].address,
            "--reader",
            &readers[1].address,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::from(File::create(&stdout)?))
        .stderr(Stdio::from(stderr))
        .spawn()?;
    let mut probe = OwnedChild { child, stderr_log };
    let deadline = Instant::now() + Duration::from_secs(90);
    let status = loop {
        if let Some(status) = probe.child.try_wait()? {
            break status;
        }
        if Instant::now() >= deadline {
            return Err(readiness_error(&mut probe, "visibility probe did not exit"));
        }
        sleep(Duration::from_millis(20)).await;
    };
    let output = std::fs::read_to_string(&stdout)?;
    if !status.success() {
        return Err(readiness_error(
            &mut probe,
            format!("visibility probe {status}: {output}"),
        ));
    }
    let report: Value = serde_json::from_str(&output)?;
    assert_eq!(report["status"], "ok");
    for operation in ["puts", "deletes"] {
        let measurements = report[operation].as_array().ok_or("missing measurements")?;
        assert_eq!(measurements.len(), 2);
        for measurement in measurements {
            assert_eq!(measurement["status"], "ok");
            let observations = measurement["readers"]
                .as_array()
                .ok_or("missing reader observations")?;
            assert_eq!(observations.len(), readers.len());
            for reader in &readers {
                let observation = observations
                    .iter()
                    .find(|observation| observation["endpoint"] == reader.address)
                    .ok_or("configured reader was not measured")?;
                assert_eq!(observation["status"], "ok");
                assert!(observation["first_match_after_ack_ms"].as_f64().is_some());
                assert!(observation["get_attempts"].as_u64().unwrap_or(0) > 0);
            }
        }
    }
    eprintln!("localstack per-reader visibility: {output}");
    Ok(report)
}

#[tokio::test]
#[ignore = "requires LocalStack; run make test-local"]
async fn localstack_visibility_probe_observes_each_configured_reader() -> Result<()> {
    let logs = tempfile::tempdir()?;
    let result = run_reader_visibility(&logs).await;
    if result.is_err() {
        let path = logs.keep();
        eprintln!(
            "localstack retained visibility failure logs at {}",
            path.display()
        );
    }
    result.map(|_| ())
}
