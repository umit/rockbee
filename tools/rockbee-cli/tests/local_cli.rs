#![cfg_attr(test, allow(clippy::unwrap_used, clippy::panic))]

use rockbee_protocol::transport::{FrameReader, read_frame, write_frame};
use rockbee_protocol::{
    Request, RequestEnvelope, Response, ResponseEnvelope, ServiceRole, StatusSnapshot,
};
use serde_json::Value;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

const PROBE_BOUND: Duration = Duration::from_secs(3);
const RUN_BOUND: Duration = Duration::from_secs(5);
static UNIQUE: AtomicU64 = AtomicU64::new(0);

fn unique_name(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let sequence = UNIQUE.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{}-{nanos}-{sequence}", std::process::id())
}

fn local() -> Command {
    Command::new(env!("CARGO_BIN_EXE_rockbee-local"))
}

fn snapshot(role: ServiceRole) -> StatusSnapshot {
    StatusSnapshot {
        role,
        connections_current: 1,
        connections_accepted_total: 2,
        connections_rejected_total: 3,
        connections_failed_total: 4,
        requests_completed_total: 5,
        error_responses_total: 6,
    }
}

async fn status_server(role: ServiceRole) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);
        let frame = read_frame(&mut input).await.unwrap().unwrap();
        let request = RequestEnvelope::decode(&frame).unwrap();
        assert_eq!(request.request, Request::Status);
        write_frame(
            &mut output,
            &ResponseEnvelope {
                request_id: request.request_id,
                response: Response::Status {
                    snapshot: snapshot(role),
                },
            },
        )
        .await
        .unwrap();
    });
    (address, task)
}

async fn eof_server() -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (input, _output) = stream.into_split();
        let mut input = FrameReader::new(input);
        let frame = read_frame(&mut input).await.unwrap().unwrap();
        let request = RequestEnvelope::decode(&frame).unwrap();
        assert_eq!(request.request, Request::Status);
    });
    (address, task)
}

async fn malformed_status_server() -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);
        let frame = read_frame(&mut input).await.unwrap().unwrap();
        let request = RequestEnvelope::decode(&frame).unwrap();
        assert_eq!(request.request, Request::Status);
        write_frame(
            &mut output,
            &ResponseEnvelope {
                request_id: request.request_id,
                response: Response::Ok,
            },
        )
        .await
        .unwrap();
    });
    (address, task)
}

async fn stalled_server() -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (input, _output) = stream.into_split();
        let mut input = FrameReader::new(input);
        let _ = read_frame(&mut input).await.unwrap().unwrap();
        std::future::pending::<()>().await;
    });
    (address, task)
}

async fn probe(gateway: &str, writer: &str, reader: &str) -> Output {
    let child = local()
        .args([
            "probe",
            "--gateway",
            gateway,
            "--writer",
            writer,
            "--reader",
            reader,
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    output_bounded(child, PROBE_BOUND).await
}

async fn output_bounded(mut child: Child, bound: Duration) -> Output {
    let deadline = Instant::now() + bound;
    loop {
        if child.try_wait().unwrap().is_some() {
            return child.wait_with_output().unwrap();
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            let _ = child.wait_with_output().unwrap();
            panic!("probe child exceeded test deadline");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn records(output: &Output) -> Vec<Value> {
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn probe_reports_every_expected_role_and_snapshot() {
    let (gateway, gateway_task) = status_server(ServiceRole::Gateway).await;
    let (writer, writer_task) = status_server(ServiceRole::Writer).await;
    let (reader, reader_task) = status_server(ServiceRole::Reader).await;

    let output = probe(&gateway, &writer, &reader).await;
    assert!(output.status.success(), "{output:?}");
    let records = records(&output);
    assert_eq!(records.len(), 3);
    for (record, endpoint, role) in [
        (&records[0], &gateway, "gateway"),
        (&records[1], &writer, "writer"),
        (&records[2], &reader, "reader"),
    ] {
        assert_eq!(record["endpoint"], endpoint.as_str());
        assert_eq!(record["expected_role"], role);
        assert_eq!(record["observed_role"], role);
        assert_eq!(record["ok"], true);
        assert_eq!(record["snapshot"]["requests_completed_total"], 5);
        assert!(record["error"].is_null());
    }
    tokio::time::timeout(PROBE_BOUND, gateway_task)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(PROBE_BOUND, writer_task)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(PROBE_BOUND, reader_task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn probe_fails_with_machine_readable_role_mismatch() {
    let (gateway, gateway_task) = status_server(ServiceRole::Reader).await;
    let (writer, writer_task) = status_server(ServiceRole::Writer).await;
    let (reader, reader_task) = status_server(ServiceRole::Reader).await;

    let output = probe(&gateway, &writer, &reader).await;
    assert!(!output.status.success());
    let records = records(&output);
    assert_eq!(records[0]["expected_role"], "gateway");
    assert_eq!(records[0]["observed_role"], "reader");
    assert_eq!(records[0]["ok"], false);
    assert!(records[0]["error"].as_str().unwrap().contains("role"));
    tokio::time::timeout(PROBE_BOUND, gateway_task)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(PROBE_BOUND, writer_task)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(PROBE_BOUND, reader_task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn probe_reports_eof_without_aborting_other_endpoint_results() {
    let (gateway, gateway_task) = eof_server().await;
    let (writer, writer_task) = status_server(ServiceRole::Writer).await;
    let (reader, reader_task) = status_server(ServiceRole::Reader).await;

    let output = probe(&gateway, &writer, &reader).await;
    assert!(!output.status.success());
    let records = records(&output);
    assert_eq!(records.len(), 3);
    assert_eq!(records[0]["ok"], false);
    assert!(records[0]["error"].as_str().unwrap().contains("EOF"));
    assert_eq!(records[1]["ok"], true);
    assert_eq!(records[2]["ok"], true);
    tokio::time::timeout(PROBE_BOUND, gateway_task)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(PROBE_BOUND, writer_task)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(PROBE_BOUND, reader_task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn probe_reports_a_malformed_status_response_without_aborting_other_results() {
    let (gateway, gateway_task) = malformed_status_server().await;
    let (writer, writer_task) = status_server(ServiceRole::Writer).await;
    let (reader, reader_task) = status_server(ServiceRole::Reader).await;

    let output = probe(&gateway, &writer, &reader).await;
    assert!(!output.status.success());
    let records = records(&output);
    assert_eq!(records.len(), 3);
    assert_eq!(records[0]["ok"], false);
    assert!(records[0]["error"].as_str().unwrap().contains("match"));
    assert_eq!(records[1]["ok"], true);
    assert_eq!(records[2]["ok"], true);
    tokio::time::timeout(PROBE_BOUND, gateway_task)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(PROBE_BOUND, writer_task)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(PROBE_BOUND, reader_task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn probe_times_out_a_stalled_endpoint_within_its_global_bound() {
    let (gateway, gateway_task) = stalled_server().await;
    let (writer, writer_task) = status_server(ServiceRole::Writer).await;
    let (reader, reader_task) = status_server(ServiceRole::Reader).await;

    let started = Instant::now();
    let output = probe(&gateway, &writer, &reader).await;
    assert!(!output.status.success());
    assert!(
        started.elapsed() < PROBE_BOUND,
        "probe exceeded its 2s deadline"
    );
    let records = records(&output);
    assert_eq!(records.len(), 3);
    assert_eq!(records[0]["ok"], false);
    assert!(records[0]["error"].as_str().unwrap().contains("timed out"));
    assert_eq!(records[1]["ok"], true);
    assert_eq!(records[2]["ok"], true);
    gateway_task.abort();
    tokio::time::timeout(PROBE_BOUND, writer_task)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(PROBE_BOUND, reader_task)
        .await
        .unwrap()
        .unwrap();
}

#[test]
fn probe_requires_exactly_one_gateway_one_writer_and_at_least_one_reader() {
    for args in [
        ["probe"].as_slice(),
        [
            "probe",
            "--gateway",
            "127.0.0.1:1",
            "--writer",
            "127.0.0.1:2",
        ]
        .as_slice(),
        [
            "probe",
            "--gateway",
            "127.0.0.1:1",
            "--gateway",
            "127.0.0.1:2",
            "--writer",
            "127.0.0.1:3",
            "--reader",
            "127.0.0.1:4",
        ]
        .as_slice(),
    ] {
        let output = local().args(args).output().unwrap();
        assert!(
            !output.status.success(),
            "args unexpectedly succeeded: {args:?}"
        );
    }
}

#[test]
fn probe_rejects_hostnames_before_networking_and_accepts_bracketed_ipv6() {
    let hostname = local()
        .args([
            "probe",
            "--gateway",
            "localhost:1",
            "--writer",
            "127.0.0.1:2",
            "--reader",
            "127.0.0.1:3",
        ])
        .output()
        .unwrap();
    assert!(!hostname.status.success());
    assert!(String::from_utf8_lossy(&hostname.stderr).contains("literal IP"));

    let ipv6 = local()
        .args([
            "probe",
            "--gateway",
            "[::1]:1",
            "--writer",
            "[::1]:2",
            "--reader",
            "[::1]:3",
        ])
        .output()
        .unwrap();
    assert!(!ipv6.status.success());
    assert_eq!(records(&ipv6).len(), 3);
}

#[test]
fn run_rejects_missing_config_before_starting_any_child() {
    let output = local()
        .args([
            "run",
            "--writer-config",
            "/definitely/not/rockbee-writer.toml",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("writer config"));
}

#[test]
fn run_rejects_missing_values_and_duplicate_singleton_flags_before_starting_children() {
    for args in [
        ["run", "--writer-config"].as_slice(),
        [
            "run",
            "--writer-config",
            "first.toml",
            "--writer-config",
            "second.toml",
        ]
        .as_slice(),
    ] {
        let output = local().args(args).output().unwrap();
        assert!(
            !output.status.success(),
            "args unexpectedly succeeded: {args:?}"
        );
    }
}

#[cfg(unix)]
struct RunFixture {
    root: PathBuf,
    writer_config: PathBuf,
    reader_config: PathBuf,
}

#[cfg(unix)]
impl RunFixture {
    fn new(writer: &str, reader: &str, gateway: &str) -> Self {
        let root = std::env::temp_dir().join(unique_name("rockbee-local-cli-test"));
        fs::create_dir_all(&root).unwrap();
        for (name, script) in [
            ("rockbee-writer", writer),
            ("rockbee-reader", reader),
            ("rockbee-gateway", gateway),
        ] {
            let path = root.join(name);
            fs::write(&path, script).unwrap();
            let mut permissions = fs::metadata(&path).unwrap().permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(path, permissions).unwrap();
        }
        let writer_config = root.join("writer.toml");
        let reader_config = root.join("reader.toml");
        fs::write(&writer_config, "").unwrap();
        fs::write(&reader_config, "").unwrap();
        Self {
            root,
            writer_config,
            reader_config,
        }
    }

    fn command(&self) -> Command {
        let mut command = local();
        command.args([
            "run",
            "--bin-dir",
            self.root.to_str().unwrap(),
            "--writer-config",
            self.writer_config.to_str().unwrap(),
            "--reader-config",
            self.reader_config.to_str().unwrap(),
        ]);
        command
    }
}

#[cfg(unix)]
impl Drop for RunFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_failure_cleans_only_the_writer_child_already_owned_by_run() {
    let marker = std::env::temp_dir().join(unique_name("rockbee-local-writer-term"));
    let fixture = RunFixture::new(
        "#!/bin/sh\ntrap 'echo stopped > \"$ROCKBEE_TEST_MARKER\"; exit 0' TERM INT\nprintf '%s\\n' '{\"event\":\"ready\",\"role\":\"writer\",\"addr\":\"127.0.0.1:41001\"}'\nwhile :; do sleep 1; done\n",
        "#!/bin/sh\nexit 17\n",
        "#!/bin/sh\nexit 18\n",
    );
    let mut unrelated = Command::new("/bin/sleep").arg("30").spawn().unwrap();
    let mut command = fixture.command();
    command.env("ROCKBEE_TEST_MARKER", &marker);
    let output = output_bounded(
        command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
        RUN_BOUND,
    )
    .await;
    assert!(!output.status.success(), "{output:?}");
    assert!(marker.is_file(), "owned writer did not receive TERM");
    assert!(
        unrelated.try_wait().unwrap().is_none(),
        "unrelated process was touched"
    );
    unrelated.kill().unwrap();
    unrelated.wait().unwrap();
    let _ = fs::remove_file(marker);
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigint_during_reader_startup_cleans_the_owned_writer_and_fails() {
    let ready_marker = std::env::temp_dir().join(unique_name("rockbee-local-writer-ready"));
    let stopped_marker = std::env::temp_dir().join(unique_name("rockbee-local-writer-stopped"));
    let fixture = RunFixture::new(
        "#!/bin/sh\ntrap 'echo stopped > \"$ROCKBEE_TEST_STOPPED\"; exit 0' TERM INT\necho ready > \"$ROCKBEE_TEST_READY\"\nprintf '%s\\n' '{\"event\":\"ready\",\"role\":\"writer\",\"addr\":\"127.0.0.1:41002\"}'\nwhile :; do sleep 1; done\n",
        "#!/bin/sh\ntrap 'exit 0' TERM INT\nwhile :; do sleep 1; done\n",
        "#!/bin/sh\nexit 18\n",
    );
    let mut command = fixture.command();
    command
        .env("ROCKBEE_TEST_READY", &ready_marker)
        .env("ROCKBEE_TEST_STOPPED", &stopped_marker)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let child = command.spawn().unwrap();
    let deadline = Instant::now() + RUN_BOUND;
    while !ready_marker.is_file() {
        assert!(Instant::now() < deadline, "writer never reached readiness");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let child_pid = child.id().to_string();
    Command::new("/bin/kill")
        .args(["-INT", &child_pid])
        .status()
        .unwrap();
    let output = output_bounded(child, RUN_BOUND).await;
    assert!(!output.status.success(), "{output:?}");
    assert!(
        stopped_marker.is_file(),
        "writer did not receive shutdown signal"
    );
    let _ = fs::remove_file(ready_marker);
    let _ = fs::remove_file(stopped_marker);
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nonzero_child_shutdown_makes_run_fail_after_normal_signal() {
    use std::io::{BufRead, BufReader};
    let fixture = RunFixture::new(
        "#!/bin/sh\ntrap 'exit 17' TERM INT\nprintf '%s\\n' '{\"event\":\"ready\",\"role\":\"writer\",\"addr\":\"127.0.0.1:41003\"}'\nwhile :; do sleep 1; done\n",
        "#!/bin/sh\ntrap 'exit 0' TERM INT\nprintf '%s\\n' '{\"event\":\"ready\",\"role\":\"reader\",\"addr\":\"127.0.0.1:41004\"}'\nwhile :; do sleep 1; done\n",
        "#!/bin/sh\ntrap 'exit 0' TERM INT\nprintf '%s\\n' '{\"event\":\"ready\",\"role\":\"gateway\",\"addr\":\"127.0.0.1:41005\"}'\nwhile :; do sleep 1; done\n",
    );
    let mut child = fixture
        .command()
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (send, lines) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if send.send(line.unwrap()).is_err() {
                break;
            }
        }
    });
    let deadline = Instant::now() + RUN_BOUND;
    let ready = loop {
        if let Ok(line) = lines.try_recv() {
            break serde_json::from_str::<Value>(&line).unwrap();
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("operator did not become ready");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert_eq!(ready["event"], "ready");
    assert!(
        Command::new("/bin/kill")
            .args(["-TERM", &child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let output = output_bounded(child, RUN_BOUND).await;
    assert!(!output.status.success(), "{output:?}");
    let stopped: Value =
        serde_json::from_str(&lines.recv_timeout(Duration::from_secs(1)).unwrap()).unwrap();
    assert_eq!(stopped["event"], "stopped");
    assert_eq!(stopped["reason"], "signal");
    assert_eq!(stopped["forced"], false);
    let writer = stopped["children"]
        .as_array()
        .unwrap()
        .iter()
        .find(|child| child["role"] == "writer")
        .unwrap();
    assert_eq!(writer["success"], false);
    assert_eq!(writer["forced"], false);
}
