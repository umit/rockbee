#![cfg_attr(test, allow(clippy::unwrap_used, clippy::panic))]

use rockbee_protocol::transport::{FrameReader, read_frame, write_frame};
use rockbee_protocol::{
    Request, RequestEnvelope, Response, ResponseEnvelope, ServiceRole, StatusSnapshot,
};
use serde_json::Value;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

const TEST_BOUND: Duration = Duration::from_secs(3);

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

async fn status_server(response: Response) -> (String, JoinHandle<()>) {
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
                response,
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
        let (input, _) = stream.into_split();
        let mut input = FrameReader::new(input);
        let _ = read_frame(&mut input).await.unwrap().unwrap();
        std::future::pending::<()>().await;
    });
    (address, task)
}

fn command(address: &str, args: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_rockbee-client"));
    command
        .args(["--addr", address, "status"])
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

async fn output(mut child: Child) -> Output {
    let deadline = Instant::now() + TEST_BOUND;
    loop {
        if child.try_wait().unwrap().is_some() {
            return child.wait_with_output().unwrap();
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            let _ = child.wait_with_output().unwrap();
            panic!("status child exceeded test deadline");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn record(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_reports_role_and_counters() {
    for (role, role_name) in [
        (ServiceRole::Gateway, "gateway"),
        (ServiceRole::Writer, "writer"),
        (ServiceRole::Reader, "reader"),
    ] {
        let (address, task) = status_server(Response::Status {
            snapshot: snapshot(role),
        })
        .await;
        let output = output(
            command(&address, &["--expect-role", role_name])
                .spawn()
                .unwrap(),
        )
        .await;
        assert!(output.status.success(), "{output:?}");
        let record = record(&output);
        assert_eq!(record["ok"], true);
        assert_eq!(record["endpoint"], address);
        assert_eq!(record["expected_role"], role_name);
        assert_eq!(record["observed_role"], role_name);
        assert_eq!(record["counters"]["requests_completed_total"], 5);
        assert!(record["error"].is_null());
        assert!(record["note"].as_str().unwrap().contains("does not prove"));
        tokio::time::timeout(TEST_BOUND, task)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_reports_role_mismatch_as_json_and_fails() {
    let (address, task) = status_server(Response::Status {
        snapshot: snapshot(ServiceRole::Reader),
    })
    .await;
    let output = output(
        command(&address, &["--expect-role", "writer"])
            .spawn()
            .unwrap(),
    )
    .await;
    assert!(!output.status.success());
    let record = record(&output);
    assert_eq!(record["ok"], false);
    assert_eq!(record["expected_role"], "writer");
    assert_eq!(record["observed_role"], "reader");
    assert_eq!(record["counters"]["connections_current"], 1);
    assert!(
        record["error"]
            .as_str()
            .unwrap()
            .contains("expected writer")
    );
    tokio::time::timeout(TEST_BOUND, task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_reports_unreachable_and_malformed_responses_as_json() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let unreachable = listener.local_addr().unwrap().to_string();
    drop(listener);
    let unreachable_output = output(command(&unreachable, &[]).spawn().unwrap()).await;
    assert!(!unreachable_output.status.success());
    let unreachable_record = record(&unreachable_output);
    assert_eq!(unreachable_record["ok"], false);
    assert!(unreachable_record["observed_role"].is_null());
    assert!(unreachable_record["counters"].is_null());
    assert!(unreachable_record["error"].as_str().is_some());

    let (address, task) = status_server(Response::Ok).await;
    let output = output(command(&address, &[]).spawn().unwrap()).await;
    assert!(!output.status.success());
    let record = record(&output);
    assert_eq!(record["ok"], false);
    assert!(record["error"].as_str().unwrap().contains("match"));
    tokio::time::timeout(TEST_BOUND, task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_honors_timeout_without_retrying() {
    let (address, task) = stalled_server().await;
    let started = Instant::now();
    let output = output(command(&address, &["--timeout-ms", "50"]).spawn().unwrap()).await;
    assert!(!output.status.success());
    assert!(started.elapsed() < Duration::from_secs(1));
    let record = record(&output);
    assert_eq!(record["ok"], false);
    assert!(record["error"].as_str().is_some());
    task.abort();
}

#[test]
fn status_rejects_invalid_flags_before_connecting() {
    for args in [
        ["--expect-role", "cache"].as_slice(),
        ["--timeout-ms", "0"].as_slice(),
        ["--timeout-ms", "not-a-number"].as_slice(),
        ["--timeout-ms", "10", "--timeout-ms", "20"].as_slice(),
        ["--expect-role"].as_slice(),
        ["--unknown", "value"].as_slice(),
    ] {
        let output = command("127.0.0.1:1", args).output().unwrap();
        assert!(
            !output.status.success(),
            "invalid status args unexpectedly succeeded: {args:?}"
        );
        assert!(
            output.stdout.is_empty(),
            "invalid args performed a Status request"
        );
    }
}
