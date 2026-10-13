//! Opt-in acceptance of the built foreground operator and real role binaries.
use rockbee_client::{Client, Result};
use serde_json::Value;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{Receiver, channel};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::{Instant, sleep, timeout};

const BOUND: Duration = Duration::from_secs(40);

struct Operator {
    child: Child,
    lines: Receiver<std::io::Result<String>>,
}

impl Operator {
    fn start(bin_dir: &std::path::Path, root: &std::path::Path) -> Result<Self> {
        let mut child = Command::new(bin_dir.join("rockbee-local"))
            .args(["run", "--bin-dir"])
            .arg(bin_dir)
            .arg("--writer-config")
            .arg(root.join("configs/writer.toml"))
            .arg("--reader-config")
            .arg(root.join("configs/reader.toml"))
            .arg("--endpoint")
            .arg(
                std::env::var("ROCKBEE_S3_ENDPOINT")
                    .unwrap_or_else(|_| "http://127.0.0.1:4566".into()),
            )
            .arg("--bucket")
            .arg(std::env::var("ROCKBEE_S3_BUCKET").unwrap_or_else(|_| "rockbee-test".into()))
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let output = child.stdout.take().expect("stdout configured as pipe");
        let (send, lines) = channel();
        std::thread::spawn(move || {
            for line in BufReader::new(output).lines() {
                if send.send(line).is_err() {
                    break;
                }
            }
        });
        Ok(Self { child, lines })
    }

    async fn event(&self) -> Result<Value> {
        timeout(BOUND, async {
            loop {
                match self.lines.try_recv() {
                    Ok(line) => return Ok(serde_json::from_str(&line?)?),
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                        return Err("operator stdout closed before event".into());
                    }
                    Err(std::sync::mpsc::TryRecvError::Empty) => {
                        sleep(Duration::from_millis(10)).await;
                    }
                }
            }
        })
        .await?
    }

    fn terminate(&mut self) -> Result<()> {
        if self.child.try_wait()?.is_none() {
            let status = Command::new("/bin/kill")
                .args(["-TERM", &self.child.id().to_string()])
                .status()?;
            if !status.success() {
                return Err("failed to signal owned operator".into());
            }
        }
        Ok(())
    }

    async fn wait(&mut self) -> Result<std::process::ExitStatus> {
        timeout(BOUND, async {
            loop {
                if let Some(status) = self.child.try_wait()? {
                    return Ok(status);
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await?
    }
}

impl Drop for Operator {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.terminate();
            let deadline = std::time::Instant::now() + BOUND;
            while matches!(self.child.try_wait(), Ok(None)) {
                if std::time::Instant::now() >= deadline {
                    // Last resort affects only this unreaped launcher. A timeout
                    // is a test failure; never discover/kill unrelated processes.
                    let _ = self.child.kill();
                    let _ = self.child.wait();
                    break;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    }
}

async fn eventually(client: &Client, key: &[u8], expected: Option<&[u8]>) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if client.get(key).await?.as_deref() == expected {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("reader did not converge to exact bytes within 30s".into());
        }
        sleep(Duration::from_millis(25)).await;
    }
}

async fn status(bin_dir: &std::path::Path, endpoint: &str, expected_role: &str) -> Result<Value> {
    let mut child = Command::new(bin_dir.join("rockbee-client"))
        .args([
            "--addr",
            endpoint,
            "status",
            "--expect-role",
            expected_role,
            "--timeout-ms",
            "2000",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let deadline = Instant::now() + Duration::from_secs(5);
    while child.try_wait()?.is_none() {
        if Instant::now() >= deadline {
            child.kill()?;
            child.wait()?;
            return Err("status exceeded external test deadline".into());
        }
        sleep(Duration::from_millis(10)).await;
    }
    let result = child.wait_with_output()?;
    if !result.status.success() {
        return Err(format!(
            "{expected_role} status failed: {}",
            String::from_utf8_lossy(&result.stderr)
        )
        .into());
    }
    Ok(serde_json::from_slice(&result.stdout)?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires release binaries, LocalStack and rockbee-test bucket; use make test-local"]
async fn local_operator_starts_probes_byte_crud_and_stops_owned_roles() -> Result<()> {
    let bin_dir = PathBuf::from(std::env::var("ROCKBEE_BIN_DIR")?);
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut operator = Operator::start(&bin_dir, &root)?;
    let ready = operator.event().await?;
    assert_eq!(ready["event"], "ready", "{ready}");
    assert!(
        ready["prefix"]
            .as_str()
            .unwrap()
            .starts_with("rockbee-local-")
    );
    let gateway = ready["gateway"].as_str().ok_or("missing gateway")?;
    let writer = ready["writer"].as_str().ok_or("missing writer")?;
    let reader = ready["readers"][0].as_str().ok_or("missing reader")?;
    assert_eq!(ready["pids"].as_array().unwrap().len(), 3);
    eprintln!("operator acceptance ready={ready}");

    let work = async {
        for (endpoint, role) in [(gateway, "gateway"), (writer, "writer"), (reader, "reader")] {
            let row = status(&bin_dir, endpoint, role).await?;
            assert_eq!(row["ok"], true, "{row}");
            assert_eq!(row["endpoint"], endpoint);
            assert_eq!(row["expected_role"], role);
            assert_eq!(row["observed_role"], role);
            assert_eq!(row["counters"]["requests_completed_total"], 0);
            assert_eq!(row["counters"]["error_responses_total"], 0);
        }

        let mut probe = Command::new(bin_dir.join("rockbee-local"))
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
            .stderr(Stdio::inherit())
            .spawn()?;
        let deadline = Instant::now() + Duration::from_secs(5);
        while probe.try_wait()?.is_none() {
            if Instant::now() >= deadline {
                probe.kill()?;
                probe.wait()?;
                return Err("probe exceeded external test deadline".into());
            }
            sleep(Duration::from_millis(10)).await;
        }
        let result = probe.wait_with_output()?;
        if !result.status.success() {
            return Err("role probe failed".into());
        }
        let rows: Vec<Value> = String::from_utf8(result.stdout)?
            .lines()
            .map(serde_json::from_str)
            .collect::<std::result::Result<_, _>>()?;
        assert_eq!(rows.len(), 3);
        for (row, role) in rows.iter().zip(["gateway", "writer", "reader"]) {
            assert_eq!(row["ok"], true, "{row}");
            assert_eq!(row["observed_role"], role);
            assert_eq!(row["snapshot"]["requests_completed_total"], 0);
        }

        let client = Client::connect(gateway).await?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("operator-{suffix}\0").into_bytes();
        let value = [0_u8, 255, 128, 0, 42];
        let handle = client.put(&key, &value, None);
        handle.await?;
        eventually(&client, &key, Some(&value)).await?;
        client.delete(&key).await?;
        eventually(&client, &key, None).await?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;

    operator.terminate()?;
    let stopped = operator.event().await?;
    let exit = operator.wait().await?;
    eprintln!("operator acceptance stopped={stopped}; exit={exit}");
    assert!(exit.success(), "{exit}: {stopped}");
    assert_eq!(stopped["event"], "stopped");
    assert_eq!(stopped["forced"], false);
    let children = stopped["children"]
        .as_array()
        .ok_or("missing exit records")?;
    assert_eq!(children.len(), 3);
    for child in children {
        assert_eq!(child["success"], true, "{child}");
        assert_eq!(child["forced"], false, "{child}");
    }
    for address in [gateway, writer, reader] {
        assert!(
            tokio::net::TcpStream::connect(address).await.is_err(),
            "listener still open: {address}"
        );
    }
    work
}
