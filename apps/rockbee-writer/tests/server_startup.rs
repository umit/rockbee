use rockbee_client::RockBeeClient;
use rockbee_test_support::Result;
use serde_json::Value;
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::time::timeout;

const READY_TIMEOUT: Duration = Duration::from_secs(10);
const EXIT_TIMEOUT: Duration = Duration::from_secs(10);
const OP_TIMEOUT: Duration = Duration::from_secs(6);

struct OwnedWriter {
    child: Child,
}

impl Drop for OwnedWriter {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_some() {
            let _ = self.child.wait();
            return;
        }
        #[cfg(unix)]
        {
            // This test owns this exact child PID. SIGTERM gives Server::run its normal shutdown.
            let _ = Command::new("kill")
                .args(["-TERM", &self.child.id().to_string()])
                .status();
        }
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if self.child.try_wait().ok().flatten().is_some() {
                let _ = self.child.wait();
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_rockbee-writer")
}

fn writer_command(directory: &TempDir, prefix: &str, address: &str) -> Command {
    let mut command = Command::new(binary());
    command
        .args([
            "--role",
            "writer",
            "--backend",
            "file",
            "--directory",
            directory.path().to_str().expect("temporary path is UTF-8"),
            "--prefix",
            prefix,
            "--addr",
            address,
        ])
        .stdin(Stdio::null());
    command
}

fn start_writer(directory: &TempDir, prefix: &str) -> Result<(OwnedWriter, String)> {
    let child = writer_command(directory, prefix, "127.0.0.1:0")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    let mut owned = OwnedWriter { child };
    let stdout = owned
        .child
        .stdout
        .take()
        .ok_or("writer stdout was not piped")?;
    let (sent, received) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let mut line = String::new();
        let result = BufReader::new(stdout).read_line(&mut line).map(|_| line);
        let _ = sent.send(result);
    });
    let line = received
        .recv_timeout(READY_TIMEOUT)
        .map_err(|_| "writer did not print a ready line within ten seconds")??;
    let ready: Value = serde_json::from_str(&line)?;
    if ready.get("event") != Some(&Value::String("ready".into()))
        || ready.get("role") != Some(&Value::String("writer".into()))
    {
        return Err(format!("unexpected writer ready line: {line:?}").into());
    }
    let address = ready["addr"]
        .as_str()
        .ok_or("writer ready address missing")?;
    let parsed: std::net::SocketAddr = address.parse()?;
    if !parsed.ip().is_loopback() || parsed.port() == 0 {
        return Err(format!("invalid writer ready address: {address}").into());
    }
    Ok((owned, address.to_owned()))
}

fn wait_for_output(mut child: Child) -> Result<Output> {
    let deadline = Instant::now() + EXIT_TIMEOUT;
    while Instant::now() < deadline {
        match child.try_wait() {
            Ok(Some(_)) => return Ok(child.wait_with_output()?),
            Ok(None) => {}
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(error.into());
            }
        }
        thread::sleep(Duration::from_millis(10));
    }
    let _ = child.kill();
    let output = child.wait_with_output()?;
    Err(format!(
        "second writer did not exit within ten seconds: stdout={:?}, stderr={:?}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
    .into())
}

async fn durable_put(client: &RockBeeClient, key: &[u8], value: &[u8]) -> Result<()> {
    match timeout(OP_TIMEOUT, client.put(key, value, None)).await {
        Ok(result) => result.map_err(Into::into),
        Err(_) => Err("durable PUT exceeded six seconds".into()),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejected_bind_does_not_fence_an_existing_writer() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let (writer_a, address) = start_writer(&directory, "startup-fence")?;
    let client = RockBeeClient::connect(&address).await?;
    durable_put(&client, b"seed", b"ack-before-second-startup").await?;

    let second = writer_command(&directory, "startup-fence", &address)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let output = wait_for_output(second)?;
    if output.status.success() {
        return Err("second writer unexpectedly exited successfully".into());
    }
    if output
        .stdout
        .windows(b"\"event\":\"ready\"".len())
        .any(|window| window == b"\"event\":\"ready\"")
    {
        return Err("second writer printed ready despite an occupied address".into());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !stderr.contains("AddrInUse") {
        return Err(format!("second writer did not report address-in-use: {stderr:?}").into());
    }

    // Before the fix B opens/fences its Slate writer before bind fails. Give the existing
    // writer more than one default one-second manifest poll to observe that fence, then require it
    // to accept a new durable mutation. This is intentionally a semantic, not throughput, wait.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    durable_put(&client, b"after-rejected-bind", b"writer-a-still-current").await?;
    let value = timeout(OP_TIMEOUT, client.get(b"after-rejected-bind")).await??;
    if value.as_deref() != Some(b"writer-a-still-current".as_slice()) {
        return Err(format!("writer A did not retain its exact value: {value:?}").into());
    }

    drop(writer_a);
    Ok(())
}
