use rockbee_test_support::Result;
use serde_json::Value;
use std::io::{BufRead, BufReader, Read};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

const EVENT_TIMEOUT: Duration = Duration::from_secs(10);

struct OwnedChild {
    child: Child,
}

impl OwnedChild {
    fn terminate(&mut self) -> Result<()> {
        #[cfg(unix)]
        let signal = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()?;
        #[cfg(unix)]
        if !signal.success() {
            return Err(format!("SIGTERM command failed with {signal}").into());
        }
        let status = wait_for_exit(&mut self.child)?;
        if !status.success() {
            return Err(format!("node did not exit cleanly after SIGTERM: {status}").into());
        }
        Ok(())
    }

    fn stderr(&mut self) -> Result<String> {
        let mut stderr = String::new();
        self.child
            .stderr
            .take()
            .ok_or("missing node stderr")?
            .read_to_string(&mut stderr)?;
        Ok(stderr)
    }
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_some() {
            let _ = self.child.wait();
            return;
        }
        #[cfg(unix)]
        {
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

fn command(directory: &TempDir, role: &str, prefix: &str, address: &str) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_rockbee-reader"));
    command
        .args([
            "--role",
            role,
            "--backend",
            "file",
            "--directory",
            directory.path().to_str().expect("temporary path is UTF-8"),
            "--prefix",
            prefix,
            "--addr",
            address,
        ])
        // Each test owns its logging configuration instead of inheriting the
        // developer's shell setting.
        .env_remove("RUST_LOG")
        .stdin(Stdio::null());
    command
}

fn spawn_lifecycle_child(
    directory: &TempDir,
    role: &str,
    prefix: &str,
) -> Result<(OwnedChild, Receiver<std::io::Result<String>>)> {
    spawn_lifecycle_child_with_rust_log(directory, role, prefix, None)
}

fn spawn_lifecycle_child_with_rust_log(
    directory: &TempDir,
    role: &str,
    prefix: &str,
    rust_log: Option<&str>,
) -> Result<(OwnedChild, Receiver<std::io::Result<String>>)> {
    let mut command = command(directory, role, prefix, "127.0.0.1:0");
    if let Some(rust_log) = rust_log {
        command.env("RUST_LOG", rust_log);
    }
    let mut child = OwnedChild {
        child: command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?,
    };
    let stdout = child.child.stdout.take().ok_or("missing node stdout")?;
    let (sent, received) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            match line {
                Ok(line) => {
                    if sent.send(Ok(line)).is_err() {
                        break;
                    }
                }
                Err(error) => {
                    let _ = sent.send(Err(error));
                    break;
                }
            }
        }
    });
    Ok((child, received))
}

fn next_event(events: &Receiver<std::io::Result<String>>) -> Result<Value> {
    let line = events
        .recv_timeout(EVENT_TIMEOUT)
        .map_err(|_| "node did not emit a lifecycle event")??;
    Ok(serde_json::from_str(&line)?)
}

fn wait_for_exit(child: &mut Child) -> Result<std::process::ExitStatus> {
    let deadline = Instant::now() + EVENT_TIMEOUT;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            return Err("node did not exit after SIGTERM".into());
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn assert_no_more_events(events: &Receiver<std::io::Result<String>>) -> Result<()> {
    match events.recv_timeout(Duration::from_millis(100)) {
        Ok(Ok(line)) => Err(format!("unexpected extra lifecycle event: {line:?}").into()),
        Ok(Err(error)) => Err(error.into()),
        Err(mpsc::RecvTimeoutError::Timeout | mpsc::RecvTimeoutError::Disconnected) => Ok(()),
    }
}

#[cfg(unix)]
#[test]
fn reader_emits_ready_then_stopped_for_sigterm() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        let objects = rockbee_test_support::backend::Backend::File {
            directory: directory.path().into(),
        }
        .connect()?;
        let writer = rockbee_test_support::Store::open(
            rockbee_test_support::storage::Role::Writer,
            "lifecycle",
            objects,
        )
        .await?;
        writer.close().await
    })?;
    {
        let role = "reader";
        let (mut process, events) = spawn_lifecycle_child(&directory, role, "lifecycle")?;
        let ready = next_event(&events)?;
        assert_eq!(ready["event"], "ready");
        assert_eq!(ready["role"], role);

        process.terminate()?;
        let stopped = next_event(&events)?;
        assert_eq!(stopped["event"], "stopped");
        assert_eq!(stopped["role"], role);
        assert_eq!(stopped["addr"], ready["addr"]);
        assert_eq!(stopped["phase"], "serving");
        assert_eq!(stopped["reason"], "shutdown_signal");
        assert!(stopped["uptime_ms"].is_u64());
        assert_no_more_events(&events)?;
        let stderr = process.stderr()?;
        assert!(
            stderr.contains("service ready"),
            "missing ready log: {stderr}"
        );
        assert!(
            stderr.contains("service stopped"),
            "missing stopped log: {stderr}"
        );
        assert!(
            !stderr.contains("\u{1b}["),
            "ANSI reached piped stderr: {stderr}"
        );
        assert!(
            !stderr.contains("ROCKBEE"),
            "terminal banner reached piped stderr: {stderr}"
        );
    }
    Ok(())
}
