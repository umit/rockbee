use rockbee_test_support::Result;
use serde_json::Value;
use std::io::{BufRead, BufReader, Read};
use std::net::TcpListener;
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
    let mut command = Command::new(env!("CARGO_BIN_EXE_rockbee-writer"));
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
fn writer_and_reader_emit_ready_then_stopped_for_sigterm() -> Result<()> {
    let directory = tempfile::tempdir()?;
    {
        let role = "writer";
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

#[cfg(unix)]
#[test]
fn node_rust_log_off_mutes_diagnostics_without_changing_lifecycle_stdout() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let (mut process, events) =
        spawn_lifecycle_child_with_rust_log(&directory, "writer", "log-off", Some("off"))?;
    let ready = next_event(&events)?;
    assert_eq!(ready["event"], "ready");
    assert_eq!(ready["role"], "writer");
    process.terminate()?;
    let stopped = next_event(&events)?;
    assert_eq!(stopped["event"], "stopped");
    assert_no_more_events(&events)?;
    assert!(
        process.stderr()?.is_empty(),
        "RUST_LOG=off emitted diagnostics"
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn node_forwards_slatedb_info_when_only_slatedb_is_enabled() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let (mut process, events) = spawn_lifecycle_child_with_rust_log(
        &directory,
        "writer",
        "slatedb-info",
        Some("off,slatedb=info"),
    )?;

    let ready = next_event(&events)?;
    assert_eq!(ready["event"], "ready");
    assert_eq!(ready["role"], "writer");
    process.terminate()?;
    let stopped = next_event(&events)?;
    assert_eq!(stopped["event"], "stopped");
    assert_no_more_events(&events)?;

    let stderr = process.stderr()?;
    assert!(
        stderr.contains("db closed"),
        "missing SlateDB close event: {stderr}"
    );
    assert!(
        !stderr.contains("service ready"),
        "Rockbee diagnostics ignored RUST_LOG target filter: {stderr}"
    );
    assert!(
        !stderr.contains("service stopped"),
        "Rockbee diagnostics ignored RUST_LOG target filter: {stderr}"
    );
    Ok(())
}

#[test]
fn writer_bind_failure_emits_failed_without_ready() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let occupied = TcpListener::bind("127.0.0.1:0")?;
    let address = occupied.local_addr()?.to_string();
    let mut child = command(&directory, "writer", "failed-startup", &address)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let status = wait_for_exit(&mut child)?;
    assert!(!status.success());
    let mut stdout = String::new();
    child
        .stdout
        .take()
        .ok_or("missing node stdout")?
        .read_to_string(&mut stdout)?;
    let events: Vec<Value> = stdout
        .lines()
        .map(serde_json::from_str)
        .collect::<std::result::Result<_, _>>()?;
    assert_eq!(events.len(), 1, "unexpected lifecycle events: {events:?}");
    let failed = &events[0];
    assert_eq!(failed["event"], "failed");
    assert_eq!(failed["role"], "writer");
    assert_eq!(failed["addr"], address);
    assert_eq!(failed["phase"], "startup");
    assert!(
        failed["error"]
            .as_str()
            .is_some_and(|error| !error.is_empty())
    );
    assert!(events.iter().all(|event| event["event"] != "ready"));
    assert!(events.iter().all(|event| event["event"] != "stopped"));
    Ok(())
}

#[test]
fn node_rejects_invalid_rust_log_before_binding() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let occupied = TcpListener::bind("127.0.0.1:0")?;
    let address = occupied.local_addr()?.to_string();
    let mut child = OwnedChild {
        child: command(&directory, "writer", "invalid-filter", &address)
            .env("RUST_LOG", "rockbee=bogus")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?,
    };
    let status = wait_for_exit(&mut child.child)?;
    assert!(!status.success());

    let mut stdout = String::new();
    child
        .child
        .stdout
        .take()
        .ok_or("missing node stdout")?
        .read_to_string(&mut stdout)?;
    assert!(
        stdout.is_empty(),
        "invalid RUST_LOG emitted stdout: {stdout}"
    );
    let mut stderr = String::new();
    child
        .child
        .stderr
        .take()
        .ok_or("missing node stderr")?
        .read_to_string(&mut stderr)?;
    assert!(
        stderr.contains("RUST_LOG"),
        "missing filter error: {stderr}"
    );
    Ok(())
}

#[test]
fn node_help_and_check_config_do_not_parse_rust_log() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let help = Command::new(env!("CARGO_BIN_EXE_rockbee-writer"))
        .arg("--help")
        .env("RUST_LOG", "rockbee=bogus")
        .output()?;
    assert!(help.status.success(), "help failed: {help:?}");

    let check_config = command(&directory, "writer", "invalid-filter-help", "127.0.0.1:0")
        .arg("--check-config")
        .env("RUST_LOG", "rockbee=bogus")
        .output()?;
    assert!(
        check_config.status.success(),
        "check-config failed: {check_config:?}"
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn writer_exits_cleanly_when_the_readiness_pipe_is_closed() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut process = OwnedChild {
        child: command(&directory, "writer", "closed-pipe", "127.0.0.1:0")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?,
    };
    let stdout = process.child.stdout.take().ok_or("missing writer stdout")?;
    let (sent, received) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let mut line = String::new();
        let result = BufReader::new(stdout).read_line(&mut line).map(|_| line);
        let _ = sent.send(result);
    });
    let ready_line = received
        .recv_timeout(EVENT_TIMEOUT)
        .map_err(|_| "writer did not emit ready before the pipe was closed")??;
    let ready: Value = serde_json::from_str(&ready_line)?;
    assert_eq!(ready["event"], "ready");
    process.terminate()?;
    Ok(())
}
