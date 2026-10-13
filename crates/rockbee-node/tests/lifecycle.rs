use rockbee_node::Result;
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
    let mut command = Command::new(env!("CARGO_BIN_EXE_rockbee-server"));
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
        .stdin(Stdio::null());
    command
}

fn spawn_lifecycle_child(
    directory: &TempDir,
    role: &str,
    prefix: &str,
) -> Result<(OwnedChild, Receiver<std::io::Result<String>>)> {
    let mut child = OwnedChild {
        child: command(directory, role, prefix, "127.0.0.1:0")
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
    for role in ["writer", "reader"] {
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
        assert!(
            process.stderr()?.is_empty(),
            "banner or error reached piped stderr"
        );
    }
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
