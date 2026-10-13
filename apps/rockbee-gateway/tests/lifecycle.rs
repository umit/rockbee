use rockbee_protocol::Result;
use serde_json::Value;
use std::io::{BufRead, BufReader, Read};
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant};

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
            return Err(format!("gateway did not exit cleanly after SIGTERM: {status}").into());
        }
        Ok(())
    }

    fn stderr(&mut self) -> Result<String> {
        let mut stderr = String::new();
        self.child
            .stderr
            .take()
            .ok_or("missing gateway stderr")?
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

fn command(address: &str) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_rockbee-gateway"));
    command
        .args([
            "--addr",
            address,
            "--writer",
            "127.0.0.1:1",
            "--reader",
            "127.0.0.1:2",
        ])
        .stdin(Stdio::null());
    command
}

fn spawn_lifecycle_child(address: &str) -> Result<(OwnedChild, Receiver<std::io::Result<String>>)> {
    let mut child = OwnedChild {
        child: command(address)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?,
    };
    let stdout = child.child.stdout.take().ok_or("missing gateway stdout")?;
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
        .map_err(|_| "gateway did not emit a lifecycle event")??;
    Ok(serde_json::from_str(&line)?)
}

fn wait_for_exit(child: &mut Child) -> Result<std::process::ExitStatus> {
    let deadline = Instant::now() + EVENT_TIMEOUT;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            return Err("gateway did not exit after SIGTERM".into());
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
fn gateway_emits_ready_then_stopped_for_sigterm() -> Result<()> {
    let (mut process, events) = spawn_lifecycle_child("127.0.0.1:0")?;
    let ready = next_event(&events)?;
    assert_eq!(ready["event"], "ready");
    assert_eq!(ready["role"], "gateway");

    process.terminate()?;
    let stopped = next_event(&events)?;
    assert_eq!(stopped["event"], "stopped");
    assert_eq!(stopped["role"], "gateway");
    assert_eq!(stopped["addr"], ready["addr"]);
    assert_eq!(stopped["phase"], "serving");
    assert_eq!(stopped["reason"], "shutdown_signal");
    assert!(stopped["uptime_ms"].is_u64());
    assert_no_more_events(&events)?;
    assert!(
        process.stderr()?.is_empty(),
        "banner or error reached piped stderr"
    );
    Ok(())
}

#[test]
fn gateway_bind_failure_emits_failed_without_ready() -> Result<()> {
    let occupied = TcpListener::bind("127.0.0.1:0")?;
    let address = occupied.local_addr()?.to_string();
    let mut child = command(&address)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let status = wait_for_exit(&mut child)?;
    assert!(!status.success());
    let mut stdout = String::new();
    child
        .stdout
        .take()
        .ok_or("missing gateway stdout")?
        .read_to_string(&mut stdout)?;
    let events: Vec<Value> = stdout
        .lines()
        .map(serde_json::from_str)
        .collect::<std::result::Result<_, _>>()?;
    assert_eq!(events.len(), 1, "unexpected lifecycle events: {events:?}");
    let failed = &events[0];
    assert_eq!(failed["event"], "failed");
    assert_eq!(failed["role"], "gateway");
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
