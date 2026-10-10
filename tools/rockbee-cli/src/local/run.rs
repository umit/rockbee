use super::{Result, config::RunConfig};
use serde_json::{Value, json};
use std::fs::{File, create_dir_all};
use std::io::{BufReader, Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::oneshot;
use tokio::time::{Instant, sleep};

const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
const SHUTDOWN_GRACE: Duration = Duration::from_secs(30);
const MAX_READY_LINE: usize = 8 * 1024;

pub(super) async fn run(config: RunConfig) -> Result<()> {
    let paths = Paths::resolve(&config)?;
    let prefix = unique_prefix()?;
    let logs = unique_log_dir()?;
    let mut signals = Signals::register()?;
    let mut children = Vec::with_capacity(3);

    let result = async {
        let writer = spawn(
            "writer",
            &paths.writer,
            [
                "--config".into(),
                path_arg(&config.writer_config)?,
                "--role".into(),
                "writer".into(),
                "--addr".into(),
                "127.0.0.1:0".into(),
                "--backend".into(),
                "localstack".into(),
                "--endpoint".into(),
                config.endpoint.clone(),
                "--bucket".into(),
                config.bucket.clone(),
                "--prefix".into(),
                prefix.clone(),
            ],
            &logs,
        )?;
        children.push(writer);
        let writer_addr = wait_ready(
            children.last_mut().expect("writer exists"),
            "writer",
            &mut signals,
        )
        .await?;

        let reader = spawn(
            "reader",
            &paths.reader,
            [
                "--config".into(),
                path_arg(&config.reader_config)?,
                "--role".into(),
                "reader".into(),
                "--addr".into(),
                "127.0.0.1:0".into(),
                "--backend".into(),
                "localstack".into(),
                "--endpoint".into(),
                config.endpoint.clone(),
                "--bucket".into(),
                config.bucket.clone(),
                "--prefix".into(),
                prefix.clone(),
            ],
            &logs,
        )?;
        children.push(reader);
        let reader_addr = wait_ready(
            children.last_mut().expect("reader exists"),
            "reader",
            &mut signals,
        )
        .await?;

        let gateway = spawn(
            "gateway",
            &paths.gateway,
            [
                "--addr".into(),
                "127.0.0.1:0".into(),
                "--writer".into(),
                writer_addr.clone(),
                "--reader".into(),
                reader_addr.clone(),
            ],
            &logs,
        )?;
        children.push(gateway);
        let gateway_addr = wait_ready(
            children.last_mut().expect("gateway exists"),
            "gateway",
            &mut signals,
        )
        .await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>((writer_addr, reader_addr, gateway_addr))
    }
    .await;

    let (writer_addr, reader_addr, gateway_addr) = match result {
        Ok(topology) => topology,
        Err(error) => {
            let cleanup = cleanup(&mut children).await;
            println!(
                "{}",
                stop_record(error.to_string(), &cleanup, &prefix, &logs)
            );
            return Err(error);
        }
    };

    let pids: Vec<_> = children
        .iter()
        .map(|child| json!({"role": child.role, "pid": child.child.id()}))
        .collect();
    println!(
        "{}",
        json!({
            "event": "ready",
            "writer": writer_addr,
            "readers": [reader_addr],
            "gateway": gateway_addr,
            "pids": pids,
            "prefix": prefix,
            "bucket": config.bucket,
            "endpoint": config.endpoint,
            "logs": logs,
        })
    );

    let unexpected = wait_for_stop(&mut children, &mut signals).await;
    let cleanup = cleanup(&mut children).await;
    println!(
        "{}",
        stop_record(unexpected.clone(), &cleanup, &prefix, &logs)
    );
    if cleanup.forced || cleanup.had_failure || unexpected != "signal" {
        Err(format!("local run stopped: {unexpected}").into())
    } else {
        Ok(())
    }
}

struct Paths {
    writer: PathBuf,
    reader: PathBuf,
    gateway: PathBuf,
}

impl Paths {
    fn resolve(config: &RunConfig) -> Result<Self> {
        for (kind, path) in [
            ("writer config", &config.writer_config),
            ("reader config", &config.reader_config),
        ] {
            if !path.is_file() {
                return Err(format!("{kind} does not exist: {}", path.display()).into());
            }
        }
        let bin_dir = match &config.bin_dir {
            Some(path) => path.clone(),
            None => std::env::current_exe()?
                .parent()
                .ok_or("rockbee-local executable has no parent directory")?
                .to_owned(),
        };
        let paths = Self {
            writer: bin_dir.join("rockbee-writer"),
            reader: bin_dir.join("rockbee-reader"),
            gateway: bin_dir.join("rockbee-gateway"),
        };
        for (kind, path) in [
            ("writer binary", &paths.writer),
            ("reader binary", &paths.reader),
            ("gateway binary", &paths.gateway),
        ] {
            if !path.is_file() {
                return Err(format!("{kind} does not exist: {}", path.display()).into());
            }
        }
        Ok(paths)
    }
}

struct ManagedChild {
    role: &'static str,
    child: Child,
    ready: oneshot::Receiver<Result<String>>,
    forced: bool,
}

impl Drop for ManagedChild {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn spawn(
    role: &'static str,
    binary: &Path,
    args: impl IntoIterator<Item = String>,
    logs: &Path,
) -> Result<ManagedChild> {
    let stdout_log = logs.join(format!("{role}.stdout.log"));
    let stderr_log = logs.join(format!("{role}.stderr.log"));
    let mut child = Command::new(binary)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::from(File::create(stderr_log)?))
        .spawn()?;
    let stdout = child.stdout.take().ok_or("child stdout pipe unavailable")?;
    let (ready_send, ready) = oneshot::channel();
    std::thread::spawn(move || copy_stdout_until_ready(stdout, stdout_log, role, ready_send));
    Ok(ManagedChild {
        role,
        child,
        ready,
        forced: false,
    })
}

fn copy_stdout_until_ready(
    stdout: impl Read,
    stdout_log: PathBuf,
    expected_role: &'static str,
    ready_send: oneshot::Sender<Result<String>>,
) {
    let mut input = BufReader::new(stdout);
    let result = (|| -> Result<String> {
        let mut log = File::create(&stdout_log)?;
        let mut line = Vec::with_capacity(MAX_READY_LINE);
        loop {
            let mut byte = [0_u8; 1];
            let read = input.read(&mut byte)?;
            if read == 0 {
                return Err("child exited before readiness JSON".into());
            }
            line.push(byte[0]);
            if line.len() > MAX_READY_LINE {
                return Err("readiness JSON line exceeds 8 KiB".into());
            }
            if byte[0] == b'\n' {
                break;
            }
        }
        log.write_all(&line)?;
        validate_ready(&line, expected_role)
    })();
    let _ = ready_send.send(result);
    if let Ok(mut log) = File::options().append(true).open(stdout_log) {
        let _ = std::io::copy(&mut input, &mut log);
    }
}

fn validate_ready(line: &[u8], role: &str) -> Result<String> {
    let ready: Value = serde_json::from_slice(line)?;
    let addr = ready["addr"]
        .as_str()
        .ok_or("child readiness JSON has no address")?
        .parse::<SocketAddr>()?;
    if ready["event"] != "ready"
        || ready["role"] != role
        || !addr.ip().is_loopback()
        || addr.port() == 0
    {
        return Err("child readiness JSON has an unexpected event, role, or address".into());
    }
    Ok(addr.to_string())
}

async fn wait_ready(
    child: &mut ManagedChild,
    expected_role: &str,
    signals: &mut Signals,
) -> Result<String> {
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    let address = tokio::select! {
        result = &mut child.ready => result.map_err(|_| "child readiness reader stopped")??,
        _ = signals.wait() => return Err("received shutdown signal during startup".into()),
        _ = tokio::time::sleep_until(deadline) => return Err(format!("{expected_role} readiness timed out").into()),
    };
    if child.child.try_wait()?.is_some() {
        return Err(format!("{expected_role} exited immediately after readiness").into());
    }
    Ok(address)
}

async fn wait_for_stop(children: &mut [ManagedChild], signals: &mut Signals) -> String {
    loop {
        tokio::select! {
            _ = signals.wait() => return "signal".to_owned(),
            _ = sleep(Duration::from_millis(100)) => {}
        }
        for child in children.iter_mut() {
            match child.child.try_wait() {
                Ok(Some(status)) => return format!("{} exited unexpectedly: {status}", child.role),
                Ok(None) => {}
                Err(error) => return format!("could not inspect {}: {error}", child.role),
            }
        }
    }
}

struct Cleanup {
    forced: bool,
    had_failure: bool,
    children: Vec<Value>,
}

fn stop_record(reason: String, cleanup: &Cleanup, prefix: &str, logs: &Path) -> Value {
    json!({
        "event": "stopped",
        "reason": reason,
        "forced": cleanup.forced,
        "children": cleanup.children,
        "prefix": prefix,
        "logs": logs,
    })
}

async fn cleanup(children: &mut [ManagedChild]) -> Cleanup {
    let mut forced = false;
    for child in children.iter_mut().rev() {
        if matches!(child.child.try_wait(), Ok(None)) {
            let pid = child.child.id().to_string();
            let _ = Command::new("/bin/kill").args(["-TERM", &pid]).status();
        }
    }
    let deadline = Instant::now() + SHUTDOWN_GRACE;
    loop {
        let mut active = false;
        for child in children.iter_mut() {
            active |= matches!(child.child.try_wait(), Ok(None));
        }
        if !active {
            break;
        }
        if Instant::now() >= deadline {
            forced = true;
            for child in children.iter_mut().rev() {
                if matches!(child.child.try_wait(), Ok(None)) {
                    child.forced = true;
                    let _ = child.child.kill();
                    let _ = child.child.wait();
                }
            }
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    let mut had_failure = false;
    let children = children
        .iter_mut()
        .map(|child| match child.child.try_wait() {
            Ok(Some(status)) => {
                had_failure |= !status.success();
                json!({
                    "role": child.role,
                    "pid": child.child.id(),
                    "exit": status.to_string(),
                    "success": status.success(),
                    "forced": child.forced,
                })
            }
            Ok(None) => {
                had_failure = true;
                json!({"role": child.role, "pid": child.child.id(), "exit": null, "success": false, "forced": child.forced})
            }
            Err(error) => {
                had_failure = true;
                json!({"role": child.role, "pid": child.child.id(), "inspection_error": error.to_string(), "success": false, "forced": child.forced})
            }
        })
        .collect();
    Cleanup {
        forced,
        had_failure,
        children,
    }
}

fn unique_prefix() -> Result<String> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    Ok(format!("rockbee-local-{}-{nanos}", std::process::id()))
}

fn unique_log_dir() -> Result<PathBuf> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let path = std::env::temp_dir().join(format!("rockbee-local-{}-{nanos}", std::process::id()));
    create_dir_all(&path)?;
    Ok(path)
}

fn path_arg(path: &Path) -> Result<String> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| format!("path is not valid UTF-8: {}", path.display()).into())
}

#[cfg(unix)]
struct Signals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl Signals {
    fn register() -> Result<Self> {
        Ok(Self {
            interrupt: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?,
            terminate: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?,
        })
    }

    async fn wait(&mut self) {
        tokio::select! {
            _ = self.interrupt.recv() => {},
            _ = self.terminate.recv() => {},
        }
    }
}

#[cfg(not(unix))]
struct Signals;

#[cfg(not(unix))]
impl Signals {
    fn register() -> Result<Self> {
        Ok(Self)
    }
    async fn wait(&mut self) {
        let _ = tokio::signal::ctrl_c().await;
    }
}
