#![cfg(unix)]

#[path = "../benches/support/mod.rs"]
#[allow(dead_code)]
mod support;

use std::fs::{self, File};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const BOUND: Duration = Duration::from_secs(45);

// These fixtures only announce readiness and stay alive. Real request timing
// belongs to Criterion; these tests exercise process ownership and interruption.
const SERVICE: &str = r#"
use std::{env, fs, io::{self, Write}, thread, time::Duration};
fn main() {
    if env::args().any(|arg| arg == "--sentinel") {
        loop { thread::sleep(Duration::from_millis(20)); }
    }
    let executable = env::current_exe().unwrap();
    let role = executable.file_name().unwrap().to_str().unwrap()
        .strip_prefix("rockbee-").unwrap();
    let directory = env::var_os("ROCKBEE_SHUTDOWN_FIXTURE").unwrap();
    let directory = std::path::Path::new(&directory);
    let temporary_pid = directory.join(format!("{role}.pid.tmp"));
    fs::write(&temporary_pid, std::process::id().to_string()).unwrap();
    fs::rename(temporary_pid, directory.join(format!("{role}.pid"))).unwrap();
    if !(role == "writer" && env::var_os("ROCKBEE_FIXTURE_HOLD_WRITER").is_some()) {
        println!("{{\"event\":\"ready\",\"role\":\"{role}\",\"addr\":\"127.0.0.1:1\"}}");
        io::stdout().flush().unwrap();
    }
    loop { thread::sleep(Duration::from_millis(20)); }
}
"#;

struct Fixture {
    directory: PathBuf,
    runner: Option<Child>,
    sentinel: Option<Child>,
}

impl Fixture {
    fn start(hold_writer: bool) -> Result<Self> {
        let directory =
            std::env::temp_dir().join(format!("rockbee-bench-shutdown-{}", uuid::Uuid::now_v7()));
        fs::create_dir(&directory)?;
        let mut fixture = Self {
            directory,
            runner: None,
            sentinel: None,
        };
        let source = fixture.directory.join("service.rs");
        let binary = fixture.directory.join("service");
        fs::write(&source, SERVICE)?;
        let compiled = Command::new("rustc")
            .args(["--edition=2024", "--crate-name", "shutdown_fixture"])
            .arg(&source)
            .arg("-o")
            .arg(&binary)
            .output()?;
        assert!(
            compiled.status.success(),
            "fixture compilation failed: {}",
            String::from_utf8_lossy(&compiled.stderr)
        );
        let binaries = fixture.directory.join("bin");
        fs::create_dir(&binaries)?;
        for role in ["writer", "reader", "gateway"] {
            fs::copy(&binary, binaries.join(format!("rockbee-{role}")))?;
        }
        fixture.sentinel = Some(
            Command::new(&binary)
                .arg("--sentinel")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()?,
        );
        let mut command = Command::new(std::env::current_exe()?);
        command
            .args(["--ignored", "--exact", "topology_helper", "--nocapture"])
            .env("ROCKBEE_SHUTDOWN_FIXTURE", &fixture.directory)
            .env("ROCKBEE_BENCH_BIN_DIR", &binaries)
            .env("ROCKBEE_BENCH_REPORT_DIR", fixture.directory.join("report"))
            .env_remove("ROCKBEE_FIXTURE_HOLD_WRITER")
            .stdin(Stdio::null())
            .stdout(File::create(fixture.directory.join("helper.stdout"))?)
            .stderr(File::create(fixture.directory.join("helper.stderr"))?);
        // Keep the test helper and its inherited children in a private group so
        // a negative-PGID signal below cannot reach this test process.
        command.process_group(0);
        if hold_writer {
            command.env("ROCKBEE_FIXTURE_HOLD_WRITER", "1");
        }
        fixture.runner = Some(command.spawn()?);
        fixture.wait_for_file(if hold_writer { "writer.pid" } else { "ready" })?;
        Ok(fixture)
    }

    fn wait_for_file(&mut self, name: &str) -> Result<()> {
        let deadline = Instant::now() + BOUND;
        while !self.directory.join(name).is_file() {
            if let Some(status) = self.runner.as_mut().expect("runner exists").try_wait()? {
                return Err(format!("helper exited {status}: {}", self.helper_log()).into());
            }
            if Instant::now() >= deadline {
                return Err(format!("timed out waiting for {name}: {}", self.helper_log()).into());
            }
            thread::sleep(Duration::from_millis(20));
        }
        Ok(())
    }

    fn helper_log(&self) -> String {
        fs::read_to_string(self.directory.join("helper.stderr")).unwrap_or_default()
    }

    fn signal(&self, signal: &str) -> Result<()> {
        let pid = self.runner.as_ref().expect("runner exists").id();
        assert!(
            send_signal(pid, signal)?,
            "could not send {signal} to helper"
        );
        Ok(())
    }

    fn signal_runner_group(&self, signal: &str) -> Result<()> {
        let pid = self.runner.as_ref().expect("runner exists").id();
        assert_eq!(
            process_group(pid)?,
            pid,
            "runner does not own the private process group"
        );
        assert!(
            send_signal_to_group(pid, signal)?,
            "could not send {signal} to helper process group {pid}"
        );
        Ok(())
    }

    fn wait(&mut self) -> Result<ExitStatus> {
        let deadline = Instant::now() + BOUND;
        loop {
            if let Some(status) = self.runner.as_mut().expect("runner exists").try_wait()? {
                return Ok(status);
            }
            if Instant::now() >= deadline {
                return Err(format!("helper did not exit: {}", self.helper_log()).into());
            }
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn assert_cleanup(&mut self, roles: &[&str]) -> Result<()> {
        let cleanup: serde_json::Value =
            serde_json::from_slice(&fs::read(self.directory.join("report/cleanup.json"))?)?;
        let children = cleanup["owned_processes"]
            .as_array()
            .ok_or("cleanup report has no owned_processes array")?;
        assert_eq!(children.len(), roles.len(), "{cleanup}");
        for role in roles {
            let pid = read_pid(&self.directory.join(format!("{role}.pid")))?;
            assert!(
                children.iter().any(|child| {
                    child["role"].as_str() == Some(role)
                        && child["pid"].as_u64() == Some(u64::from(pid))
                }),
                "cleanup omitted {role} PID {pid}: {cleanup}"
            );
            assert!(!process_exists(pid)?, "{role} PID {pid} survived cleanup");
        }
        assert!(
            self.sentinel
                .as_mut()
                .expect("sentinel exists")
                .try_wait()?
                .is_none(),
            "cleanup stopped an unrelated process"
        );
        Ok(())
    }

    fn assert_cleanup_used_sigterm(&self, roles: &[&str]) -> Result<()> {
        let cleanup: serde_json::Value =
            serde_json::from_slice(&fs::read(self.directory.join("report/cleanup.json"))?)?;
        let children = cleanup["owned_processes"]
            .as_array()
            .ok_or("cleanup report has no owned_processes array")?;
        let expected_status = ExitStatus::from_raw(15).to_string();
        for role in roles {
            let status = children
                .iter()
                .find(|child| child["role"].as_str() == Some(*role))
                .and_then(|child| child["exit_status"].as_str());
            assert_eq!(
                status,
                Some(expected_status.as_str()),
                "{role} was not stopped by the benchmark supervisor: {cleanup}"
            );
        }
        Ok(())
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // Failure cleanup is independent of LocalTopology so the red regression
        // run cannot leave services behind after its unhandled signal.
        if let Some(runner) = &mut self.runner {
            if matches!(runner.try_wait(), Ok(None)) {
                let _ = runner.kill();
            }
            let _ = runner.wait();
        }
        for role in ["gateway", "reader", "writer"] {
            let Ok(pid) = read_pid(&self.directory.join(format!("{role}.pid"))) else {
                continue;
            };
            // A reaped PID could be reused: check this fixture's unique executable
            // path before sending a fallback signal to a grandchild.
            let expected = self.directory.join("bin").join(format!("rockbee-{role}"));
            if owned_process(pid, &expected) {
                let _ = send_signal(pid, "-KILL");
                let deadline = Instant::now() + Duration::from_secs(2);
                while owned_process(pid, &expected) && Instant::now() < deadline {
                    thread::sleep(Duration::from_millis(20));
                }
            }
        }
        if let Some(sentinel) = &mut self.sentinel {
            if matches!(sentinel.try_wait(), Ok(None)) {
                let _ = sentinel.kill();
            }
            let _ = sentinel.wait();
        }
        let _ = fs::remove_dir_all(&self.directory);
    }
}

fn read_pid(path: &Path) -> Result<u32> {
    Ok(fs::read_to_string(path)?.trim().parse()?)
}

fn send_signal(pid: u32, signal: &str) -> Result<bool> {
    Ok(Command::new("/bin/kill")
        .args([signal, &pid.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?
        .success())
}

fn send_signal_to_group(pgid: u32, signal: &str) -> Result<bool> {
    Ok(Command::new("/bin/kill")
        .args([signal, &format!("-{pgid}")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?
        .success())
}

fn process_group(pid: u32) -> Result<u32> {
    let output = Command::new("ps")
        .args(["-o", "pgid=", "-p", &pid.to_string()])
        .output()?;
    if !output.status.success() {
        return Err(format!("could not read process group for {pid}").into());
    }
    Ok(String::from_utf8(output.stdout)?.trim().parse()?)
}

fn process_exists(pid: u32) -> Result<bool> {
    send_signal(pid, "-0")
}

fn owned_process(pid: u32, expected: &Path) -> bool {
    Command::new("ps")
        .args(["-ww", "-p", &pid.to_string(), "-o", "command="])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .is_some_and(|output| {
            String::from_utf8_lossy(&output.stdout).trim() == expected.to_string_lossy()
                || String::from_utf8_lossy(&output.stdout)
                    .trim()
                    .starts_with(&format!("{} ", expected.display()))
        })
}

#[test]
#[ignore = "subprocess helper for the shutdown regression tests"]
fn topology_helper() -> Result<()> {
    let directory = PathBuf::from(
        std::env::var_os("ROCKBEE_SHUTDOWN_FIXTURE")
            .ok_or("helper requires its fixture environment")?,
    );
    let topology = support::LocalTopology::start()?;
    fs::write(directory.join("ready"), b"ready")?;
    loop {
        if directory.join("fail-now").exists() {
            topology.benchmark_failure("expected request failure".to_owned());
        }
        if directory.join("drop-now").exists() {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    drop(topology);
    Ok(())
}

#[test]
fn sigterm_during_writer_startup_cleans_owned_child() -> Result<()> {
    let mut fixture = Fixture::start(true)?;
    fixture.signal("-TERM")?;
    let status = fixture.wait()?;
    assert_eq!(status.code(), Some(143), "{status}");
    assert_no_benchmark_panic(&fixture);
    fixture.assert_cleanup(&["writer"])
}

#[test]
fn sigterm_during_active_run_cleans_all_owned_children() -> Result<()> {
    let mut fixture = Fixture::start(false)?;
    fixture.signal("-TERM")?;
    let status = fixture.wait()?;
    assert_eq!(status.code(), Some(143), "{status}");
    assert_no_benchmark_panic(&fixture);
    fixture.assert_cleanup(&["writer", "reader", "gateway"])
}

#[test]
fn sigint_during_active_run_cleans_all_owned_children() -> Result<()> {
    let mut fixture = Fixture::start(false)?;
    fixture.signal("-INT")?;
    let status = fixture.wait()?;
    assert_eq!(status.code(), Some(130), "{status}");
    assert_no_benchmark_panic(&fixture);
    fixture.assert_cleanup(&["writer", "reader", "gateway"])
}

#[test]
fn ctrl_c_to_runner_group_is_supervised_before_children_stop() -> Result<()> {
    let mut fixture = Fixture::start(false)?;
    fixture.signal_runner_group("-INT")?;
    let status = fixture.wait()?;
    assert_eq!(status.code(), Some(130), "{status}");
    assert_no_benchmark_panic(&fixture);
    let roles = ["writer", "reader", "gateway"];
    fixture.assert_cleanup(&roles)?;
    fixture.assert_cleanup_used_sigterm(&roles)
}

#[test]
fn normal_drop_cleans_all_owned_children() -> Result<()> {
    let mut fixture = Fixture::start(false)?;
    fs::write(fixture.directory.join("drop-now"), b"drop")?;
    let status = fixture.wait()?;
    assert!(status.success(), "{status}: {}", fixture.helper_log());
    fixture.assert_cleanup(&["writer", "reader", "gateway"])
}

#[test]
fn request_failure_without_signal_remains_visible_and_cleans_owned_children() -> Result<()> {
    let mut fixture = Fixture::start(false)?;
    fs::write(fixture.directory.join("fail-now"), b"fail")?;
    let status = fixture.wait()?;
    assert_eq!(status.code(), Some(101), "{status}");
    assert!(
        fixture
            .helper_log()
            .contains("warm_get benchmark failed: expected request failure"),
        "expected the helper to preserve the request failure: {}",
        fixture.helper_log()
    );
    fixture.assert_cleanup(&["writer", "reader", "gateway"])
}

fn assert_no_benchmark_panic(fixture: &Fixture) {
    assert!(
        !fixture.helper_log().contains("warm_get benchmark failed:"),
        "signal shutdown printed a benchmark panic: {}",
        fixture.helper_log()
    );
}
