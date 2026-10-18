//! Owns a disposable, local three-process topology for the Criterion executable.

#[cfg(unix)]
mod signals;

use serde_json::json;
use std::env;
use std::fs::{self, File};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub(super) struct LocalTopology {
    address: String,
    report_directory: PathBuf,
    processes: Arc<Mutex<OwnedProcesses>>,
    #[cfg(unix)]
    _signals: signals::ShutdownSignals,
}

struct OwnedProcesses {
    report_directory: PathBuf,
    children: Vec<(&'static str, Child)>,
    stopped: bool,
    interrupted_by: Option<i32>,
}

impl LocalTopology {
    pub(super) fn start() -> Result<Self> {
        let executable = env::current_exe()?;
        let binary_directory = match env::var_os("ROCKBEE_BENCH_BIN_DIR") {
            Some(directory) => PathBuf::from(directory),
            None => executable
                .parent()
                .and_then(Path::parent)
                .ok_or("cannot locate release binaries beside the benchmark executable")?
                .to_path_buf(),
        };
        for role in ["writer", "reader", "gateway"] {
            if !binary_directory.join(format!("rockbee-{role}")).is_file() {
                return Err(format!(
                    "missing rockbee-{role} in {}; first build the three service binaries with cargo build --release",
                    binary_directory.display()
                )
                .into());
            }
        }

        let run_id = uuid::Uuid::now_v7().to_string();
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let report_directory = match env::var_os("ROCKBEE_BENCH_REPORT_DIR") {
            Some(directory) => PathBuf::from(directory),
            None => workspace
                .join("reports")
                .join(format!("criterion-{run_id}")),
        };
        if let Some(parent) = report_directory.parent() {
            fs::create_dir_all(parent)?;
        }
        // Refuse to overwrite an earlier measurement or its storage files.
        fs::create_dir(&report_directory)?;
        let report_directory = report_directory.canonicalize()?;
        let object_directory = report_directory.join("objects");
        fs::create_dir(&object_directory)?;
        let prefix = format!("criterion-{run_id}");
        let metadata = json!({
            "workspace_head": output("git", &["-C", workspace.to_str().ok_or("workspace path is not UTF-8")?, "rev-parse", "HEAD"]),
            "rustc": output("rustc", &["--version"]),
            "os": env::consts::OS,
            "architecture": env::consts::ARCH,
            "cpu": output("sysctl", &["-n", "machdep.cpu.brand_string"]),
            "logical_cpus": thread::available_parallelism().ok().map(usize::from),
            "ram_bytes": output("sysctl", &["-n", "hw.memsize"]),
            "power_before": output("pmset", &["-g", "batt"]),
            "power_settings": output("pmset", &["-g", "custom"]),
            "binary_directory": binary_directory,
            "profile": "release / bench optimized",
            "backend": "local filesystem object store",
            "prefix": prefix,
            "client_connections": 1,
            "gateway_processes": 1,
            "reader_processes": 1,
            "writer_processes": 1,
            "max_in_flight": 128,
            "fixture_keys": 1024,
            "fixture_value_bytes": 1024,
            "warmup_seconds": 10,
            "target_measurement_seconds": 60,
            "criterion_samples": 50,
            "get_requests_per_criterion_iteration": 128,
            "scope": "warm closed-loop GET; all processes share this machine; not cold S3 or a proven capacity limit; Criterion time is not request latency",
            "readiness": {},
            "commands": {},
            "binary_sha256": {},
        });
        fs::write(
            report_directory.join("metadata.json"),
            serde_json::to_vec_pretty(&metadata)?,
        )?;
        eprintln!("Benchmark report directory: {}", report_directory.display());

        let processes = Arc::new(Mutex::new(OwnedProcesses {
            report_directory: report_directory.clone(),
            children: Vec::new(),
            stopped: false,
            interrupted_by: None,
        }));
        // Register before the first spawn, including the wait for writer readiness.
        #[cfg(unix)]
        let signals = signals::ShutdownSignals::install(Arc::clone(&processes))?;
        let mut topology = Self {
            address: String::new(),
            report_directory,
            processes,
            #[cfg(unix)]
            _signals: signals,
        };
        let storage = [
            "--backend".to_owned(),
            "file".to_owned(),
            "--directory".to_owned(),
            object_directory
                .to_str()
                .ok_or("storage path is not UTF-8")?
                .to_owned(),
            "--prefix".to_owned(),
            prefix,
        ];
        let writer = topology.start_role(&binary_directory, "writer", &storage)?;
        let reader = topology.start_role(&binary_directory, "reader", &storage)?;
        topology.address = topology.start_role(
            &binary_directory,
            "gateway",
            &["--writer".to_owned(), writer, "--reader".to_owned(), reader],
        )?;
        Ok(topology)
    }

    pub(super) fn address(&self) -> &str {
        &self.address
    }

    pub(super) fn report_directory(&self) -> &Path {
        &self.report_directory
    }

    /// Preserve real failures, but let a requested shutdown finish before exiting.
    #[allow(clippy::panic)] // A failed request must invalidate Criterion's measurement.
    pub(super) fn benchmark_failure(&self, message: String) -> ! {
        // This runs only on failure. Taking the registry lock waits for signal
        // cleanup, including cleanup.json, without checking every measured GET.
        let interrupted_by = owned_processes(&self.processes).interrupted_by;
        if let Some(signal) = interrupted_by {
            std::process::exit(128 + signal);
        }
        panic!("warm_get benchmark failed: {message}");
    }

    fn start_role(
        &mut self,
        binaries: &Path,
        role: &'static str,
        args: &[String],
    ) -> Result<String> {
        let stdout_path = self.report_directory.join(format!("{role}.stdout.log"));
        let stderr_path = self.report_directory.join(format!("{role}.stderr.log"));
        let binary = binaries.join(format!("rockbee-{role}"));
        {
            // Signal cleanup must not run between spawning and registering a child.
            let mut processes = owned_processes(&self.processes);
            if processes.stopped {
                return Err("benchmark topology is shutting down".into());
            }
            let mut command = Command::new(&binary);
            command
                .args(["--addr", "127.0.0.1:0"])
                .args(args)
                .stdin(Stdio::null())
                .stdout(File::create(&stdout_path)?)
                .stderr(File::create(&stderr_path)?);
            // Terminal Ctrl-C goes to the runner first. It records cancellation
            // before closing service connections through owned-process cleanup.
            #[cfg(unix)]
            command.process_group(0);
            let child = command.spawn()?;
            processes.children.push((role, child));
        }
        let deadline = Instant::now() + Duration::from_secs(90);
        while Instant::now() < deadline {
            let status = {
                let mut processes = owned_processes(&self.processes);
                if processes.stopped {
                    return Err("benchmark topology is shutting down".into());
                }
                processes
                    .children
                    .last_mut()
                    .expect("just added service")
                    .1
                    .try_wait()?
            };
            if let Some(status) = status {
                return Err(
                    format!("{role} exited {status}; see {}", stderr_path.display()).into(),
                );
            }
            for line in fs::read_to_string(&stdout_path)?.lines() {
                let Ok(event) = serde_json::from_str::<serde_json::Value>(line) else {
                    continue;
                };
                if event["event"] == "ready" && event["role"] == role {
                    let address = event["addr"]
                        .as_str()
                        .ok_or("ready event has no address")?
                        .to_owned();
                    let metadata_path = self.report_directory.join("metadata.json");
                    let mut metadata: serde_json::Value =
                        serde_json::from_slice(&fs::read(&metadata_path)?)?;
                    metadata["readiness"][role] = event;
                    metadata["commands"][role] =
                        json!({"binary": binary, "args": args, "listen": "127.0.0.1:0"});
                    metadata["binary_sha256"][role] = json!(
                        binary
                            .to_str()
                            .and_then(|path| output("shasum", &["-a", "256", path]))
                    );
                    fs::write(metadata_path, serde_json::to_vec_pretty(&metadata)?)?;
                    eprintln!("{role} ready at {address}");
                    return Ok(address);
                }
            }
            thread::sleep(Duration::from_millis(100));
        }
        Err(format!("{role} did not become ready within 90 seconds").into())
    }
}

impl Drop for LocalTopology {
    fn drop(&mut self) {
        owned_processes(&self.processes).cleanup();
        // The signal guard is dropped next, outside the process registry lock.
    }
}

impl OwnedProcesses {
    /// Called with the registry locked, through both normal Drop and signal handling.
    fn cleanup(&mut self) {
        if self.stopped {
            return;
        }
        self.stopped = true;
        let mut stopped = Vec::new();
        for (role, child) in self.children.iter_mut().rev() {
            if matches!(child.try_wait(), Ok(None)) {
                #[cfg(unix)]
                {
                    // Ask only the process created and still owned by this runner to shut down.
                    let _ = Command::new("kill")
                        .args(["-TERM", &child.id().to_string()])
                        .status();
                }
                let deadline = Instant::now() + Duration::from_secs(10);
                while matches!(child.try_wait(), Ok(None)) && Instant::now() < deadline {
                    thread::sleep(Duration::from_millis(50));
                }
                if matches!(child.try_wait(), Ok(None)) {
                    let _ = child.kill();
                }
            }
            let status = child.wait();
            stopped.push(json!({"role": role, "pid": child.id(), "exit_status": status.ok().map(|s| s.to_string())}));
        }
        let _ = fs::write(
            self.report_directory.join("cleanup.json"),
            json!({"owned_processes": stopped, "power_after": output("pmset", &["-g", "batt"])})
                .to_string(),
        );
    }
}

fn owned_processes(processes: &Mutex<OwnedProcesses>) -> MutexGuard<'_, OwnedProcesses> {
    // Even a startup panic must leave its owned children available for cleanup.
    processes.lock().unwrap_or_else(|error| error.into_inner())
}

fn output(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}
