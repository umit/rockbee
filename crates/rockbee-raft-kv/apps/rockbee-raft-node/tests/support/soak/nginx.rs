//! Opt-in real-process topology: clients -> Nginx -> three gateways -> three nodes.
//!
//! Nginx runs in a single foreground process for exact child ownership. This is
//! a local correctness/load experiment, not a production deployment template.

use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    net::{SocketAddr, TcpListener as PortReservation},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};

use bytes::Bytes;
use rockbee_raft_client::{ClientConfig, RockBeeRaftClient};
use tokio::{
    signal::unix::{SignalKind, signal},
    sync::watch,
    time::{Instant, sleep, timeout, timeout_at},
};

use super::{
    ProcessCluster, SoakConfig, StopReason, connect_and_warm_clients, create_report_directory,
    measure, request_stop, write_report,
};

/// Holds the exact child handle from spawn through shutdown, including panic paths.
struct OwnedProcess {
    role: String,
    child: Child,
}

impl OwnedProcess {
    fn start(role: &str, command: &mut Command, directory: &Path) -> Result<Self, String> {
        let log = File::create(directory.join(format!("{role}.log")))
            .map_err(|error| error.to_string())?;
        let child = command
            .stdin(Stdio::null())
            .stdout(log.try_clone().map_err(|error| error.to_string())?)
            .stderr(log)
            .spawn()
            .map_err(|error| format!("start {role}: {error}"))?;
        Ok(Self {
            role: role.into(),
            child,
        })
    }

    fn ensure_running(&mut self) -> Result<(), String> {
        match self.child.try_wait().map_err(|error| error.to_string())? {
            None => Ok(()),
            Some(status) => Err(format!("{} exited unexpectedly: {status}", self.role)),
        }
    }

    async fn close(&mut self) -> Result<(), String> {
        self.ensure_running()?;
        let sent = Command::new("/bin/kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .map_err(|error| error.to_string())?;
        if !sent.success() {
            return Err(format!("{} SIGTERM failed: {sent}", self.role));
        }
        let status = timeout(Duration::from_secs(30), async {
            loop {
                if let Some(status) = self.child.try_wait().map_err(|error| error.to_string())? {
                    return Ok::<_, String>(status);
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .map_err(|_| format!("{} shutdown exceeded 30 seconds", self.role))??;
        if status.success() {
            Ok(())
        } else {
            Err(format!("{} exited unsuccessfully: {status}", self.role))
        }
    }
}

impl Drop for OwnedProcess {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

fn executable(name: &str) -> Result<PathBuf, String> {
    let path = PathBuf::from(std::env::var(name).map_err(|error| format!("{name}: {error}"))?);
    let path = path
        .canonicalize()
        .map_err(|error| format!("{name}: {error}"))?;
    let metadata = path.metadata().map_err(|error| error.to_string())?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o111 == 0 {
        return Err(format!("{name} must name an executable file"));
    }
    Ok(path)
}

struct ProxyTopology {
    // Insertion order is gateways then proxy; reverse cleanup stops proxy first.
    processes: Vec<OwnedProcess>,
    gateway_addresses: Vec<SocketAddr>,
    address: SocketAddr,
    directory: PathBuf,
}

impl ProxyTopology {
    fn new(directory: &Path) -> Self {
        Self {
            processes: Vec::new(),
            gateway_addresses: Vec::new(),
            address: "127.0.0.1:0".parse().unwrap(),
            directory: directory.to_owned(),
        }
    }

    fn record_child(&self, role: &str, pid: u32, address: SocketAddr) -> Result<(), String> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.directory.join("topology.txt"))
            .map_err(|error| error.to_string())?;
        writeln!(file, "{role}={pid}@{address}").map_err(|error| error.to_string())
    }

    async fn start(
        &mut self,
        cluster: &ProcessCluster,
        config: SoakConfig,
        gateway_bin: &Path,
        nginx_bin: &Path,
        stop: &watch::Sender<StopReason>,
    ) -> Result<(), String> {
        for index in 1..=3 {
            check_running(stop)?;
            let port = PortReservation::bind("127.0.0.1:0").map_err(|error| error.to_string())?;
            let address = port.local_addr().map_err(|error| error.to_string())?;
            let role = format!("gateway_{index}");
            let mut command = Command::new(gateway_bin);
            command.args([
                "--listen",
                &address.to_string(),
                "--cluster-id",
                "09090909090909090909090909090909",
                "--shards",
                "4",
                "--hash-seed",
                "0",
            ]);
            command
                .arg("--max-connections")
                .arg(config.gateway_server.max_connections.to_string());
            command
                .arg("--server-request-mib")
                .arg((config.gateway_server.request_bytes / 1024 / 1024).to_string());
            command
                .arg("--server-response-mib")
                .arg((config.gateway_server.response_bytes / 1024 / 1024).to_string());
            for node in &cluster.directory.nodes {
                command
                    .arg("--seed")
                    .arg(format!("{}@{}", node.node_id.get(), node.data_address));
            }
            drop(port);
            self.processes
                .push(OwnedProcess::start(&role, &mut command, &self.directory)?);
            self.gateway_addresses.push(address);
            let pid = self.processes.last().unwrap().child.id();
            self.record_child(&role, pid, address)?;
            wait_until_readable(self.processes.last_mut().unwrap(), address, stop).await?;
        }
        check_running(stop)?;
        let port = PortReservation::bind("127.0.0.1:0").map_err(|error| error.to_string())?;
        self.address = port.local_addr().map_err(|error| error.to_string())?;
        let config_path = self.directory.join("nginx.conf");
        fs::write(
            &config_path,
            nginx_config(self.address, &self.gateway_addresses),
        )
        .map_err(|error| error.to_string())?;
        let check = Command::new(nginx_bin)
            .arg("-p")
            .arg(&self.directory)
            .arg("-c")
            .arg(&config_path)
            .arg("-t")
            .output()
            .map_err(|error| format!("validate nginx config: {error}"))?;
        fs::write(self.directory.join("nginx-check.log"), &check.stderr)
            .map_err(|error| error.to_string())?;
        if !check.status.success() {
            return Err(format!("nginx config rejected: {}", check.status));
        }
        check_running(stop)?;
        let mut command = Command::new(nginx_bin);
        command
            .arg("-p")
            .arg(&self.directory)
            .arg("-c")
            .arg(&config_path);
        drop(port);
        self.processes
            .push(OwnedProcess::start("nginx", &mut command, &self.directory)?);
        let pid = self.processes.last().unwrap().child.id();
        self.record_child("nginx", pid, self.address)?;
        wait_until_readable(self.processes.last_mut().unwrap(), self.address, stop).await?;
        Ok(())
    }

    async fn close(&mut self, statuses: &mut Vec<String>) {
        for child in self.processes.iter_mut().rev() {
            let result = child.close().await;
            statuses.push(format!(
                "{}={}",
                child.role,
                result.map_or_else(|error| error, |()| "ok".into())
            ));
        }
        self.processes.clear(); // Drop also reaps a child whose graceful close failed.
    }
}

impl Drop for ProxyTopology {
    fn drop(&mut self) {
        while self.processes.pop().is_some() {}
    }
}

fn check_running(stop: &watch::Sender<StopReason>) -> Result<(), String> {
    if *stop.borrow() == StopReason::Running {
        Ok(())
    } else {
        Err("stopped during ingress startup".into())
    }
}

async fn wait_until_readable(
    process: &mut OwnedProcess,
    address: SocketAddr,
    stop: &watch::Sender<StopReason>,
) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        check_running(stop)?;
        process.ensure_running()?;
        let connected = timeout_at(
            deadline,
            RockBeeRaftClient::connect(address, ClientConfig::default()),
        )
        .await
        .map_err(|_| format!("{} connect readiness exceeded deadline", process.role))?;
        if let Ok(client) = connected {
            let result = timeout_at(
                deadline,
                client.get(Bytes::from_static(b"nginx-soak/readiness")),
            )
            .await;
            client.close().await;
            match result
                .map_err(|_| format!("{} read readiness exceeded deadline", process.role))?
            {
                Ok(None) => return Ok(()),
                Ok(Some(_)) => return Err("fresh ingress readiness key exists".into()),
                Err(_) => {}
            }
        }
        if Instant::now() >= deadline {
            return Err(format!("{} readiness exceeded deadline", process.role));
        }
        sleep(Duration::from_millis(20)).await;
    }
}

fn nginx_config(address: SocketAddr, gateways: &[SocketAddr]) -> String {
    let servers = gateways
        .iter()
        .map(|address| format!("        server {address};\n"))
        .collect::<String>();
    format!(
        r#"daemon off;
master_process off;
pid nginx.pid;
error_log nginx-error.log info;
events {{ worker_connections 1024; }}
stream {{
    log_format sessions '$remote_addr:$remote_port|$upstream_addr|$status|$bytes_received|$bytes_sent|$session_time';
    access_log nginx-access.log sessions;
    upstream rockbee_gateways {{
        least_conn;
{servers}    }}
    server {{
        listen {address};
        proxy_connect_timeout 5s;
        proxy_timeout 60s;
        proxy_next_upstream off;
        proxy_pass rockbee_gateways;
    }}
}}
"#
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit gateway/nginx executables and owns seven localhost processes"]
async fn three_gateway_soak() {
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_micros()
        .is_test(true)
        .try_init();
    let gateway_bin = executable("ROCKBEE_SOAK_GATEWAY_BIN").unwrap();
    let nginx_bin = executable("ROCKBEE_SOAK_NGINX_BIN").unwrap();
    let mut config = SoakConfig::from_environment().unwrap();
    config.gateway_count = 3;
    let mut interrupt = signal(SignalKind::interrupt()).unwrap();
    let mut terminate = signal(SignalKind::terminate()).unwrap();
    let (stop, _) = watch::channel(StopReason::Running);
    let signal_stop = stop.clone();
    let signal_task = tokio::spawn(async move {
        tokio::select! { _ = interrupt.recv() => {}, _ = terminate.recv() => {} }
        request_stop(&signal_stop, StopReason::Signal);
    });
    let report = create_report_directory().canonicalize().unwrap();
    eprintln!("SOAK_REPORT={}", report.display());
    let mut cluster =
        ProcessCluster::start_with_server_budget(Some(report.clone()), config.node_budget_mib);
    cluster.wait_until_directory_is_published().await;
    let mut ingress = ProxyTopology::new(&report);
    let mut clients = Vec::new();
    let started = Instant::now();
    let setup = async {
        ingress
            .start(&cluster, config, &gateway_bin, &nginx_bin, &stop)
            .await?;
        connect_and_warm_clients(&mut clients, ingress.address, config, &stop).await
    }
    .await;
    let warmup = started.elapsed();
    let measurement = if setup.is_ok() && *stop.borrow() == StopReason::Running {
        Some(measure(&clients, config, &stop, &cluster, &report).await)
    } else {
        request_stop(&stop, StopReason::Validation);
        None
    };
    let mut shutdown = Vec::new();
    for (id, client) in clients.iter().enumerate() {
        let closed = timeout(Duration::from_secs(10), client.close())
            .await
            .is_ok();
        shutdown.push(format!(
            "client_{id}={}",
            if closed { "ok" } else { "timeout" }
        ));
    }
    ingress.close(&mut shutdown).await;
    for node in &mut cluster.nodes {
        let result = node.try_stop_gracefully().await;
        shutdown.push(format!(
            "node_{}={}",
            node.child.id(),
            result.map_or_else(|error| error, |()| "ok".into())
        ));
        node.reap();
    }
    signal_task.abort();
    let _ = signal_task.await;
    let passed = write_report(
        &report,
        config,
        warmup,
        measurement.as_ref(),
        *stop.borrow(),
        &shutdown,
        setup.as_ref().err().map(String::as_str),
    )
    .unwrap();
    let mut manifest = OpenOptions::new()
        .append(true)
        .open(report.join("manifest.txt"))
        .unwrap();
    writeln!(manifest, "ingress=nginx_tcp\ngateway_processes=3\nnginx_processes=1\nnginx_master_process=off\nnginx_upstream_retry=off\nproxy_address={}\nwarmup_includes_ingress_startup=true", ingress.address).unwrap();
    drop(cluster);
    assert!(
        passed,
        "nginx three-gateway soak failed; report: {}",
        report.display()
    );
}
