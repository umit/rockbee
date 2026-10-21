//! Owned CLI processes: startup, abrupt death, completed-move recovery and SIGTERM.
#![cfg(unix)]
#![allow(clippy::unwrap_used)]

#[path = "support/config_files.rs"]
mod config_files;
#[path = "support/movement_recovery.rs"]
mod movement_recovery;
#[path = "support/soak/mod.rs"]
mod soak;
#[path = "support/storage_lock.rs"]
mod storage_lock;

use std::{
    fs::OpenOptions,
    net::TcpListener as PortReservation,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};

use bytes::Bytes;
use rockbee_raft_client::{ClientConfig, RockBeeRaftClient};
use rockbee_raft_gateway::{GatewayConfig, GatewayRuntime, SeedNode};
use rockbee_raft_protocol::{
    ClusterId, MutationOutcome, NodeId, ShardConfig,
    backend::{BackendOperation, BackendReply, BackendRequest, BackendResponse},
    routing::{ClusterDirectory, NodeEndpoint},
    transport::{Link, LinkConfig, server::ServerConfig},
};
use tokio::{
    net::TcpListener,
    time::{Instant, sleep, timeout},
};

const CLUSTER_HEX: &str = "09090909090909090909090909090909";

/// Own the process handle, never discover/kill processes by name or bound port.
struct OwnedNode {
    child: Child,
    log_path: PathBuf,
}

impl OwnedNode {
    fn reap(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }

    fn crash(&mut self) -> std::process::ExitStatus {
        if self.child.try_wait().unwrap().is_none() {
            self.child.kill().unwrap();
        }
        self.child.wait().unwrap()
    }

    async fn stop_gracefully(&mut self) {
        self.try_stop_gracefully().await.unwrap();
    }

    async fn try_stop_gracefully(&mut self) -> Result<(), String> {
        if let Some(status) = self.child.try_wait().map_err(|error| error.to_string())? {
            return Err(format!("node exited before SIGTERM: {status}"));
        }
        let status = Command::new("/bin/kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .map_err(|error| error.to_string())?;
        if !status.success() {
            return Err(format!("sending SIGTERM failed: {status}"));
        }
        let status = timeout(Duration::from_secs(10), async {
            loop {
                if let Some(status) = self.child.try_wait().map_err(|error| error.to_string())? {
                    return Ok::<_, String>(status);
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .map_err(|_| "owned node exceeded graceful shutdown deadline".to_owned())??;
        if !status.success() {
            return Err(format!(
                "node SIGTERM cleanup exited unsuccessfully: {status}"
            ));
        }
        Ok(())
    }
}

impl Drop for OwnedNode {
    fn drop(&mut self) {
        // Assertions elsewhere still clean up the exact children this test owns.
        self.reap();
        if std::thread::panicking()
            && let Ok(log) = std::fs::read_to_string(&self.log_path)
        {
            eprintln!("owned node {} log:\n{}", self.child.id(), log);
        }
    }
}

struct ProcessCluster {
    // Drop children before deleting their database/log directory.
    nodes: Vec<OwnedNode>,
    directory: ClusterDirectory,
    root: tempfile::TempDir,
    report_dir: Option<PathBuf>,
    server_budget_mib: u64,
}

impl ProcessCluster {
    fn start() -> Self {
        Self::start_with_report(None)
    }

    fn start_with_report(report_dir: Option<PathBuf>) -> Self {
        Self::start_with_server_budget(report_dir, 64)
    }

    fn start_with_server_budget(report_dir: Option<PathBuf>, server_budget_mib: u64) -> Self {
        let mut reservations = Vec::new();
        let mut nodes = Vec::new();
        for id in 1..=3 {
            let peer = PortReservation::bind("127.0.0.1:0").unwrap();
            let data = PortReservation::bind("127.0.0.1:0").unwrap();
            nodes.push(NodeEndpoint {
                node_id: NodeId::new(id).unwrap(),
                peer_address: peer.local_addr().unwrap(),
                data_address: data.local_addr().unwrap(),
            });
            reservations.extend([peer, data]);
        }
        let directory = ClusterDirectory::initial(
            ClusterId::new([9; 16]).unwrap(),
            ShardConfig::new(4, 0).unwrap(),
            nodes,
        )
        .unwrap();
        let mut cluster = Self {
            nodes: Vec::new(),
            directory,
            root: tempfile::tempdir().unwrap(),
            report_dir,
            server_budget_mib,
        };
        // CLI processes bind themselves. Release only these owned reservations;
        // an unrelated bind race makes startup fail, never kills that process.
        drop(reservations);
        cluster.spawn_nodes("--create");
        cluster
    }

    fn spawn_nodes(&mut self, mode: &str) {
        // Start the logical initializer last; readiness still waits for quorum.
        for id in [2, 3, 1] {
            self.nodes.push(spawn_node(
                self.root.path(),
                &self.directory,
                id,
                mode,
                self.server_budget_mib,
            ));
        }
    }

    async fn wait_until_directory_is_published(&mut self) {
        let expected = self.directory.clone();
        let mut links = Vec::new();
        let result = self
            .wait_until_expected_directory_is_published(&expected, &mut links)
            .await;
        for link in links {
            link.close().await;
        }
        result.expect("owned processes must publish their initial directory");
    }

    async fn wait_until_expected_directory_is_published(
        &mut self,
        expected: &ClusterDirectory,
        links: &mut Vec<Link<BackendRequest, BackendResponse>>,
    ) -> Result<(), String> {
        timeout(Duration::from_secs(30), async {
            loop {
                for node in &mut self.nodes {
                    if let Some(status) =
                        node.child.try_wait().map_err(|error| error.to_string())?
                    {
                        return Err(format!(
                            "node {} exited during startup: {status}",
                            node.child.id()
                        ));
                    }
                }
                for endpoint in &self.directory.nodes {
                    let deadline = Instant::now() + Duration::from_millis(250);
                    let Ok(link) = Link::<BackendRequest, BackendResponse>::connect(
                        endpoint.data_address,
                        LinkConfig::default(),
                        deadline,
                    )
                    .await
                    else {
                        continue;
                    };
                    links.push(link);
                    let link = links.last().unwrap();
                    let result = match link.try_submit(
                        BackendRequest {
                            cluster_id: self.directory.cluster_id,
                            target_node: endpoint.node_id,
                            operation: BackendOperation::Directory,
                        },
                        deadline,
                    ) {
                        Ok(pending) => pending.await.ok(),
                        Err(_) => None,
                    };
                    link.close().await;
                    links.pop();
                    if let Some(response) = result
                        && response.reply == BackendReply::Directory(Ok(expected.clone()))
                    {
                        return Ok(());
                    }
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .map_err(|_| "owned processes must publish the expected directory".to_owned())?
    }

    async fn connect_client(&self) -> (GatewayRuntime, RockBeeRaftClient) {
        let mut gateway = None;
        let client = self
            .connect_client_with_gateway(&mut gateway)
            .await
            .unwrap();
        (gateway.unwrap(), client)
    }

    async fn connect_client_with_gateway(
        &self,
        gateway: &mut Option<GatewayRuntime>,
    ) -> Result<RockBeeRaftClient, String> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|error| error.to_string())?;
        let address = listener.local_addr().map_err(|error| error.to_string())?;
        *gateway = Some(
            GatewayRuntime::start_with_listener(
                GatewayConfig {
                    cluster_id: self.directory.cluster_id,
                    expected_shard_config: self.directory.shard_config,
                    seeds: self
                        .directory
                        .nodes
                        .iter()
                        .map(|node| SeedNode {
                            node_id: node.node_id,
                            address: node.data_address,
                        })
                        .collect(),
                    listen_address: address,
                    backend_link: LinkConfig::default(),
                    server: ServerConfig::default(),
                    startup_timeout: Duration::from_secs(10),
                },
                listener,
            )
            .await
            .map_err(|error| error.to_string())?,
        );
        RockBeeRaftClient::connect(address, ClientConfig::default())
            .await
            .map_err(|error| error.to_string())
    }
}

impl Drop for ProcessCluster {
    fn drop(&mut self) {
        // Reap first so every owned process has finished writing its failure log.
        for node in &mut self.nodes {
            node.reap();
        }
        if let Some(report_dir) = &self.report_dir {
            for name in ["node-1.log", "node-2.log", "node-3.log", "node-join.log"] {
                let source = self.root.path().join(name);
                if source.exists()
                    && let Err(error) = std::fs::copy(source, report_dir.join(name))
                {
                    eprintln!("could not preserve owned node log: {error}");
                }
            }
        }
    }
}

fn spawn_node(
    root: &Path,
    directory: &ClusterDirectory,
    id: u64,
    mode: &str,
    server_budget_mib: u64,
) -> OwnedNode {
    let log_path = root.join(format!("node-{id}.log"));
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_rockbee-raft-node"));
    command
        .arg("--node-id")
        .arg(id.to_string())
        .args([
            "--cluster-id",
            CLUSTER_HEX,
            "--shards",
            "4",
            "--hash-seed",
            "0",
        ])
        .arg("--data-dir")
        .arg(root.join(format!("node-{id}")))
        .arg(mode)
        .arg("--server-request-mib")
        .arg(server_budget_mib.to_string())
        .arg("--server-response-mib")
        .arg(server_budget_mib.to_string())
        .stdin(Stdio::null())
        .stdout(log.try_clone().unwrap())
        .stderr(log);
    for node in &directory.nodes {
        command.arg("--node").arg(format!(
            "{}@{}@{}",
            node.node_id.get(),
            node.peer_address,
            node.data_address
        ));
    }
    if id == 1 {
        command.arg("--initialize");
    }
    OwnedNode {
        child: command.spawn().unwrap(),
        log_path,
    }
}

async fn read_from_available_quorum(client: &RockBeeRaftClient, key: Bytes) -> Option<Bytes> {
    timeout(Duration::from_secs(25), async {
        loop {
            // Only read failures are retried while this group's election settles.
            // A successful absence is returned immediately, never hidden by polling.
            if let Ok(value) = client.get(key.clone()).await {
                return value;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("data group must obtain read authority")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_processes_recover_committed_data_after_abrupt_death_and_close_on_sigterm() {
    use tokio::signal::unix::{SignalKind, signal};
    // Register before any child is launched. Direct SIGTERM to this test PID
    // must also drop the fixture and reap its children, not bypass Drop.
    let mut interrupt = signal(SignalKind::interrupt()).unwrap();
    let mut terminate = signal(SignalKind::terminate()).unwrap();
    let cancelled = tokio::select! {
        _ = recovery_scenario() => false,
        _ = interrupt.recv() => true,
        _ = terminate.recv() => true,
    };
    assert!(
        !cancelled,
        "process test cancelled after owned-child cleanup"
    );
}

async fn recovery_scenario() {
    let mut cluster = ProcessCluster::start();
    if let Some(ready_file) = std::env::var_os("ROCKBEE_PROCESS_TEST_READY_FILE") {
        // An external cancellation check controls this test-only barrier. The
        // file contains only PIDs of children this fixture has actually spawned.
        let pids = cluster
            .nodes
            .iter()
            .map(|node| node.child.id().to_string())
            .collect::<Vec<_>>()
            .join("\n");
        tokio::fs::write(ready_file, pids).await.unwrap();
        std::future::pending::<()>().await;
    }
    cluster.wait_until_directory_is_published().await;
    let (mut gateway, client) = cluster.connect_client().await;
    let key = Bytes::from_static(b"orders/42");
    let value = Bytes::from_static(br#"{"status":"paid"}"#);
    assert_eq!(read_from_available_quorum(&client, key.clone()).await, None);
    assert_eq!(
        client.put(key.clone(), value.clone()).unwrap().await,
        MutationOutcome::Committed
    );
    assert_eq!(client.get(key.clone()).await.unwrap(), Some(value.clone()));
    client.close().await;
    gateway.close().await;

    for node in &mut cluster.nodes {
        node.crash();
    }
    cluster.nodes.clear();
    cluster.spawn_nodes("--open");
    cluster.wait_until_directory_is_published().await;
    let (mut gateway, client) = cluster.connect_client().await;
    assert_eq!(
        read_from_available_quorum(&client, key.clone()).await,
        Some(value)
    );
    assert_eq!(
        client.delete(key.clone()).unwrap().await,
        MutationOutcome::Committed
    );
    assert_eq!(client.get(key).await.unwrap(), None);
    client.close().await;
    gateway.close().await;
    for node in &mut cluster.nodes {
        node.stop_gracefully().await;
    }
}

fn spawn_join(
    root: &Path,
    directory: &ClusterDirectory,
    endpoint: &NodeEndpoint,
    mode: &str,
) -> OwnedNode {
    let log_path = root.join("node-join.log");
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_rockbee-raft-node"));
    command
        .args([
            "--join",
            "--cluster-id",
            CLUSTER_HEX,
            "--shards",
            "4",
            "--hash-seed",
            "0",
        ])
        .arg("--local-node")
        .arg(format!(
            "{}@{}@{}",
            endpoint.node_id.get(),
            endpoint.peer_address,
            endpoint.data_address
        ))
        .arg("--data-dir")
        .arg(root.join("join-db"))
        .arg(mode)
        .stdin(Stdio::null())
        .stdout(log.try_clone().unwrap())
        .stderr(log);
    for node in &directory.nodes {
        command
            .arg("--seed")
            .arg(format!("{}@{}", node.node_id.get(), node.data_address));
    }
    OwnedNode {
        child: command.spawn().unwrap(),
        log_path,
    }
}

async fn wait_join_listener(node: &mut OwnedNode, endpoint: &NodeEndpoint, cluster_id: ClusterId) {
    let mut links = Vec::new();
    let result = wait_join_listener_with_links(node, endpoint, cluster_id, &mut links).await;
    for link in links {
        link.close().await;
    }
    result.expect("explicit join process should discover ABC and start its listener");
}

async fn wait_join_listener_with_links(
    node: &mut OwnedNode,
    endpoint: &NodeEndpoint,
    cluster_id: ClusterId,
    links: &mut Vec<Link<BackendRequest, BackendResponse>>,
) -> Result<(), String> {
    timeout(Duration::from_secs(15), async {
        loop {
            if let Some(status) = node.child.try_wait().map_err(|error| error.to_string())? {
                return Err(format!("join process exited during startup: {status}"));
            }
            let deadline = Instant::now() + Duration::from_millis(250);
            if let Ok(link) = Link::<BackendRequest, BackendResponse>::connect(
                endpoint.data_address,
                LinkConfig::default(),
                deadline,
            )
            .await
            {
                links.push(link);
                let link = links.last().unwrap();
                let reply = match link.try_submit(
                    BackendRequest {
                        cluster_id,
                        target_node: endpoint.node_id,
                        operation: BackendOperation::Directory,
                    },
                    deadline,
                ) {
                    Ok(pending) => pending.await.ok(),
                    Err(_) => None,
                };
                link.close().await;
                links.pop();
                if matches!(
                    reply.map(|response| response.reply),
                    Some(BackendReply::Directory(Err(_)))
                ) {
                    return Ok(());
                }
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| "explicit join process must discover ABC and start its listener".to_owned())?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_join_process_discovers_live_abc_and_reopens_its_distinct_database() {
    let mut cluster = ProcessCluster::start();
    cluster.wait_until_directory_is_published().await;
    let root = tempfile::tempdir().unwrap();
    let peer = PortReservation::bind("127.0.0.1:0").unwrap();
    let data = PortReservation::bind("127.0.0.1:0").unwrap();
    let endpoint = NodeEndpoint {
        node_id: NodeId::new(4).unwrap(),
        peer_address: peer.local_addr().unwrap(),
        data_address: data.local_addr().unwrap(),
    };
    drop(peer);
    drop(data);
    let mut joined = spawn_join(root.path(), &cluster.directory, &endpoint, "--create");
    wait_join_listener(&mut joined, &endpoint, cluster.directory.cluster_id).await;
    assert!(root.path().join("join-db").exists());
    joined.stop_gracefully().await;
    let mut reopened = spawn_join(root.path(), &cluster.directory, &endpoint, "--open");
    wait_join_listener(&mut reopened, &endpoint, cluster.directory.cluster_id).await;
    reopened.stop_gracefully().await;
    // D's start/open must leave the original ABC publication usable.
    cluster.wait_until_directory_is_published().await;
    for node in &mut cluster.nodes {
        node.stop_gracefully().await;
    }
}

#[test]
fn invalid_join_cli_rejects_conflicting_or_missing_intent_before_database_creation() {
    for extra in [
        vec!["--initialize"],
        vec!["--node", "1@127.0.0.1:7101@127.0.0.1:8101"],
        vec!["--open"],
    ] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("db");
        let output = Command::new(env!("CARGO_BIN_EXE_rockbee-raft-node"))
            .args([
                "--join",
                "--cluster-id",
                CLUSTER_HEX,
                "--local-node",
                "4@127.0.0.1:7104@127.0.0.1:8104",
                "--seed",
                "1@127.0.0.1:8101",
                "--create",
            ])
            .arg("--data-dir")
            .arg(&path)
            .args(extra)
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(!path.exists());
    }
}
