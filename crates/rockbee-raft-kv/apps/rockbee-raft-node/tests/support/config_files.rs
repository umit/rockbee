//! Actual node binaries started from file-backed bootstrap configuration.
#![allow(clippy::unwrap_used)]

use std::{
    fs::{self, OpenOptions},
    net::TcpListener as PortReservation,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use bytes::Bytes;
use rockbee_raft_protocol::{
    ClusterId, MutationOutcome, NodeId, ShardConfig,
    routing::{ClusterDirectory, NodeEndpoint},
};
use tokio::signal::unix::{SignalKind, signal};

use super::*;

struct ConfiguredCluster {
    cluster: ProcessCluster,
    configs: Vec<PathBuf>,
    expected_data_root: PathBuf,
    unrelated_working_directory: PathBuf,
}

impl ConfiguredCluster {
    fn start() -> Self {
        let root = tempfile::tempdir().unwrap();
        let config_directory = root.path().join("configs");
        let unrelated_working_directory = root.path().join("elsewhere");
        fs::create_dir_all(&config_directory).unwrap();
        fs::create_dir(&unrelated_working_directory).unwrap();

        let mut reservations = Vec::new();
        let mut endpoints = Vec::new();
        for id in 1..=3 {
            let peer = PortReservation::bind("127.0.0.1:0").unwrap();
            let data = PortReservation::bind("127.0.0.1:0").unwrap();
            endpoints.push(NodeEndpoint {
                node_id: NodeId::new(id).unwrap(),
                peer_address: peer.local_addr().unwrap(),
                data_address: data.local_addr().unwrap(),
            });
            reservations.extend([peer, data]);
        }
        let directory = ClusterDirectory::initial(
            ClusterId::new([9; 16]).unwrap(),
            ShardConfig::new(4, 0).unwrap(),
            endpoints,
        )
        .unwrap();
        let configs: Vec<PathBuf> = (1..=3)
            .map(|id| write_config(&config_directory, &directory, id))
            .collect();
        let mut cluster = ProcessCluster {
            nodes: Vec::new(),
            directory,
            root,
            report_dir: None,
            server_budget_mib: 64,
        };

        // Release only listeners reserved by this fixture once all TOMLs exist.
        drop(reservations);
        spawn_configured_nodes(
            &mut cluster,
            &configs,
            &unrelated_working_directory,
            "--create",
        );
        Self {
            expected_data_root: cluster.root.path().join("data"),
            cluster,
            configs,
            unrelated_working_directory,
        }
    }

    fn reopen(&mut self) {
        for node in &mut self.cluster.nodes {
            node.crash();
        }
        self.cluster.nodes.clear();
        spawn_configured_nodes(
            &mut self.cluster,
            &self.configs,
            &self.unrelated_working_directory,
            "--open",
        );
    }
}

fn write_config(config_directory: &Path, directory: &ClusterDirectory, id: u64) -> PathBuf {
    let nodes = directory
        .nodes
        .iter()
        .map(|node| {
            format!(
                "[[nodes]]\nnode_id = {}\npeer_address = \"{}\"\ndata_address = \"{}\"\n",
                node.node_id.get(),
                node.peer_address,
                node.data_address
            )
        })
        .collect::<String>();
    let config = format!(
        "mode = \"bootstrap\"\nnode_id = {id}\ncluster_id = \"{CLUSTER_HEX}\"\ndata_dir = \"../data/node-{id}\"\nshards = 4\nhash_seed = 0\n\n{nodes}"
    );
    let path = config_directory.join(format!("node-{id}.toml"));
    fs::write(&path, config).unwrap();
    path
}

fn spawn_configured_nodes(
    cluster: &mut ProcessCluster,
    configs: &[PathBuf],
    working_directory: &Path,
    mode: &str,
) {
    // Start the initializer last, as the normal CLI process fixture does.
    for id in [2_usize, 3, 1] {
        cluster.nodes.push(spawn_configured_node(
            &configs[id - 1],
            working_directory,
            mode == "--create" && id == 1,
            mode,
        ));
    }
}

fn spawn_configured_node(
    config: &Path,
    working_directory: &Path,
    initialize: bool,
    mode: &str,
) -> OwnedNode {
    let id = config
        .file_stem()
        .and_then(|name| name.to_str())
        .expect("fixture TOML name")
        .strip_prefix("node-")
        .expect("fixture TOML node prefix");
    let log_path = config.with_file_name(format!("node-{id}.log"));
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_rockbee-raft-node"));
    command
        .current_dir(working_directory)
        .arg("--config")
        .arg(config)
        .arg(mode)
        .stdin(Stdio::null())
        .stdout(log.try_clone().unwrap())
        .stderr(log);
    if initialize {
        command.arg("--initialize");
    }
    OwnedNode {
        child: command.spawn().unwrap(),
        log_path,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn toml_configured_processes_reopen_persisted_bytes_and_delete_them() {
    // Register parent cancellation handling before the fixture creates children.
    let mut interrupt = signal(SignalKind::interrupt()).unwrap();
    let mut terminate = signal(SignalKind::terminate()).unwrap();
    let cancelled = tokio::select! {
        _ = toml_reopen_scenario() => false,
        _ = interrupt.recv() => true,
        _ = terminate.recv() => true,
    };
    assert!(
        !cancelled,
        "TOML process test cancelled after owned-child cleanup"
    );
}

async fn toml_reopen_scenario() {
    let mut configured = ConfiguredCluster::start();
    configured.cluster.wait_until_directory_is_published().await;
    assert!(configured.expected_data_root.join("node-1").exists());
    assert!(!configured.unrelated_working_directory.join("data").exists());

    let (mut gateway, client) = configured.cluster.connect_client().await;
    let key = Bytes::from_static(b"toml/orders/42");
    let value = Bytes::from_static(br#"{"status":"paid"}"#);
    assert_eq!(read_from_available_quorum(&client, key.clone()).await, None);
    assert_eq!(
        client.put(key.clone(), value.clone()).unwrap().await,
        MutationOutcome::Committed
    );
    client.close().await;
    gateway.close().await;

    configured.reopen();
    configured.cluster.wait_until_directory_is_published().await;
    let (mut gateway, client) = configured.cluster.connect_client().await;
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
    for node in &mut configured.cluster.nodes {
        node.stop_gracefully().await;
    }
}

#[test]
fn invalid_toml_cli_never_creates_storage_and_open_does_not_create_missing_storage() {
    for contents in [
        valid_config("data/unknown").replace("shards = 4", "unknown_field = true\nshards = 4"),
        valid_config(""),
    ] {
        let root = tempfile::tempdir().unwrap();
        let config = root.path().join("node.toml");
        let data = root.path().join("data");
        fs::write(&config, contents).unwrap();
        assert_rejected(Command::new(env!("CARGO_BIN_EXE_rockbee-raft-node")).args([
            "--config",
            config.to_str().unwrap(),
            "--create",
        ]));
        assert_no_storage_entries(root.path());
        assert!(!data.exists());
    }

    let root = tempfile::tempdir().unwrap();
    let config = root.path().join("node.toml");
    let data = root.path().join("data");
    fs::write(&config, valid_config("data/mixed")).unwrap();
    assert_rejected(Command::new(env!("CARGO_BIN_EXE_rockbee-raft-node")).args([
        "--config",
        config.to_str().unwrap(),
        "--create",
        "--node-id",
        "99",
    ]));
    assert_no_storage_entries(root.path());
    assert!(!data.exists());

    assert_rejected(Command::new(env!("CARGO_BIN_EXE_rockbee-raft-node")).args([
        "--config",
        config.to_str().unwrap(),
        "--open",
    ]));
    assert_no_storage_entries(root.path());
    assert!(!data.exists());
}

fn assert_rejected(command: &mut Command) {
    let log_directory = tempfile::tempdir().unwrap();
    let log_path = log_directory.path().join("rejected-node.log");
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .unwrap();
    command
        .stdin(Stdio::null())
        .stdout(log.try_clone().unwrap())
        .stderr(log);
    let mut node = OwnedNode {
        child: command.spawn().unwrap(),
        log_path,
    };
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = node.child.try_wait().unwrap() {
            assert!(
                !status.success(),
                "invalid CLI invocation unexpectedly succeeded"
            );
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "invalid CLI invocation did not reject before its bounded deadline"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn assert_no_storage_entries(root: &Path) {
    let mut entries = fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    entries.sort();
    assert_eq!(entries, [std::ffi::OsString::from("node.toml")]);
}

fn valid_config(data_dir: &str) -> String {
    format!(
        "mode = \"bootstrap\"\nnode_id = 1\ncluster_id = \"{CLUSTER_HEX}\"\ndata_dir = \"{data_dir}\"\nshards = 4\nhash_seed = 0\n\n[[nodes]]\nnode_id = 1\npeer_address = \"127.0.0.1:7101\"\ndata_address = \"127.0.0.1:8101\"\n[[nodes]]\nnode_id = 2\npeer_address = \"127.0.0.1:7102\"\ndata_address = \"127.0.0.1:8102\"\n[[nodes]]\nnode_id = 3\npeer_address = \"127.0.0.1:7103\"\ndata_address = \"127.0.0.1:8103\"\n"
    )
}
