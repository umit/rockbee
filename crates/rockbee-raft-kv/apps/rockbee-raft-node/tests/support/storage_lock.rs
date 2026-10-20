//! Owned CLI coverage for a second process contending for a live RocksDB lock.
//!
//! A child of `processes.rs`, reusing that test's exact child owners.

use super::*;
use rockbee_raft_node::{
    control::{DIRECTORY_KEY, ORIGIN_KEY},
    storage::{OpenMode, Storage, StorageIdentity, StorageOptions},
};
use rockbee_raft_protocol::{GroupId, KvRequest, KvResponse, decode_frame};
use std::{fs::OpenOptions, process::ExitStatus, sync::Arc};
use tokio::task::JoinHandle;

const TARGET_NODE: u64 = 1;
const OBSERVATION_TIMEOUT: Duration = Duration::from_secs(45);

struct Scenario {
    cluster: ProcessCluster,
    gateway: Option<GatewayRuntime>,
    pending_gateway: Option<JoinHandle<Result<GatewayRuntime, String>>>,
    gateway_address: Option<std::net::SocketAddr>,
    gateway_startup_failure: Option<String>,
    client: Option<RockBeeRaftClient>,
    contender: Option<OwnedNode>,
    links: Vec<Link<BackendRequest, BackendResponse>>,
    deadline: Instant,
}

impl Scenario {
    fn new() -> Self {
        Self {
            cluster: ProcessCluster::start(),
            gateway: None,
            pending_gateway: None,
            gateway_address: None,
            gateway_startup_failure: None,
            client: None,
            contender: None,
            links: Vec::new(),
            deadline: Instant::now() + OBSERVATION_TIMEOUT,
        }
    }

    fn remaining(&self) -> Result<(), String> {
        if Instant::now() >= self.deadline {
            return Err("storage-lock observation deadline expired".into());
        }
        Ok(())
    }

    async fn observe(&mut self) -> Result<OfflineExpectation, String> {
        let expected = self.cluster.directory.clone();
        self.cluster
            .wait_until_expected_directory_is_published(&expected, &mut self.links)
            .await?;
        self.begin_gateway().await?;
        self.finish_gateway().await?;
        let client = RockBeeRaftClient::connect(
            self.gateway_address.expect("owned gateway address"),
            ClientConfig::default(),
        )
        .await
        .map_err(|error| error.to_string())?;
        self.client = Some(client);

        let [(first_key, first_group), (second_key, second_group)] =
            distinct_group_keys(self.cluster.directory.shard_config)?;
        let first_value = Bytes::from_static(b"storage-lock/first-before-conflict");
        let second_value = Bytes::from_static(b"storage-lock/second-before-conflict");
        let updated_first_value = Bytes::from_static(b"storage-lock/first-after-conflict");
        let client = self.client.as_ref().expect("owned public client");
        for (key, value) in [(&first_key, &first_value), (&second_key, &second_value)] {
            if client
                .put(key.clone(), value.clone())
                .map_err(|error| error.to_string())?
                .await
                != MutationOutcome::Committed
            {
                return Err("seed PUT must commit before lock contention".into());
            }
        }

        self.target_strong_read(first_group, first_key.clone(), first_value.clone())
            .await?;
        self.target_strong_read(second_group, second_key.clone(), second_value.clone())
            .await?;
        self.remaining()?;

        let (directory, lock_path) = self.contender_directory()?;
        self.contender = Some(spawn_lock_contender(
            self.cluster.root.path(),
            &directory,
            NodeId::new(TARGET_NODE).map_err(|error| error.to_string())?,
        )?);
        let contender = self.contender.as_mut().expect("owned lock contender");
        let status = wait_for_normal_exit(contender, self.deadline).await?;
        if status.code().is_none_or(|code| code == 0) {
            return Err(format!(
                "lock contender must exit normally with a nonzero status, got {status}"
            ));
        }
        let log =
            std::fs::read_to_string(&contender.log_path).map_err(|error| error.to_string())?;
        let lock = lock_path.to_string_lossy();
        if !log.contains(lock.as_ref())
            || !log.contains("While lock file")
            || !log.contains("Resource temporarily unavailable")
            || !log.contains("storage open failed")
        {
            return Err(format!(
                "contender must report RocksDB lock contention at {lock}, log was: {log}"
            ));
        }
        if log.contains("raft_storage_opened")
            || log.contains("raft_node_groups_ready")
            || log.contains("raft_peer_server_start")
            || log.contains("node listeners ready")
        {
            return Err(
                "lock contender reached storage-opened or groups-ready after lock contention"
                    .into(),
            );
        }
        let native_error = log
            .lines()
            .find(|line| line.contains("While lock file"))
            .expect("validated native lock diagnostic");
        eprintln!(
            "raft_storage_lock_rejected target={TARGET_NODE} contender_pid={} exit={status} native_error={native_error}",
            contender.child.id()
        );
        let client = self.client.as_ref().expect("same owned public client");
        if client
            .put(first_key.clone(), updated_first_value.clone())
            .map_err(|error| error.to_string())?
            .await
            != MutationOutcome::Committed
        {
            return Err("same client update after lock contention must commit".into());
        }
        if client
            .get(first_key.clone())
            .await
            .map_err(|error| error.to_string())?
            != Some(updated_first_value.clone())
            || client
                .get(second_key.clone())
                .await
                .map_err(|error| error.to_string())?
                != Some(second_value.clone())
        {
            return Err("same client must retain both exact values after lock contention".into());
        }
        self.target_strong_read(first_group, first_key.clone(), updated_first_value.clone())
            .await?;
        self.target_strong_read(second_group, second_key.clone(), second_value.clone())
            .await?;
        for node in &mut self.cluster.nodes {
            if let Some(status) = node.child.try_wait().map_err(|error| error.to_string())? {
                return Err(format!(
                    "original node exited during lock scenario: {status}"
                ));
            }
        }
        Ok(OfflineExpectation {
            first_key,
            first_group,
            first_value: updated_first_value,
            second_key,
            second_group,
            second_value,
        })
    }

    async fn target_strong_read(
        &mut self,
        group: GroupId,
        key: Bytes,
        expected: Bytes,
    ) -> Result<(), String> {
        self.remaining()?;
        let target = self
            .cluster
            .directory
            .node(NodeId::new(TARGET_NODE).map_err(|error| error.to_string())?)
            .cloned()
            .ok_or_else(|| "target node is absent from original directory".to_owned())?;
        let deadline = self.deadline.min(Instant::now() + Duration::from_secs(5));
        let link = Link::<BackendRequest, BackendResponse>::connect(
            target.data_address,
            LinkConfig::default(),
            deadline,
        )
        .await
        .map_err(|error| format!("target local strong-read connection failed: {error}"))?;
        self.links.push(link);
        let link = self.links.last().expect("owned target strong-read link");
        let pending = link
            .try_submit(
                BackendRequest {
                    cluster_id: self.cluster.directory.cluster_id,
                    target_node: target.node_id,
                    operation: BackendOperation::Kv {
                        group_id: group,
                        request: KvRequest::Get { key },
                    },
                },
                deadline,
            )
            .map_err(|error| format!("target local strong-read admission failed: {error}"))?;
        let response = pending
            .await
            .map_err(|error| format!("target local strong-read transport failed: {error}"))?;
        match response.reply {
            BackendReply::Kv {
                response: KvResponse::Value(Some(value)),
                ..
            } if value == expected => Ok(()),
            other => Err(format!(
                "target lacks bounded strong-read authority or exact local value for group {}: {other:?}",
                group.get()
            )),
        }
    }

    async fn begin_gateway(&mut self) -> Result<(), String> {
        if self.pending_gateway.is_some() || self.gateway.is_some() {
            return Err("one owned gateway startup at a time".into());
        }
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|error| error.to_string())?;
        let address = listener.local_addr().map_err(|error| error.to_string())?;
        let directory = self.cluster.directory.clone();
        self.gateway_address = Some(address);
        self.pending_gateway = Some(tokio::spawn(async move {
            GatewayRuntime::start_with_listener(
                GatewayConfig {
                    cluster_id: directory.cluster_id,
                    expected_shard_config: directory.shard_config,
                    seeds: directory
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
            .map_err(|error| error.to_string())
        }));
        Ok(())
    }

    async fn finish_gateway(&mut self) -> Result<(), String> {
        let result = self
            .pending_gateway
            .as_mut()
            .expect("owned gateway startup required")
            .await;
        self.pending_gateway.take();
        match result {
            Ok(Ok(gateway)) => self.gateway = Some(gateway),
            Ok(Err(error)) => {
                self.gateway_startup_failure = Some(error.clone());
                return Err(error);
            }
            Err(error) => {
                let error = format!("gateway startup task failed: {error}");
                self.gateway_startup_failure = Some(error.clone());
                return Err(error);
            }
        }
        Ok(())
    }

    fn contender_directory(&self) -> Result<(ClusterDirectory, PathBuf), String> {
        let peer = PortReservation::bind("127.0.0.1:0").map_err(|error| error.to_string())?;
        let data = PortReservation::bind("127.0.0.1:0").map_err(|error| error.to_string())?;
        let target = NodeId::new(TARGET_NODE).map_err(|error| error.to_string())?;
        let mut directory = self.cluster.directory.clone();
        let replacement = NodeEndpoint {
            node_id: target,
            peer_address: peer.local_addr().map_err(|error| error.to_string())?,
            data_address: data.local_addr().map_err(|error| error.to_string())?,
        };
        let entry = directory
            .nodes
            .iter_mut()
            .find(|entry| entry.node_id == target)
            .ok_or_else(|| "target node is absent from contender directory".to_owned())?;
        *entry = replacement;
        directory.validate().map_err(|error| error.to_string())?;
        drop(peer);
        drop(data);
        Ok((
            directory,
            self.cluster.root.path().join("node-1").join("LOCK"),
        ))
    }

    async fn close(&mut self) -> Result<(), String> {
        let mut errors = Vec::new();
        if self.pending_gateway.is_some()
            && let Err(error) = self.finish_gateway().await
        {
            errors.push(error);
        }
        for link in self.links.drain(..) {
            link.close().await;
        }
        if let Some(client) = self.client.take() {
            client.close().await;
        }
        if let Some(mut gateway) = self.gateway.take() {
            gateway.close().await;
        }
        for node in &mut self.cluster.nodes {
            if let Err(error) = node.try_stop_gracefully().await {
                errors.push(error);
            }
        }
        if let Some(mut contender) = self.contender.take() {
            contender.reap();
        }
        if let Some(error) = self.gateway_startup_failure.take() {
            errors.push(error);
        }
        if !errors.is_empty() {
            return Err(errors.join("; "));
        }
        Ok(())
    }
}

struct OfflineExpectation {
    first_key: Bytes,
    first_group: GroupId,
    first_value: Bytes,
    second_key: Bytes,
    second_group: GroupId,
    second_value: Bytes,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_open_rejects_a_live_target_store_lock_without_disturbing_the_cluster() {
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("install storage-lock SIGTERM handler");
    #[cfg(unix)]
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .expect("install storage-lock SIGINT handler");
    let interrupted = async {
        #[cfg(unix)]
        tokio::select! {
            _ = terminate.recv() => {},
            _ = interrupt.recv() => {},
        }
        #[cfg(not(unix))]
        std::future::pending::<()>().await;
    };

    let owner = Arc::new(tokio::sync::Mutex::new(Scenario::new()));
    let observer_owner = Arc::clone(&owner);
    let mut observer = tokio::spawn(async move {
        let mut scenario = observer_owner.lock().await;
        scenario.observe().await
    });
    let observed = tokio::select! {
        result = &mut observer => Ok(result),
        _ = sleep(OBSERVATION_TIMEOUT) => Err("storage-lock observer deadline expired"),
        _ = interrupted => Err("storage-lock observer interrupted by signal"),
    };
    let expected = match observed {
        Ok(result) => result
            .map_err(|error| format!("storage-lock observer task failed: {error}"))
            .and_then(|result| result),
        Err(reason) => {
            observer.abort();
            let _ = observer.await;
            Err(reason.to_owned())
        }
    };
    let mutex = match Arc::try_unwrap(owner) {
        Ok(mutex) => mutex,
        Err(_) => panic!("observer releases outer storage-lock owner"),
    };
    let mut scenario = mutex.into_inner();
    let cleanup = scenario.close().await;
    let expected = expected.expect("storage-lock observation after owned cleanup");
    cleanup.expect("storage-lock owned cleanup");
    inspect_offline_target(&scenario.cluster, &expected)
        .await
        .expect("storage-lock offline target inspection");
    eprintln!(
        "raft_storage_lock_complete target={TARGET_NODE} original_nodes=3 same_client_update=committed direct_strong_reads=4 offline_groups={}:{} metadata=directory_and_origin cleanup=joined",
        expected.first_group.get(),
        expected.second_group.get()
    );
}

fn spawn_lock_contender(
    root: &Path,
    directory: &ClusterDirectory,
    node_id: NodeId,
) -> Result<OwnedNode, String> {
    let log_path = root.join("node-lock-contender.log");
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .map_err(|error| error.to_string())?;
    let mut command = Command::new(env!("CARGO_BIN_EXE_rockbee-raft-node"));
    command
        .arg("--node-id")
        .arg(node_id.get().to_string())
        .args([
            "--cluster-id",
            CLUSTER_HEX,
            "--shards",
            "4",
            "--hash-seed",
            "0",
        ])
        .arg("--data-dir")
        .arg(root.join(format!("node-{}", node_id.get())))
        .arg("--open")
        .stdin(Stdio::null())
        .stdout(log.try_clone().map_err(|error| error.to_string())?)
        .stderr(log);
    for node in &directory.nodes {
        command.arg("--node").arg(format!(
            "{}@{}@{}",
            node.node_id.get(),
            node.peer_address,
            node.data_address
        ));
    }
    Ok(OwnedNode {
        child: command.spawn().map_err(|error| error.to_string())?,
        log_path,
    })
}

async fn wait_for_normal_exit(
    child: &mut OwnedNode,
    deadline: Instant,
) -> Result<ExitStatus, String> {
    loop {
        if let Some(status) = child.child.try_wait().map_err(|error| error.to_string())? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            return Err("lock contender did not exit normally before observation deadline".into());
        }
        sleep(Duration::from_millis(20)).await;
    }
}

fn distinct_group_keys(shards: ShardConfig) -> Result<[(Bytes, GroupId); 2], String> {
    let first_key = Bytes::from_static(b"storage-lock/two-groups/first");
    let first_group = shards
        .group_for_key(&first_key)
        .map_err(|error| error.to_string())?;
    for suffix in 0_u16..=u16::MAX {
        let candidate = Bytes::from(format!("storage-lock/two-groups/second/{suffix}"));
        let group = shards
            .group_for_key(&candidate)
            .map_err(|error| error.to_string())?;
        if group != first_group {
            return Ok([(first_key, first_group), (candidate, group)]);
        }
    }
    Err("could not find two keys assigned to different data groups".into())
}

async fn inspect_offline_target(
    cluster: &ProcessCluster,
    expected: &OfflineExpectation,
) -> Result<(), String> {
    let identity = StorageIdentity {
        cluster_id: cluster.directory.cluster_id,
        node_id: NodeId::new(TARGET_NODE).map_err(|error| error.to_string())?,
        shard_config: cluster.directory.shard_config,
    };
    let storage = Storage::open(
        cluster.root.path().join("node-1"),
        identity,
        StorageOptions::default(),
        OpenMode::OpenExisting,
    )
    .await
    .map_err(|error| format!("offline target OpenExisting failed: {error}"))?;
    for (group, key, value) in [
        (
            expected.first_group,
            &expected.first_key,
            &expected.first_value,
        ),
        (
            expected.second_group,
            &expected.second_key,
            &expected.second_value,
        ),
    ] {
        let stores = storage
            .open_group(group)
            .map_err(|error| error.to_string())?;
        let local = stores
            .state_machine
            .reader()
            .read_local(key.clone())
            .await
            .map_err(|error| error.to_string())?;
        if local.value != Some(value.clone()) {
            return Err(format!(
                "offline target value mismatch for group {}",
                group.get()
            ));
        }
    }
    let metadata = storage
        .open_group(GroupId::new(0))
        .map_err(|error| error.to_string())?;
    for key in [DIRECTORY_KEY, ORIGIN_KEY] {
        let stored = metadata
            .state_machine
            .reader()
            .read_local(Bytes::from_static(key))
            .await
            .map_err(|error| error.to_string())?
            .value
            .ok_or_else(|| "offline target metadata key is absent".to_owned())?;
        let actual: ClusterDirectory = decode_frame(&stored).map_err(|error| error.to_string())?;
        if actual != cluster.directory {
            return Err("offline target metadata directory/origin differs from original".into());
        }
    }
    drop(metadata);
    drop(storage);
    Ok(())
}
