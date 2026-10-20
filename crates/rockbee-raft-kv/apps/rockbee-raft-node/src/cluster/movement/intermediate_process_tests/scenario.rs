use super::wire::{
    ChildCleanup, CleanupReceipt, Command as ChildCommand, Config, Envelope, Held, Receipt,
};
use super::*;
use crate::{
    cluster::{NodeError, NodeRuntime},
    config::{JoinConfig, JoinSeed, NodeConfig},
    rpc::PeerLimits,
    storage::{OpenMode, Storage, StorageIdentity, StorageOptions},
};
use rockbee_raft_client::{ClientConfig, RockBeeRaftClient};
use rockbee_raft_gateway::{GatewayConfig, GatewayError, GatewayRuntime, SeedNode};
use rockbee_raft_protocol::{MutationOutcome, transport::server::ServerConfig};
use std::{
    future::Future,
    process::{Child, Command, Stdio},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{net::TcpListener, sync::Mutex as AsyncMutex, task::JoinHandle};

struct OwnedChild {
    child: Child,
    config: Config,
    sequence: u8,
    reaped: bool,
}
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if !self.reaped {
            let _ = self.child.kill();
            let _ = self.child.wait();
            self.reaped = true;
        }
    }
}
impl OwnedChild {
    fn send(&mut self, command: ChildCommand) {
        self.sequence = self.sequence.checked_add(1).unwrap();
        assert!(self.sequence <= wire::MAX_COMMANDS);
        wire::write(
            &self.config.path("command.bin"),
            &self
                .config
                .envelope(self.sequence, self.child.id(), command),
        );
        // Keep exact commands in addition to the single live command slot.
        let archived = self.config.path(&format!("command-{}.bin", self.sequence));
        std::fs::copy(self.config.path("command.bin"), archived).unwrap();
    }
    async fn receipt(&mut self, sequence: u8, kind: &str, deadline: Instant) -> Receipt {
        loop {
            if let Some(receipt) = wire::read::<Envelope<Receipt>>(
                &self.config.path(&format!("receipt-{sequence}-{kind}.bin")),
            ) {
                self.config.check(&receipt, sequence, self.child.id());
                return receipt.body;
            }
            assert!(
                self.child.try_wait().unwrap().is_none(),
                "child exited before exact {kind} receipt; see {}",
                self.config.files.display()
            );
            assert!(
                Instant::now() < deadline,
                "child {kind} deadline; see {}",
                self.config.files.display()
            );
            sleep(Duration::from_millis(5)).await;
        }
    }

    fn has_stopped_receipt(&self, sequence: u8) -> bool {
        let Some(receipt) = wire::read::<Envelope<Receipt>>(
            &self.config.path(&format!("receipt-{sequence}-stopped.bin")),
        ) else {
            return false;
        };
        self.config.check(&receipt, sequence, self.child.id());
        matches!(receipt.body, Receipt::Stopped)
    }

    async fn stop_and_reap(&mut self) -> ChildCleanup {
        if self.reaped {
            return ChildCleanup::ExpectedFaultReaped;
        }
        if let Some(status) = self.child.try_wait().unwrap() {
            self.reaped = true;
            return ChildCleanup::UnexpectedExit {
                sequence: self.sequence,
                status: status.to_string(),
            };
        }

        self.send(ChildCommand::Stop);
        let sequence = self.sequence;
        let deadline = Instant::now() + Duration::from_secs(10);
        let stopped = loop {
            if let Some(receipt) = wire::read::<Envelope<Receipt>>(
                &self.config.path(&format!("receipt-{sequence}-stopped.bin")),
            ) {
                self.config.check(&receipt, sequence, self.child.id());
                break matches!(receipt.body, Receipt::Stopped);
            }
            if let Some(status) = self.child.try_wait().unwrap() {
                self.reaped = true;
                return if self.has_stopped_receipt(sequence) && status.success() {
                    ChildCleanup::Graceful {
                        sequence,
                        status: status.to_string(),
                    }
                } else {
                    ChildCleanup::UnexpectedExit {
                        sequence,
                        status: status.to_string(),
                    }
                };
            }
            if Instant::now() >= deadline {
                break false;
            }
            sleep(Duration::from_millis(10)).await;
        };

        while Instant::now() < deadline {
            if let Some(status) = self.child.try_wait().unwrap() {
                self.reaped = true;
                return if stopped && status.success() {
                    ChildCleanup::Graceful {
                        sequence,
                        status: status.to_string(),
                    }
                } else {
                    ChildCleanup::UnexpectedExit {
                        sequence,
                        status: status.to_string(),
                    }
                };
            }
            sleep(Duration::from_millis(10)).await;
        }

        self.child.kill().unwrap();
        let status = self.child.wait().unwrap();
        self.reaped = true;
        ChildCleanup::ForcedKill {
            sequence,
            status: status.to_string(),
        }
    }
}
#[derive(Default)]
struct Resources {
    nodes: Vec<NodeRuntime>,
    child: Option<OwnedChild>,
    gateway: Option<GatewayRuntime>,
    client: Option<RockBeeRaftClient>,
    links: Vec<BackendLink>,
    pending_node: Option<JoinHandle<Result<NodeRuntime, NodeError>>>,
    pending_gateway: Option<JoinHandle<Result<GatewayRuntime, GatewayError>>>,
    node_startup_failure: Option<String>,
    gateway_startup_failure: Option<String>,
}
impl Resources {
    fn begin_node<F>(&mut self, startup: F)
    where
        F: Future<Output = Result<NodeRuntime, NodeError>> + Send + 'static,
    {
        assert!(self.pending_node.is_none());
        self.pending_node = Some(tokio::spawn(startup));
    }

    async fn finish_node(&mut self) -> Result<(), String> {
        let result = self
            .pending_node
            .as_mut()
            .expect("owned node startup required")
            .await;
        self.pending_node.take();
        let node = match result {
            Ok(Ok(node)) => node,
            Ok(Err(error)) => {
                return Err(
                    self.remember_node_startup_failure(format!("node startup failed: {error}"))
                );
            }
            Err(error) => {
                return Err(self.remember_node_startup_failure(format!(
                    "owned node startup task failed: {error}"
                )));
            }
        };
        self.nodes.push(node);
        Ok(())
    }

    fn begin_gateway<F>(&mut self, startup: F)
    where
        F: Future<Output = Result<GatewayRuntime, GatewayError>> + Send + 'static,
    {
        assert!(self.pending_gateway.is_none());
        self.pending_gateway = Some(tokio::spawn(startup));
    }

    async fn finish_gateway(&mut self) -> Result<(), String> {
        let result = self
            .pending_gateway
            .as_mut()
            .expect("owned gateway startup required")
            .await;
        self.pending_gateway.take();
        let gateway = match result {
            Ok(Ok(gateway)) => gateway,
            Ok(Err(error)) => {
                return Err(self
                    .remember_gateway_startup_failure(format!("gateway startup failed: {error}")));
            }
            Err(error) => {
                return Err(self.remember_gateway_startup_failure(format!(
                    "owned gateway startup task failed: {error}"
                )));
            }
        };
        self.gateway = Some(gateway);
        Ok(())
    }

    fn remember_node_startup_failure(&mut self, error: String) -> String {
        self.node_startup_failure
            .get_or_insert_with(|| bounded_failure(&error));
        error
    }

    fn remember_gateway_startup_failure(&mut self, error: String) -> String {
        self.gateway_startup_failure
            .get_or_insert_with(|| bounded_failure(&error));
        error
    }

    fn restore_elections(&self) {
        for node in &self.nodes {
            node.service.groups[&GROUP]
                .raft
                .runtime_config()
                .elect(true);
            node.service.groups[&GROUP]
                .raft
                .runtime_config()
                .heartbeat(true);
        }
    }
    async fn cleanup(&mut self) -> CleanupReceipt {
        self.restore_elections();
        // Startup tasks stay owned by Resources while the assertion task is
        // cancelable. Joining them here transfers successful runtimes into the
        // normal close path; a startup future is never aborted as cleanup.
        let pending_node_startups_joined = match self.pending_node.is_some() {
            true => self.finish_node().await.is_ok(),
            false => self.node_startup_failure.is_none(),
        };
        let pending_gateway_startup_joined = match self.pending_gateway.is_some() {
            true => self.finish_gateway().await.is_ok(),
            false => self.gateway_startup_failure.is_none(),
        };
        let child = match self.child.as_mut() {
            Some(child) => Some(child.stop_and_reap().await),
            None => None,
        };
        // Taking only after explicit reaping prevents Drop from disguising a
        // forced fallback as a clean shutdown.
        self.child.take();
        for link in self.links.drain(..) {
            link.close().await;
        }
        if let Some(client) = self.client.take() {
            client.close().await;
        }
        if let Some(mut gateway) = self.gateway.take() {
            gateway.close().await;
        }
        for node in &mut self.nodes {
            node.close().await;
        }
        self.nodes.clear();
        CleanupReceipt {
            complete: pending_node_startups_joined
                && pending_gateway_startup_joined
                && child.as_ref().is_none_or(|outcome| {
                    matches!(
                        outcome,
                        ChildCleanup::ExpectedFaultReaped | ChildCleanup::Graceful { .. }
                    )
                }),
            child,
            pending_node_startups_joined,
            pending_gateway_startup_joined,
            node_startup_failure: self.node_startup_failure.take(),
            gateway_startup_failure: self.gateway_startup_failure.take(),
        }
    }
    async fn backend(
        &mut self,
        endpoint: &NodeEndpoint,
        cluster: ClusterId,
        operation: BackendOperation,
        deadline: Instant,
    ) -> Result<BackendReply, String> {
        let link = BackendLink::connect(endpoint.data_address, LinkConfig::default(), deadline)
            .await
            .map_err(|error| error.to_string())?;
        self.links.push(link);
        let response = self.links.last().unwrap().try_submit(
            BackendRequest {
                cluster_id: cluster,
                target_node: endpoint.node_id,
                operation,
            },
            deadline,
        );
        let result = match response {
            Ok(response) => response.await.map_err(|error| error.to_string()),
            Err(error) => Err(error.to_string()),
        };
        self.links.last().unwrap().close().await;
        self.links.pop();
        let response = result?;
        assert_eq!(response.cluster_id, cluster);
        assert_eq!(response.node_id, endpoint.node_id);
        Ok(response.reply)
    }
    async fn authority(
        &mut self,
        config: &Config,
        candidates: &[usize],
        deadline: Instant,
    ) -> (NodeEndpoint, AppliedMembership) {
        loop {
            for index in candidates {
                let endpoint = &config.endpoints[*index];
                let attempt = deadline.min(Instant::now() + Duration::from_millis(500));
                if let Ok(BackendReply::Membership {
                    data_group,
                    result: Ok(observed),
                }) = self
                    .backend(
                        endpoint,
                        config.directory.cluster_id,
                        BackendOperation::ObserveMembership { data_group: GROUP },
                        attempt,
                    )
                    .await
                {
                    assert_eq!(data_group, GROUP);
                    observed.validate().unwrap();
                    return (endpoint.clone(), *observed);
                }
            }
            assert!(
                Instant::now() < deadline,
                "fresh typed actual authority required"
            );
            sleep(Duration::from_millis(10)).await;
        }
    }
    async fn metadata(&mut self, config: &Config, deadline: Instant) -> (NodeEndpoint, MoveState) {
        loop {
            for endpoint in &config.endpoints[..3] {
                if endpoint.node_id.get() == 2
                    && self.child.as_ref().is_none_or(|child| child.reaped)
                {
                    continue;
                }
                let attempt = deadline.min(Instant::now() + Duration::from_millis(500));
                if let Ok(BackendReply::MoveState {
                    data_group,
                    result: Ok(state),
                }) = self
                    .backend(
                        endpoint,
                        config.directory.cluster_id,
                        BackendOperation::GetMove { data_group: GROUP },
                        attempt,
                    )
                    .await
                {
                    assert_eq!(data_group, GROUP);
                    return (endpoint.clone(), *state);
                }
            }
            assert!(
                Instant::now() < deadline,
                "fresh typed metadata authority required"
            );
            sleep(Duration::from_millis(10)).await;
        }
    }
    fn launch(&mut self, config: Config) {
        assert!(self.child.is_none());
        config.validate();
        std::fs::create_dir_all(&config.files).unwrap();
        let config_path = config.path("config.bin");
        wire::write(&config_path, &config);
        let log = std::fs::File::create(config.path("child.log")).unwrap();
        let child = Command::new(std::env::current_exe().unwrap())
            .args([CHILD_FILTER, "--exact", "--test-threads=1", "--nocapture"])
            .env(MARKER, config_path)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap();
        self.child = Some(OwnedChild {
            child,
            config,
            sequence: 0,
            reaped: false,
        });
    }
}

fn bounded_failure(error: &str) -> String {
    error.chars().take(2048).collect()
}

/// A consumed startup JoinHandle must leave failure evidence for the common
/// cleanup path. This is fixture-only and starts neither a listener nor a
/// RocksDB/OpenRaft runtime.
#[tokio::test]
async fn consumed_startup_panics_remain_visible_to_cleanup() {
    let mut resources = Resources::default();
    resources.begin_node(async {
        panic!("node constructor panic before handoff");
    });
    assert!(resources.finish_node().await.is_err());

    resources.begin_gateway(async {
        panic!("gateway constructor panic before handoff");
    });
    assert!(resources.finish_gateway().await.is_err());

    let cleanup = resources.cleanup().await;
    assert!(!cleanup.complete);
    assert!(!cleanup.pending_node_startups_joined);
    assert!(!cleanup.pending_gateway_startup_joined);
    assert!(
        cleanup
            .node_startup_failure
            .as_deref()
            .is_some_and(|failure| failure.contains("node startup task failed"))
    );
    assert!(
        cleanup
            .gateway_startup_failure
            .as_deref()
            .is_some_and(|failure| failure.contains("gateway startup task failed"))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn joint_controller_process_kill_recovers_same_job_and_public_traffic() {
    let _ = env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("rockbee_raft_node=info"),
    )
    .is_test(true)
    .try_init();
    // Install signal/deadline ownership before any runtime or child launch.
    #[cfg(unix)]
    let mut terminate =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
    #[cfg(unix)]
    let mut interrupt =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(180);
    let report = tempfile::Builder::new()
        .prefix("rockbee-joint-process-")
        .tempdir()
        .unwrap()
        .keep();
    eprintln!(
        "joint process typed receipts/logs retained at {}",
        report.display()
    );
    let owner = Arc::new(AsyncMutex::new(Resources::default()));
    let scenario_owner = Arc::clone(&owner);
    let scenario_report = report.clone();
    let mut task = tokio::spawn(async move {
        run(
            &mut *scenario_owner.lock().await,
            &scenario_report,
            deadline,
        )
        .await
    });
    let signal = async {
        #[cfg(unix)]
        tokio::select! { _ = terminate.recv() => {}, _ = interrupt.recv() => {} }
        #[cfg(not(unix))]
        std::future::pending::<()>().await;
    };
    let outcome = tokio::select! {
        outcome = &mut task => outcome,
        _ = sleep_until(deadline) => { task.abort(); task.await },
        _ = signal => { task.abort(); task.await },
    };
    let cleanup = owner.lock().await.cleanup().await;
    wire::write(&report.join("cleanup.bin"), &cleanup);
    assert!(
        cleanup.complete,
        "owned cleanup incomplete: {cleanup:?}; reports {}",
        report.display()
    );
    if let Err(error) = outcome {
        if error.is_panic() {
            std::panic::resume_unwind(error.into_panic());
        }
        panic!(
            "joint process scenario interrupted/deadline after owned cleanup; reports {}",
            report.display()
        );
    }
}

async fn run(resources: &mut Resources, report: &Path, deadline: Instant) {
    let mut listeners = Vec::new();
    let mut endpoints = Vec::new();
    for id in 1..=4 {
        let peer = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let data = TcpListener::bind("127.0.0.1:0").await.unwrap();
        endpoints.push(NodeEndpoint {
            node_id: NodeId::new(id).unwrap(),
            peer_address: peer.local_addr().unwrap(),
            data_address: data.local_addr().unwrap(),
        });
        listeners.push(Some((peer, data)));
    }
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        .to_le_bytes();
    let directory = ClusterDirectory::initial(
        ClusterId::new(nonce).unwrap(),
        ShardConfig::new(2, 0).unwrap(),
        endpoints[..3].to_vec(),
    )
    .unwrap();
    let request = MoveReplica {
        job_id: MoveJobId::new(nonce).unwrap(),
        data_group: GROUP,
        remove_node: NodeId::new(3).unwrap(),
        add_node: endpoints[3].clone(),
        expected_directory_revision: 1,
    };
    let config = Config {
        nonce,
        incarnation: 1,
        directory: directory.clone(),
        endpoints: endpoints.clone(),
        database: report.join("b-db"),
        files: report.join("incarnation-1"),
        reopen: false,
        request,
    };
    for index in [0, 2] {
        let (peer, data) = listeners[index].take().unwrap();
        let config = NodeConfig::new(
            directory.clone(),
            endpoints[index].node_id,
            report.join(format!("node-{}", index + 1)),
            OpenMode::CreateNew,
        )
        .unwrap();
        resources.begin_node(NodeRuntime::start_with_listeners(config, peer, data));
        resources.finish_node().await.unwrap();
    }
    // B's endpoint reservation is released only for the exact child launch.
    // A competing bind is an explicit bounded startup failure, never a new port.
    drop(listeners[1].take());
    resources.launch(config.clone());
    require_ready(resources, &config, deadline).await;
    resources.nodes[0].initialize_groups().await.unwrap();
    resources.nodes[0]
        .bootstrap_directory(deadline)
        .await
        .unwrap();
    let (peer, data) = listeners[3].take().unwrap();
    let join = JoinConfig {
        local_endpoint: endpoints[3].clone(),
        cluster_id: directory.cluster_id,
        shard_config: directory.shard_config,
        seeds: endpoints[..3]
            .iter()
            .map(|endpoint| JoinSeed {
                node_id: endpoint.node_id,
                address: endpoint.data_address,
            })
            .collect(),
        startup_timeout: Duration::from_secs(10),
        data_directory: report.join("node-4"),
        open_mode: OpenMode::CreateNew,
        storage: StorageOptions::default(),
        peers: PeerLimits::default(),
        server: ServerConfig::default(),
    };
    resources.begin_node(NodeRuntime::start_join_with_listeners(join, peer, data));
    resources.finish_node().await.unwrap();
    // B alone retains ordinary automatic elections/heartbeats. Establish real
    // authority by TCP barrier before restoring every parent control.
    for node in &resources.nodes {
        node.service.groups[&GROUP]
            .raft
            .runtime_config()
            .elect(false);
        node.service.groups[&GROUP]
            .raft
            .runtime_config()
            .heartbeat(false);
    }
    let (_, b_source) = resources.authority(&config, &[1], deadline).await;
    assert_eq!(
        topology(
            &b_source,
            &config.request.derive_intent(&directory).unwrap()
        )
        .unwrap(),
        Topology::Source
    );
    resources.restore_elections();
    wire::write(&report.join("b-actual-initial-authority.bin"), &b_source);

    let public = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = public.local_addr().unwrap();
    let gateway_config = GatewayConfig {
        cluster_id: directory.cluster_id,
        expected_shard_config: directory.shard_config,
        seeds: directory
            .nodes
            .iter()
            .map(|endpoint| SeedNode {
                node_id: endpoint.node_id,
                address: endpoint.data_address,
            })
            .collect(),
        listen_address: address,
        backend_link: LinkConfig::default(),
        server: ServerConfig::default(),
        startup_timeout: Duration::from_secs(10),
    };
    resources.begin_gateway(GatewayRuntime::start_with_listener(gateway_config, public));
    resources.finish_gateway().await.unwrap();
    resources.client = Some(
        RockBeeRaftClient::connect(address, ClientConfig::default())
            .await
            .unwrap(),
    );
    let keys = keys_for(&directory, GROUP, 3);
    let sibling = keys_for(&directory, GroupId::new(2), 1).remove(0);
    let value = Bytes::from_static(b"joint process durable baseline");
    let client = resources.client.as_ref().unwrap();
    put(client, keys[0].clone(), value.clone()).await;
    put(
        client,
        keys[1].clone(),
        Bytes::from_static(b"deleted before actual joint"),
    )
    .await;
    delete(client, keys[1].clone()).await;
    put(client, sibling.clone(), value.clone()).await;
    get(client, keys[0].clone(), Some(value.clone()), deadline).await;
    get(client, keys[1].clone(), None, deadline).await;
    let (_, baseline_observed) = resources.authority(&config, &[1], deadline).await;
    let baseline = native(baseline_observed.applied);
    wire::write(&report.join("baseline-membership.bin"), &baseline_observed);
    let (metadata, before) = resources.metadata(&config, deadline).await;
    assert_eq!(before.directory, directory);
    assert!(before.record.is_none());
    let accepted = resources
        .backend(
            &metadata,
            directory.cluster_id,
            BackendOperation::MoveReplica(config.request.clone()),
            deadline,
        )
        .await
        .unwrap();
    let intent = config.request.derive_intent(&directory).unwrap();
    assert!(
        matches!(&accepted, BackendReply::Move { data_group, job_id, result: AdminResult::Accepted { intent: actual } } if *data_group == GROUP && *job_id == config.request.job_id && *actual == intent)
    );
    let (_, pending) = resources.metadata(&config, deadline).await;
    assert_eq!(pending.directory.revision, 1);
    assert_eq!(pending.record.as_ref().unwrap().intent, intent);
    assert_eq!(
        pending.record.as_ref().unwrap().progress,
        MoveProgress::Pending
    );
    assert_eq!(pending.directory, directory);
    // The exact D endpoint is retained in the pending intent, not published
    // into the revision-one directory before actual uniform target completion.
    assert_eq!(
        pending.record.as_ref().unwrap().intent.add_node,
        endpoints[3]
    );
    wire::write(&report.join("exact-pending.bin"), &pending);
    resources
        .child
        .as_mut()
        .unwrap()
        .send(ChildCommand::Arm { baseline });
    let arm = resources
        .child
        .as_mut()
        .unwrap()
        .receipt(1, "armed", deadline)
        .await;
    assert!(
        matches!(arm, Receipt::Armed { source } if topology(&source, &intent).unwrap() == Topology::Source)
    );
    let scheduled = resources
        .backend(
            &endpoints[1],
            directory.cluster_id,
            BackendOperation::ReconcileMove {
                data_group: GROUP,
                job_id: config.request.job_id,
            },
            deadline,
        )
        .await
        .unwrap();
    assert!(
        matches!(scheduled, BackendReply::Reconcile { data_group, job_id, result: Ok(ReconcileStatus::Scheduled) } if data_group == GROUP && job_id == config.request.job_id)
    );
    // backend() joined the original typed Link after its actual response.
    wire::write(
        &report.join("scheduled-response-not-reply-loss.bin"),
        &scheduled,
    );
    let Receipt::Held(held) = resources
        .child
        .as_mut()
        .unwrap()
        .receipt(1, "held", deadline)
        .await
    else {
        panic!("actual held receipt required");
    };
    require_joint(held.applied, &held.membership, &endpoints, baseline);
    all_parent_joint(resources, &held, baseline, &endpoints, deadline, report).await;
    assert_eq!(resources.metadata(&config, deadline).await.1, pending);
    get(
        resources.client.as_ref().unwrap(),
        sibling.clone(),
        Some(value.clone()),
        deadline,
    )
    .await;
    let control = Bytes::from_static(b"sibling commits while B native call held");
    put(
        resources.client.as_ref().unwrap(),
        sibling.clone(),
        control.clone(),
    )
    .await;
    get(
        resources.client.as_ref().unwrap(),
        sibling.clone(),
        Some(control.clone()),
        deadline,
    )
    .await;
    for node in &resources.nodes {
        assert!(node.service.movement.tasks.lock().unwrap().is_empty());
    }
    resources
        .child
        .as_mut()
        .unwrap()
        .send(ChildCommand::ConfirmHeld);
    let Receipt::Held(confirmed) = resources
        .child
        .as_mut()
        .unwrap()
        .receipt(2, "held", deadline)
        .await
    else {
        panic!("fresh held confirmation required");
    };
    assert_eq!(confirmed, held);
    kill_exact_child(resources, report);
    inspect_offline(&config, &held, baseline, &keys, &value, report).await;
    // Local all-store receipts above are not post-kill authority. This separate
    // TCP observation performs a new barrier using both joint quorum halves.
    let (survivor, joint_authority) = resources.authority(&config, &[0, 2, 3], deadline).await;
    assert_eq!(
        topology(&joint_authority, &intent).unwrap(),
        Topology::Joint
    );
    assert_eq!(
        native(joint_authority.membership),
        held.membership.log_id().unwrap()
    );
    assert!(covers(
        Some(native(joint_authority.applied)),
        *held.membership.log_id()
    ));
    assert!(covers(
        Some(native(joint_authority.applied)),
        Some(baseline)
    ));
    require_observed_peers(&joint_authority, &endpoints, false);
    wire::write(
        &report.join("post-kill-actual-joint-authority.bin"),
        &(survivor, joint_authority),
    );
    assert_eq!(resources.metadata(&config, deadline).await.1, pending);
    for node in &resources.nodes {
        assert!(node.service.movement.tasks.lock().unwrap().is_empty());
    }
    resources.child.take();
    let mut reopened = config.clone();
    reopened.incarnation = 2;
    reopened.reopen = true;
    reopened.files = report.join("incarnation-2");
    resources.launch(reopened.clone());
    require_ready(resources, &reopened, deadline).await;
    let (authority, resumed_observation) = resources
        .authority(&reopened, &[0, 1, 2, 3], deadline)
        .await;
    assert_eq!(
        topology(&resumed_observation, &intent).unwrap(),
        Topology::Joint
    );
    assert_eq!(resources.metadata(&reopened, deadline).await.1, pending);
    let resumed = resources
        .backend(
            &authority,
            directory.cluster_id,
            BackendOperation::ReconcileMove {
                data_group: GROUP,
                job_id: config.request.job_id,
            },
            deadline,
        )
        .await
        .unwrap();
    assert!(
        matches!(resumed, BackendReply::Reconcile { data_group, job_id, result: Ok(ReconcileStatus::Scheduled) } if data_group == GROUP && job_id == config.request.job_id)
    );
    let completed = loop {
        let (_, state) = resources.metadata(&reopened, deadline).await;
        if matches!(
            state.record.as_ref().map(|record| &record.progress),
            Some(MoveProgress::Completed { .. })
        ) {
            break state;
        }
        assert_eq!(state, pending);
        assert!(Instant::now() < deadline);
        sleep(Duration::from_millis(10)).await;
    };
    require_completed(&completed, &pending, &intent, &endpoints);
    let (_, target) = resources.authority(&reopened, &[0, 1, 3], deadline).await;
    require_target(&target, &intent).unwrap();
    require_observed_peers(&target, &endpoints, true);
    let MoveProgress::Completed { membership } = completed.record.as_ref().unwrap().progress else {
        unreachable!();
    };
    let published = LogId::new(
        openraft::CommittedLeaderId::new(membership.term, membership.leader_node_id.get()),
        membership.index,
    );
    assert!(covers(Some(native(target.applied)), Some(published)));
    assert!(covers(Some(native(target.membership)), Some(published)));
    assert!(covers(Some(native(target.applied)), Some(baseline)));
    wire::write(&report.join("exact-completed.bin"), &completed);
    wire::write(&report.join("fresh-target-authority.bin"), &target);
    resources
        .child
        .as_mut()
        .unwrap()
        .send(ChildCommand::ObserveFinal {
            published,
            baseline,
        });
    let Receipt::LocalTarget {
        applied,
        membership,
    } = resources
        .child
        .as_mut()
        .unwrap()
        .receipt(1, "target", deadline)
        .await
    else {
        panic!("restarted B local target required");
    };
    require_local_target(applied, &membership, &reopened, published, baseline);
    let client = resources.client.as_ref().unwrap();
    get(client, keys[0].clone(), Some(value.clone()), deadline).await;
    get(client, keys[1].clone(), None, deadline).await;
    get(client, sibling.clone(), Some(control), deadline).await;
    put(client, keys[2].clone(), value.clone()).await;
    get(client, keys[2].clone(), Some(value), deadline).await;
    delete(client, keys[2].clone()).await;
    get(client, keys[2].clone(), None, deadline).await;
    let conflicting = MoveReplica {
        job_id: MoveJobId::new([231; 16]).unwrap(),
        data_group: GROUP,
        remove_node: NodeId::new(4).unwrap(),
        add_node: endpoints[2].clone(),
        expected_directory_revision: 2,
    };
    assert_ne!(conflicting.job_id, config.request.job_id);
    conflicting.derive_intent(&completed.directory).unwrap();
    let (metadata, state) = resources.metadata(&reopened, deadline).await;
    assert_eq!(state, completed);
    let rejection = resources
        .backend(
            &metadata,
            directory.cluster_id,
            BackendOperation::MoveReplica(conflicting),
            deadline,
        )
        .await
        .unwrap();
    assert!(matches!(
        rejection,
        BackendReply::Move {
            result: AdminResult::Conflict { .. },
            ..
        }
    ));
    assert_eq!(resources.metadata(&reopened, deadline).await.1, completed);
    wire::write(
        &report.join("valid-conflicting-job-rejected.bin"),
        &rejection,
    );
    wire::write(&report.join("all-public-invariants-verified.bin"), &true);
}

async fn require_ready(resources: &mut Resources, config: &Config, deadline: Instant) {
    let ready = resources
        .child
        .as_mut()
        .unwrap()
        .receipt(
            0,
            "ready",
            deadline.min(Instant::now() + Duration::from_secs(20)),
        )
        .await;
    assert!(
        matches!(ready, Receipt::Ready { endpoint, reopen, runner_count: 0 } if endpoint == config.endpoints[1] && reopen == config.reopen)
    );
}
fn keys_for(directory: &ClusterDirectory, group: GroupId, count: usize) -> Vec<Bytes> {
    let keys: Vec<_> = (0..1000)
        .map(|index| Bytes::from(format!("joint-process-key-{index}")))
        .filter(|key| directory.shard_config.group_for_key(key).unwrap() == group)
        .take(count)
        .collect();
    assert_eq!(keys.len(), count);
    keys
}
async fn put(client: &RockBeeRaftClient, key: Bytes, value: Bytes) {
    // Submit exactly once; an uncertain mutation is never replayed.
    assert_eq!(
        client.put(key, value).unwrap().await,
        MutationOutcome::Committed
    );
}
async fn delete(client: &RockBeeRaftClient, key: Bytes) {
    assert_eq!(
        client.delete(key).unwrap().await,
        MutationOutcome::Committed
    );
}
async fn get(client: &RockBeeRaftClient, key: Bytes, expected: Option<Bytes>, deadline: Instant) {
    loop {
        match client.get(key.clone()).await {
            Ok(actual) => {
                assert_eq!(
                    actual, expected,
                    "successful wrong value/absence is never hidden by polling"
                );
                return;
            }
            Err(error) => assert!(
                Instant::now() < deadline,
                "public GET transport/authority deadline: {error}"
            ),
        }
        sleep(Duration::from_millis(10)).await;
    }
}
async fn all_parent_joint(
    resources: &Resources,
    held: &Held,
    baseline: LogId,
    endpoints: &[NodeEndpoint],
    deadline: Instant,
    report: &Path,
) {
    loop {
        let mut observed = Vec::new();
        for node in &resources.nodes {
            let (applied, membership) = node.service.groups[&GROUP]
                .reader
                .read_membership()
                .await
                .unwrap();
            observed.push((node.config.node_id, applied, membership));
        }
        if observed.iter().all(|(_, applied, membership)| {
            *membership == held.membership
                && covers(*applied, *membership.log_id())
                && covers(*applied, Some(baseline))
        }) {
            for (_, applied, membership) in &observed {
                require_joint(*applied, membership, endpoints, baseline);
            }
            wire::write(
                &report.join("all-parent-local-joint-not-authority.bin"),
                &observed,
            );
            return;
        }
        assert!(
            Instant::now() < deadline,
            "all four exact coherent joint identities required: {observed:?}"
        );
        sleep(Duration::from_millis(10)).await;
    }
}
#[derive(Serialize)]
struct KillReceipt {
    nonce: [u8; 16],
    incarnation: u8,
    pid: u32,
    confirmed_sequence: u8,
    group: GroupId,
    job: MoveJobId,
    signal: Option<i32>,
    status: String,
    confirmed_then_kill_then_wait: bool,
}
fn kill_exact_child(resources: &mut Resources, report: &Path) {
    let child = resources.child.as_mut().unwrap();
    assert_eq!(child.sequence, 2);
    assert!(
        child.child.try_wait().unwrap().is_none(),
        "already exited child fails the fault premise"
    );
    let pid = child.child.id();
    child.child.kill().unwrap();
    let status = child.child.wait().unwrap();
    child.reaped = true;
    #[cfg(unix)]
    let signal = {
        use std::os::unix::process::ExitStatusExt;
        status.signal()
    };
    #[cfg(not(unix))]
    let signal = None;
    #[cfg(unix)]
    assert_eq!(
        signal,
        Some(9),
        "owned B must terminate abruptly by SIGKILL"
    );
    assert!(!status.success());
    wire::write(
        &report.join("actual-kill-and-wait.bin"),
        &KillReceipt {
            nonce: child.config.nonce,
            incarnation: child.config.incarnation,
            pid,
            confirmed_sequence: 2,
            group: GROUP,
            job: child.config.request.job_id,
            signal,
            status: status.to_string(),
            confirmed_then_kill_then_wait: true,
        },
    );
}
async fn inspect_offline(
    config: &Config,
    held: &Held,
    baseline: LogId,
    keys: &[Bytes],
    value: &Bytes,
    report: &Path,
) {
    let storage = Storage::open(
        config.database.clone(),
        StorageIdentity {
            cluster_id: config.directory.cluster_id,
            node_id: NodeId::new(2).unwrap(),
            shard_config: config.directory.shard_config,
        },
        StorageOptions::default(),
        OpenMode::OpenExisting,
    )
    .await
    .unwrap();
    let stores = storage.open_group(GROUP).unwrap();
    let reader = stores.state_machine.reader();
    let (applied, membership) = reader.read_membership().await.unwrap();
    assert_eq!(membership, held.membership);
    require_joint(applied, &membership, &config.endpoints, baseline);
    let retained = reader.read_local(keys[0].clone()).await.unwrap();
    let deleted = reader.read_local(keys[1].clone()).await.unwrap();
    assert_eq!(retained.value.as_ref(), Some(value));
    assert_eq!(deleted.value, None);
    assert!(covers(retained.applied, Some(baseline)) && covers(deleted.applied, Some(baseline)));
    wire::write(
        &report.join("offline-b-same-database-joint.bin"),
        &(applied, membership, true, true),
    );
    drop(reader);
    drop(stores);
    drop(storage);
}
fn require_observed_peers(observed: &AppliedMembership, endpoints: &[NodeEndpoint], target: bool) {
    assert_eq!(
        observed.peers.iter().flatten().count(),
        if target { 3 } else { 4 }
    );
    for endpoint in endpoints {
        assert_eq!(
            observed.peer(endpoint.node_id),
            (!(target && endpoint.node_id.get() == 3)).then_some(endpoint.peer_address)
        );
    }
}
fn require_completed(
    state: &MoveState,
    pending: &MoveState,
    intent: &MoveIntent,
    endpoints: &[NodeEndpoint],
) {
    assert_eq!(state.directory.revision, 2);
    assert_eq!(state.directory.voters(GROUP).unwrap(), intent.target_voters);
    assert_eq!(state.record.as_ref().unwrap().intent, *intent);
    assert!(matches!(
        state.record.as_ref().unwrap().progress,
        MoveProgress::Completed { .. }
    ));
    assert_eq!(state.directory.nodes, endpoints);
    assert_eq!(state.directory.cluster_id, pending.directory.cluster_id);
    assert_eq!(state.directory.shard_config, pending.directory.shard_config);
    for endpoint in endpoints {
        assert_eq!(state.directory.node(endpoint.node_id), Some(endpoint));
    }
    for group in [GroupId::new(0), GroupId::new(2)] {
        assert_eq!(
            state.directory.voters(group),
            pending.directory.voters(group)
        );
    }
}
