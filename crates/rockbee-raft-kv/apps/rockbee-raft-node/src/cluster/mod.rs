//! Process lifecycle and group orchestration; storage and RPC keep their own jobs.
mod admission;
mod bootstrap;
#[cfg(all(test, feature = "diagnostics"))]
mod group_isolation_tests;
#[cfg(test)]
#[path = "../../../rockbee-raft-gateway/tests/support/history.rs"]
// Shared by node fixtures; the gateway suite exercises the full model.
#[allow(dead_code)]
mod history_model;
mod join;
mod movement;
#[cfg(test)]
mod public_reply_loss_tests;
mod service;

use std::{collections::BTreeMap, sync::Arc};

use log::{error, info};
use openraft::{BasicNode, Config, Raft};
use rockbee_raft_protocol::{
    GroupId, NodeId, ProtocolError,
    transport::server::{Server, ServerError},
};
use tokio::net::TcpListener;

use crate::{
    config::{JoinConfig, NodeConfig, ResolvedStartup},
    rpc::{PeerError, PeerIdentity, PeerNetworkFactory, PeerResources, PeerServer},
    storage::{Storage, StorageIdentity, StorageOpenError},
    types::TypeConfig,
};

use service::HostedGroup;
pub use service::NodeService;

/// Startup errors preserve the boundary that rejected the operation.
#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("invalid node configuration: {0}")]
    Config(String),
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    #[error(transparent)]
    Storage(#[from] StorageOpenError),
    #[error(transparent)]
    Peer(#[from] PeerError),
    #[error(transparent)]
    Server(#[from] ServerError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("raft operation failed: {0}")]
    Raft(String),
}

/// Owns one database, multiple Raft groups, and separate peer/data listeners.
pub struct NodeRuntime {
    config: ResolvedStartup,
    service: Arc<NodeService>,
    peer_server: PeerServer,
    data_server: Server,
    _storage: Storage,
    #[cfg(feature = "diagnostics")]
    diagnostic_tasks: tokio::task::JoinSet<()>,
    closed: bool,
}

impl NodeRuntime {
    /// Discovers current metadata before opening an uninitialized data node.
    pub async fn start_join(config: JoinConfig) -> Result<Self, NodeError> {
        config.validate()?;
        let deadline = tokio::time::Instant::now() + config.startup_timeout;
        join::discover_join_directory(&config, deadline).await?;
        let peer = TcpListener::bind(config.local_endpoint.peer_address).await?;
        let data = TcpListener::bind(config.local_endpoint.data_address).await?;
        Self::start_resolved(ResolvedStartup::join(config), peer, data).await
    }

    /// Join equivalent with owned listeners for race-free startup.
    pub async fn start_join_with_listeners(
        config: JoinConfig,
        peer: TcpListener,
        data: TcpListener,
    ) -> Result<Self, NodeError> {
        config.validate()?;
        check_listeners(&config.local_endpoint, &peer, &data)?;
        let deadline = tokio::time::Instant::now() + config.startup_timeout;
        join::discover_join_directory(&config, deadline).await?;
        Self::start_resolved(ResolvedStartup::join(config), peer, data).await
    }

    /// Binds configured listeners, then recovers/creates the local groups.
    pub async fn start(config: NodeConfig) -> Result<Self, NodeError> {
        config.validate()?;
        let endpoint = config.endpoint()?;
        let peer = TcpListener::bind(endpoint.peer_address).await?;
        let data = TcpListener::bind(endpoint.data_address).await?;
        Self::start_with_listeners(config, peer, data).await
    }

    /// Starts with owned listeners, allowing race-free ephemeral ports in tests.
    pub async fn start_with_listeners(
        config: NodeConfig,
        peer: TcpListener,
        data: TcpListener,
    ) -> Result<Self, NodeError> {
        Self::start_resolved(ResolvedStartup::bootstrap(config)?, peer, data).await
    }

    async fn start_resolved(
        config: ResolvedStartup,
        peer: TcpListener,
        data: TcpListener,
    ) -> Result<Self, NodeError> {
        check_listeners(&config.endpoint, &peer, &data)?;
        Self::start_validated(
            config,
            peer,
            data,
            #[cfg(test)]
            None,
        )
        .await
    }

    /// Only fixture-owned relay mappings may differ from the private listener.
    #[cfg(test)]
    async fn start_with_peer_relay(
        config: NodeConfig,
        peer: TcpListener,
        data: TcpListener,
        relays: &crate::rpc::test_support::PeerRelays,
    ) -> Result<Self, NodeError> {
        let config = ResolvedStartup::bootstrap(config)?;
        check_relay_listeners(&config.endpoint, &peer, &data, relays)?;
        Self::start_validated(config, peer, data, Some(relays.dispatch_tracker())).await
    }

    #[cfg(test)]
    async fn start_join_with_peer_relay(
        config: JoinConfig,
        peer: TcpListener,
        data: TcpListener,
        relays: &crate::rpc::test_support::PeerRelays,
    ) -> Result<Self, NodeError> {
        config.validate()?;
        check_relay_listeners(&config.local_endpoint, &peer, &data, relays)?;
        let deadline = tokio::time::Instant::now() + config.startup_timeout;
        join::discover_join_directory(&config, deadline).await?;
        Self::start_validated(
            ResolvedStartup::join(config),
            peer,
            data,
            Some(relays.dispatch_tracker()),
        )
        .await
    }

    async fn start_validated(
        config: ResolvedStartup,
        peer: TcpListener,
        data: TcpListener,
        #[cfg(test)] tracker: Option<crate::rpc::test_support::DispatchTracker>,
    ) -> Result<Self, NodeError> {
        let identity = PeerIdentity {
            cluster_id: config.cluster_id,
            node_id: config.node_id,
        };
        info!("raft_node_start node={}", identity.node_id.get());
        let resources = Arc::new(PeerResources::new(config.peers)?);
        let storage = Storage::open(
            config.data_directory.clone(),
            StorageIdentity {
                cluster_id: identity.cluster_id,
                node_id: identity.node_id,
                shard_config: config.shard_config,
            },
            config.storage.clone(),
            config.open_mode,
        )
        .await?;
        let groups = start_groups(&config, &storage, identity, &resources).await?;
        info!(
            "raft_node_groups_ready node={} groups={}",
            identity.node_id.get(),
            groups.len()
        );
        let service = Arc::new(NodeService::new(
            identity,
            config.shard_config,
            config.bootstrap_origin.clone(),
            config.storage.clone(),
            config.peers,
            groups,
            config.metadata_seeds.clone(),
        ));
        let peer_groups = service
            .groups
            .iter()
            .map(|(id, group)| (*id, group.raft.clone()))
            .collect();
        #[cfg(test)]
        let mut peer_server = match tracker {
            Some(tracker) => PeerServer::start_with_dispatch_tracker(
                peer,
                identity,
                peer_groups,
                resources,
                tracker,
            ),
            None => PeerServer::start(peer, identity, peer_groups, resources),
        };
        #[cfg(not(test))]
        let mut peer_server = PeerServer::start(peer, identity, peer_groups, resources);
        let data_server = match Server::start(data, config.server, Arc::clone(&service)) {
            Ok(server) => server,
            Err(error) => {
                error!(
                    "raft_data_server_start_failed node={} error={}",
                    identity.node_id.get(),
                    error
                );
                stop_groups(&service.groups).await;
                peer_server.close().await;
                return Err(error.into());
            }
        };
        #[cfg(feature = "diagnostics")]
        let diagnostic_tasks = observe_group_states(config.node_id, &service);
        Ok(Self {
            config,
            service,
            peer_server,
            data_server,
            _storage: storage,
            #[cfg(feature = "diagnostics")]
            diagnostic_tasks,
            closed: false,
        })
    }

    /// Initializes pristine groups from one designated node, never from each peer.
    /// Existing group histories are retained; the metadata bootstrap checks the
    /// stored directory separately before declaring the cluster ready.
    pub async fn initialize_groups(&self) -> Result<(), NodeError> {
        self.require_initializer()?;
        for (id, group) in &self.service.groups {
            if group
                .raft
                .is_initialized()
                .await
                .map_err(|error| NodeError::Raft(error.to_string()))?
            {
                continue;
            }
            let origin = self.config.origin()?;
            let voters = origin
                .voters(*id)
                .ok_or_else(|| NodeError::Config("missing group placement".into()))?;
            let mut members = BTreeMap::new();
            for voter in voters {
                let node = origin
                    .node(*voter)
                    .ok_or_else(|| NodeError::Config("unknown configured voter".into()))?;
                members.insert(voter.get(), BasicNode::new(node.peer_address.to_string()));
            }
            group
                .raft
                .initialize(members)
                .await
                .map_err(|error| NodeError::Raft(error.to_string()))?;
            info!(
                "raft_group_initialized node={} group={}",
                self.config.node_id.get(),
                id.get()
            );
        }
        Ok(())
    }

    fn require_initializer(&self) -> Result<(), NodeError> {
        let initializer = self
            .config
            .origin()?
            .nodes
            .iter()
            .map(|node| node.node_id)
            .min();
        if initializer != Some(self.config.node_id) {
            return Err(NodeError::Config(
                "only the lowest configured node ID initializes groups".into(),
            ));
        }
        Ok(())
    }

    /// A diagnostic hint only. Requests still require the destination's authority.
    pub fn leader_hint(&self, group: GroupId) -> Option<NodeId> {
        self.service.leader_hint(group)
    }

    /// Applied log progress for diagnostics and convergence assertions.
    pub fn applied_index(&self, group: GroupId) -> Option<u64> {
        self.service
            .groups
            .get(&group)?
            .raft
            .metrics()
            .borrow()
            .last_applied
            .map(|id| id.index)
    }

    /// Exposes the node's typed backend service, without exposing public-client internals.
    pub fn service(&self) -> Arc<NodeService> {
        Arc::clone(&self.service)
    }

    /// Stops admission, resolves Raft work, then joins both listener task trees.
    /// Accepted writes retain their server permits until their Raft call ends.
    pub async fn close(&mut self) {
        if self.closed {
            return;
        }
        info!("raft_node_stop node={}", self.config.node_id.get());
        self.data_server.stop_accepting();
        self.service.movement.stop();
        stop_groups(&self.service.groups).await;
        self.service.movement.drain().await;
        self.data_server.close().await;
        self.peer_server.close().await;
        #[cfg(feature = "diagnostics")]
        {
            self.diagnostic_tasks.abort_all();
            while self.diagnostic_tasks.join_next().await.is_some() {}
        }
        self.closed = true;
        info!("raft_node_stopped node={}", self.config.node_id.get());
    }
}

/// Watches the library's actual term/role metrics; notification coalescing means
/// these are observation times, not an exhaustive log of every transition.
#[cfg(feature = "diagnostics")]
fn observe_group_states(node: NodeId, service: &NodeService) -> tokio::task::JoinSet<()> {
    let mut tasks = tokio::task::JoinSet::new();
    if !crate::diagnostics::enabled() {
        return tasks;
    }
    for (id, group) in &service.groups {
        let id = id.get();
        let mut metrics = group.raft.metrics();
        tasks.spawn(async move {
            let mut previous = None;
            loop {
                let state = {
                    let current = metrics.borrow_and_update();
                    (current.current_term, current.state, current.current_leader)
                };
                if previous.as_ref() != Some(&state) {
                    crate::diagnostics::Span::start(node.get(), id, None, "raft_state", false)
                        .note(&format_args!(
                            "term={} role={:?} leader={:?}",
                            state.0, state.1, state.2
                        ));
                    previous = Some(state);
                }
                if metrics.changed().await.is_err() {
                    break;
                }
            }
        });
    }
    tasks
}

async fn start_groups(
    config: &ResolvedStartup,
    storage: &Storage,
    identity: PeerIdentity,
    resources: &Arc<PeerResources>,
) -> Result<BTreeMap<GroupId, HostedGroup>, NodeError> {
    let mut groups = BTreeMap::new();
    let first = if config.bootstrap_origin.is_some() {
        0
    } else {
        1
    };
    let hosted_groups = usize::try_from(u64::from(config.shard_config.count()) + 1 - first)
        .map_err(|_| NodeError::Config("hosted group count exceeds this platform".into()))?;
    let mutation_capacity = admission::mutation_capacity(config.server, hosted_groups);
    for index in first..=u64::from(config.shard_config.count()) {
        let id = GroupId::new(index);
        let result = start_group(config, storage, identity, resources, id, mutation_capacity).await;
        match result {
            Ok(group) => {
                groups.insert(id, group);
            }
            Err(error) => {
                stop_groups(&groups).await;
                return Err(error);
            }
        }
    }
    Ok(groups)
}

async fn start_group(
    config: &ResolvedStartup,
    storage: &Storage,
    identity: PeerIdentity,
    resources: &Arc<PeerResources>,
    id: GroupId,
    mutation_capacity: usize,
) -> Result<HostedGroup, NodeError> {
    let stores = storage.open_group(id)?;
    let reader = stores.state_machine.reader();
    // Match explicit wire bounds; other OpenRaft timing/runtime defaults stay intact.
    let raft_config = Config {
        cluster_name: format!("rockbee/group-{}", id.get()),
        max_payload_entries: 128,
        snapshot_max_chunk_size: 256 * 1024,
        ..Config::default()
    }
    .validate()
    .map_err(|error| NodeError::Config(error.to_string()))?;
    let raft = Raft::<TypeConfig>::new(
        config.node_id.get(),
        Arc::new(raft_config),
        PeerNetworkFactory::new(identity, id, Arc::clone(resources)),
        stores.log_store,
        stores.state_machine,
    )
    .await
    .map_err(|error| NodeError::Raft(error.to_string()))?;
    Ok(HostedGroup {
        raft,
        reader,
        mutation_admission: Arc::new(tokio::sync::Semaphore::new(mutation_capacity)),
    })
}

async fn stop_groups(groups: &BTreeMap<GroupId, HostedGroup>) {
    for group in groups.values() {
        let _ = group.raft.shutdown().await;
    }
}

fn check_listeners(
    endpoint: &rockbee_raft_protocol::routing::NodeEndpoint,
    peer: &TcpListener,
    data: &TcpListener,
) -> Result<(), NodeError> {
    if peer.local_addr()? != endpoint.peer_address || data.local_addr()? != endpoint.data_address {
        return Err(NodeError::Config(
            "listeners do not match advertised node addresses".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
fn check_relay_listeners(
    endpoint: &rockbee_raft_protocol::routing::NodeEndpoint,
    peer: &TcpListener,
    data: &TcpListener,
    relays: &crate::rpc::test_support::PeerRelays,
) -> Result<(), NodeError> {
    if data.local_addr()? != endpoint.data_address
        || !relays.matches_binding(endpoint.node_id, endpoint.peer_address, peer.local_addr()?)
    {
        return Err(NodeError::Config(
            "listeners do not match owned test relay mapping".into(),
        ));
    }
    Ok(())
}
