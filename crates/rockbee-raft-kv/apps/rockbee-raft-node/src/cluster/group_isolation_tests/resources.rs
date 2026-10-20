//! Owns the real three-node fixture until every accepted native call has ended.

use super::*;
use crate::{config::NodeConfig, rpc::PeerLimits, storage::OpenMode};
use rockbee_raft_gateway::{GatewayConfig, SeedNode};
use rockbee_raft_protocol::{ClusterId, NodeId, ShardConfig, routing::NodeEndpoint};
use tokio::net::TcpListener;

pub(super) struct Resources {
    pub(super) directory: ClusterDirectory,
    pub(super) nodes: Vec<NodeRuntime>,
    pub(super) relays: PeerRelays,
    pub(super) gateway: Option<GatewayRuntime>,
    pub(super) gateway_address: std::net::SocketAddr,
    pub(super) clients: Vec<RockBeeRaftClient>,
    roots: Vec<tempfile::TempDir>,
}

impl Resources {
    /// Only binds fixture listeners; the outer test owns this value before
    /// starting any native database constructor.
    pub(super) async fn prepare()
    -> Result<(Self, Vec<(TcpListener, TcpListener)>, TcpListener), String> {
        let cluster = ClusterId::new([113; 16]).map_err(message)?;
        let mut endpoints = Vec::new();
        let mut listeners = Vec::new();
        let mut forwarded = Vec::new();
        for id in 1..=3 {
            let node_id = NodeId::new(id).map_err(message)?;
            let advertised = TcpListener::bind("127.0.0.1:0").await.map_err(message)?;
            let private = TcpListener::bind("127.0.0.1:0").await.map_err(message)?;
            let data = TcpListener::bind("127.0.0.1:0").await.map_err(message)?;
            endpoints.push(NodeEndpoint {
                node_id,
                peer_address: advertised.local_addr().map_err(message)?,
                data_address: data.local_addr().map_err(message)?,
            });
            forwarded.push((node_id, advertised, private.local_addr().map_err(message)?));
            listeners.push((private, data));
        }
        let directory =
            ClusterDirectory::initial(cluster, ShardConfig::new(2, 0).map_err(message)?, endpoints)
                .map_err(message)?;
        let public = TcpListener::bind("127.0.0.1:0").await.map_err(message)?;
        let gateway_address = public.local_addr().map_err(message)?;
        let resources = Self {
            directory,
            nodes: Vec::new(),
            relays: PeerRelays::start(cluster, forwarded, PeerLimits::default()),
            gateway: None,
            gateway_address,
            clients: Vec::new(),
            roots: Vec::new(),
        };
        Ok((resources, listeners, public))
    }

    pub(super) async fn start_nodes(
        &mut self,
        listeners: Vec<(TcpListener, TcpListener)>,
        public: TcpListener,
    ) -> Result<(), String> {
        for (endpoint, (peer, data)) in self.directory.nodes.iter().zip(listeners) {
            let root = tempfile::tempdir().map_err(message)?;
            let config = NodeConfig::new(
                self.directory.clone(),
                endpoint.node_id,
                root.path().join("db"),
                OpenMode::CreateNew,
            )
            .map_err(message)?;
            self.roots.push(root);
            let node = NodeRuntime::start_with_peer_relay(config, peer, data, &self.relays)
                .await
                .map_err(message)?;
            // Fix only this fixture's election schedule. All byte/count limits,
            // heartbeat intervals and production runtime defaults stay intact.
            for group in node.service.groups.values() {
                group.raft.runtime_config().elect(false);
            }
            self.nodes.push(node);
        }
        self.nodes[0].initialize_groups().await.map_err(message)?;
        self.nodes[0]
            .bootstrap_directory(Instant::now() + OBSERVATION_LIMIT)
            .await
            .map_err(message)?;
        for group in [GroupId::new(0), HEALTHY] {
            authority(&self.nodes[0], group).await?;
            for node in &self.nodes {
                node.service.groups[&group]
                    .raft
                    .runtime_config()
                    .elect(true);
            }
        }
        authority(&self.nodes[0], STALLED).await?;
        self.gateway = Some(
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
                    listen_address: self.gateway_address,
                    backend_link: Default::default(),
                    server: ServerConfig::default(),
                    startup_timeout: OBSERVATION_LIMIT,
                },
                public,
            )
            .await
            .map_err(message)?,
        );
        Ok(())
    }

    pub(super) async fn client(&mut self) -> Result<usize, String> {
        let client = RockBeeRaftClient::connect(self.gateway_address, ClientConfig::default())
            .await
            .map_err(message)?;
        self.clients.push(client);
        Ok(self.clients.len() - 1)
    }

    pub(super) fn heal(&self) {
        self.relays.heal();
        for node in &self.nodes {
            for group in node.service.groups.values() {
                group.raft.runtime_config().elect(true);
                group.raft.runtime_config().heartbeat(true);
            }
        }
    }

    pub(super) async fn close(&mut self) {
        self.heal();
        for client in &self.clients {
            client.close().await;
        }
        if let Some(gateway) = self.gateway.as_mut() {
            gateway.close().await;
        }
        for node in &mut self.nodes {
            node.close().await;
        }
        self.relays.close().await;
        log::info!(
            "raft_group_isolation_cleanup nodes={} relays=joined",
            self.nodes.len()
        );
    }
}
