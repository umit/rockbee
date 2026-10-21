//! Common startup values after bootstrap or join settings have been validated.
use super::{JoinConfig, JoinSeed, NodeConfig};
use crate::{
    cluster::NodeError,
    rpc::PeerLimits,
    storage::{OpenMode, StorageOptions},
};
use rockbee_raft_protocol::{
    ClusterId, NodeId, ShardConfig,
    routing::{ClusterDirectory, NodeEndpoint},
    transport::server::ServerConfig,
};
use std::path::PathBuf;

/// Values shared by both startup modes, after mode-specific validation.
#[derive(Clone, Debug)]
pub(crate) struct ResolvedStartup {
    pub node_id: NodeId,
    pub cluster_id: ClusterId,
    pub shard_config: ShardConfig,
    pub endpoint: NodeEndpoint,
    pub bootstrap_origin: Option<ClusterDirectory>,
    pub metadata_seeds: Vec<JoinSeed>,
    pub data_directory: PathBuf,
    pub open_mode: OpenMode,
    pub storage: StorageOptions,
    pub peers: PeerLimits,
    pub server: ServerConfig,
}

impl ResolvedStartup {
    pub(crate) fn bootstrap(config: NodeConfig) -> Result<Self, NodeError> {
        config.validate()?;
        Ok(Self {
            node_id: config.node_id,
            cluster_id: config.directory.cluster_id,
            shard_config: config.directory.shard_config,
            endpoint: config.endpoint()?.clone(),
            metadata_seeds: config
                .directory
                .nodes
                .iter()
                .map(|node| JoinSeed {
                    node_id: node.node_id,
                    address: node.data_address,
                })
                .collect(),
            bootstrap_origin: Some(config.directory),
            data_directory: config.data_directory,
            open_mode: config.open_mode,
            storage: config.storage,
            peers: config.peers,
            server: config.server,
        })
    }

    pub(crate) fn join(config: JoinConfig) -> Self {
        Self {
            node_id: config.local_endpoint.node_id,
            cluster_id: config.cluster_id,
            shard_config: config.shard_config,
            endpoint: config.local_endpoint,
            bootstrap_origin: None,
            metadata_seeds: config.seeds,
            data_directory: config.data_directory,
            open_mode: config.open_mode,
            storage: config.storage,
            peers: config.peers,
            server: config.server,
        }
    }

    pub(crate) fn origin(&self) -> Result<&ClusterDirectory, NodeError> {
        self.bootstrap_origin.as_ref().ok_or_else(|| {
            NodeError::Config("join nodes cannot initialize groups or bootstrap metadata".into())
        })
    }
}
