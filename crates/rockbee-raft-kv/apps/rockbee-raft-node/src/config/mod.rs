//! Validated process configuration, separate from startup and request execution.
use std::path::PathBuf;

use rockbee_raft_protocol::{
    NodeId,
    routing::{ClusterDirectory, NodeEndpoint},
    transport::server::ServerConfig,
};

use crate::{
    cluster::NodeError,
    rpc::PeerLimits,
    storage::{OpenMode, StorageOptions},
};

/// Settings for one storage process. All initial nodes use the same directory.
#[derive(Clone, Debug)]
pub struct NodeConfig {
    /// Canonical immutable placement used for first bootstrap and restart checks.
    pub directory: ClusterDirectory,
    /// This process's stable identity in that placement.
    pub node_id: NodeId,
    /// RocksDB and owned snapshot files live below this path.
    pub data_directory: PathBuf,
    /// Explicit creation/recovery intent; missing data is never silently recreated.
    pub open_mode: OpenMode,
    /// Shared process storage budgets, not per-group caches.
    pub storage: StorageOptions,
    /// Shared replication limits, separate from public/backend data traffic.
    pub peers: PeerLimits,
    /// Aggregate admission for gateway-to-node requests.
    pub server: ServerConfig,
}

impl NodeConfig {
    /// Constructs validated settings with ordinary storage/transport defaults.
    pub fn new(
        directory: ClusterDirectory,
        node_id: NodeId,
        data_directory: PathBuf,
        open_mode: OpenMode,
    ) -> Result<Self, NodeError> {
        let config = Self {
            directory,
            node_id,
            data_directory,
            open_mode,
            storage: StorageOptions::default(),
            peers: PeerLimits::default(),
            server: ServerConfig::default(),
        };
        config.validate()?;
        Ok(config)
    }

    /// Rejects invalid settings before opening a database or spawning Raft groups.
    pub fn validate(&self) -> Result<(), NodeError> {
        self.directory.validate()?;
        self.endpoint()?;
        if self.data_directory.as_os_str().is_empty() {
            return Err(NodeError::Config("data directory cannot be empty".into()));
        }
        self.storage.validate()?;
        self.peers.validate()?;
        self.server
            .validate()
            .map_err(|error| NodeError::Config(error.to_string()))?;
        Ok(())
    }

    /// Finds this node's advertised addresses without inventing fallback ports.
    pub fn endpoint(&self) -> Result<&NodeEndpoint, NodeError> {
        self.directory
            .node(self.node_id)
            .ok_or_else(|| NodeError::Config("node is absent from configured directory".into()))
    }
}
