//! Settings and discovery candidates for a node joining an existing cluster.
use crate::{
    cluster::NodeError,
    rpc::PeerLimits,
    storage::{OpenMode, StorageOptions},
};
use rockbee_raft_protocol::{
    ClusterId, NodeId, ShardConfig, routing::NodeEndpoint, transport::server::ServerConfig,
};
use std::{collections::BTreeSet, net::SocketAddr, path::PathBuf, time::Duration};

/// One backend seed is a discovery candidate, never metadata authority.
#[derive(Clone, Copy, Debug)]
pub struct JoinSeed {
    /// Stable identity expected from this existing metadata backend candidate.
    pub node_id: NodeId,
    /// Backend/data listener address, never a private Raft peer address.
    pub address: SocketAddr,
}

/// Explicit startup settings for a distinct, initially uninitialized data node.
#[derive(Clone, Debug)]
pub struct JoinConfig {
    /// This data node's sole local identity and advertised peer/backend addresses.
    pub local_endpoint: NodeEndpoint,
    /// Expected immutable cluster identity, checked against discovery and storage.
    pub cluster_id: ClusterId,
    /// Expected immutable hash/shards; a discovered directory is no bootstrap origin.
    pub shard_config: ShardConfig,
    /// One through sixteen candidates, normally all ABC backend endpoints.
    pub seeds: Vec<JoinSeed>,
    /// Original discovery budget in 1..=60000ms, shared fairly among candidates.
    pub startup_timeout: Duration,
    /// Distinct durable local database and owned snapshot directory.
    pub data_directory: PathBuf,
    /// Explicit creation/recovery intent; join never initializes Raft membership.
    pub open_mode: OpenMode,
    /// Existing shared process storage budgets and data-log/snapshot limits.
    pub storage: StorageOptions,
    /// Existing bounded private replication admission, without metadata proposals.
    pub peers: PeerLimits,
    /// Aggregate admission for gateway-to-node backend requests.
    pub server: ServerConfig,
}

impl JoinConfig {
    /// Validates join settings without creating local storage.
    pub fn validate(&self) -> Result<(), NodeError> {
        validate_local_endpoint(&self.local_endpoint)?;
        if self.seeds.is_empty() || self.seeds.len() > 16 {
            return Err(NodeError::Config(
                "join needs one through sixteen unique backend seeds".into(),
            ));
        }
        let mut ids = BTreeSet::new();
        let mut addresses = BTreeSet::new();
        for seed in &self.seeds {
            if !valid_address(seed.address)
                || !ids.insert(seed.node_id)
                || !addresses.insert(seed.address)
                || seed.node_id == self.local_endpoint.node_id
                || [
                    self.local_endpoint.peer_address,
                    self.local_endpoint.data_address,
                ]
                .contains(&seed.address)
            {
                return Err(NodeError::Config(
                    "invalid or colliding join seed identity/address".into(),
                ));
            }
        }
        if self.startup_timeout < Duration::from_millis(1)
            || self.startup_timeout > Duration::from_secs(60)
        {
            return Err(NodeError::Config(
                "join startup timeout must be between one millisecond and sixty seconds".into(),
            ));
        }
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
}

fn valid_address(address: SocketAddr) -> bool {
    address.port() != 0 && !address.ip().is_unspecified() && !address.ip().is_multicast()
}

fn validate_local_endpoint(endpoint: &NodeEndpoint) -> Result<(), NodeError> {
    if !valid_address(endpoint.peer_address)
        || !valid_address(endpoint.data_address)
        || endpoint.peer_address == endpoint.data_address
    {
        return Err(NodeError::Config(
            "local peer/backend addresses must be distinct concrete nonzero addresses".into(),
        ));
    }
    Ok(())
}
