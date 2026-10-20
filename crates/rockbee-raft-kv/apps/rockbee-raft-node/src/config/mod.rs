//! Validated process configuration, separate from startup and request execution.
use std::{collections::BTreeSet, net::SocketAddr, path::PathBuf, time::Duration};

use rockbee_raft_protocol::{
    ClusterId, NodeId, ShardConfig,
    routing::{ClusterDirectory, NodeEndpoint},
    transport::server::ServerConfig,
};

use crate::{
    cluster::NodeError,
    control::MetadataCommand,
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
        validate_metadata_candidate(
            &MetadataCommand::InitializeDirectoryIfAbsent {
                initial_directory: self.directory.clone(),
            },
            &self.storage,
            self.peers,
        )?;
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

/// Reused before every internal control proposal. Input size, durable output
/// capacity and private replication admission are distinct bounded checks.
pub(crate) fn validate_metadata_candidate(
    command: &MetadataCommand,
    storage: &StorageOptions,
    peers: PeerLimits,
) -> Result<(), NodeError> {
    command.validate()?;
    let minimum = storage.metadata_minimum_chunk_bytes()?;
    if storage.io_chunk_bytes < minimum {
        return Err(NodeError::Config(format!(
            "metadata output requires chunk bytes of at least {minimum}"
        )));
    }
    let entry_bytes = crate::storage::metadata_entry_bytes(command)?;
    if entry_bytes > storage.max_entry_bytes {
        return Err(NodeError::Config(format!(
            "metadata durable entry requires {entry_bytes} bytes"
        )));
    }
    let frame_bytes = crate::rpc::metadata_append_bytes(command)?;
    if frame_bytes > peers.max_frame_bytes {
        return Err(NodeError::Config(format!(
            "metadata private append requires {frame_bytes} frame bytes"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use rockbee_raft_protocol::{ClusterId, ShardConfig};

    fn config() -> NodeConfig {
        let nodes = (1..=3)
            .map(|id| NodeEndpoint {
                node_id: NodeId::new(id).unwrap(),
                peer_address: format!("127.0.0.1:{}", 7000 + id).parse().unwrap(),
                data_address: format!("127.0.0.1:{}", 8000 + id).parse().unwrap(),
            })
            .collect();
        let directory = ClusterDirectory::initial(
            ClusterId::new([19; 16]).unwrap(),
            ShardConfig::new(1, 0).unwrap(),
            nodes,
        )
        .unwrap();
        NodeConfig::new(
            directory,
            NodeId::new(1).unwrap(),
            PathBuf::from("unused-budget-test-db"),
            OpenMode::CreateNew,
        )
        .unwrap()
    }

    #[test]
    fn node_rejects_insufficient_metadata_output_budget() {
        let mut config = config();
        config.storage.max_entry_bytes = 4096;
        // The accepted control reservation is entry + record + batch + entry
        // overhead + expanded effects. This remains a valid raw-KV budget.
        config.storage.io_chunk_bytes = config.storage.metadata_minimum_chunk_bytes().unwrap() - 1;
        assert!(config.storage.validate().is_ok());
        assert!(
            config.validate().is_err(),
            "a production node must reserve the complete metadata output budget"
        );
    }

    #[test]
    fn node_metadata_output_full_entry_and_frame_boundaries() {
        let mut config = config();
        let command = MetadataCommand::InitializeDirectoryIfAbsent {
            initial_directory: config.directory.clone(),
        };
        config.storage.max_entry_bytes = 4096;
        config.storage.io_chunk_bytes = config.storage.metadata_minimum_chunk_bytes().unwrap();
        config.validate().unwrap();
        config.storage.io_chunk_bytes -= 1;
        assert!(config.validate().is_err());
        config.storage.io_chunk_bytes = StorageOptions::default().io_chunk_bytes;

        let required = crate::storage::metadata_entry_bytes(&command).unwrap();
        let body_only = postcard::experimental::serialized_size(&command).unwrap();
        assert!(
            required > body_only,
            "assigned LogId and durable envelope must be counted"
        );
        config.storage.max_entry_bytes = required;
        config.validate().unwrap();
        config.storage.max_entry_bytes = required - 1;
        assert!(config.validate().is_err());
        config.storage.max_entry_bytes = body_only;
        assert!(config.validate().is_err());
        config.storage.max_entry_bytes = required;

        let frame = crate::rpc::metadata_append_bytes(&command).unwrap();
        config.peers.max_snapshot_chunk_bytes = 1;
        config.peers.max_frame_bytes = frame;
        config.validate().unwrap();
        config.peers.max_frame_bytes = frame - 1;
        assert!(config.validate().is_err());
    }

    #[test]
    fn node_requires_canonical_revision_one_origin_and_checked_expansion() {
        for shape in 0..4 {
            let mut config = config();
            match shape {
                0 => config.directory.revision = 2,
                1 => config.directory.nodes.swap(0, 1),
                2 => config.directory.placements.swap(0, 1),
                _ => {
                    config.directory.nodes.truncate(1);
                    for placement in &mut config.directory.placements {
                        placement.voters.truncate(1);
                    }
                }
            }
            config.directory.validate().unwrap();
            assert!(config.validate().is_err());
        }
        let storage = StorageOptions {
            max_entry_bytes: usize::MAX,
            ..StorageOptions::default()
        };
        assert!(storage.metadata_minimum_chunk_bytes().is_err());
    }

    #[test]
    fn reusable_candidate_guard_covers_later_control_operations() {
        use crate::control::MembershipObservation;
        use rockbee_raft_protocol::{
            GroupId,
            control::{CommittedLogPosition, MoveJobId, MoveReplica},
        };
        let mut config = config();
        let movement = MoveReplica {
            job_id: MoveJobId::new([1; 16]).unwrap(),
            data_group: GroupId::new(1),
            remove_node: NodeId::new(3).unwrap(),
            add_node: NodeEndpoint {
                node_id: NodeId::new(4).unwrap(),
                peer_address: "127.0.0.1:7004".parse().unwrap(),
                data_address: "127.0.0.1:8004".parse().unwrap(),
            },
            expected_directory_revision: 1,
        }
        .derive_intent(&config.directory)
        .unwrap();
        let commands = [
            MetadataCommand::BeginMove {
                intent: movement.clone(),
            },
            MetadataCommand::CompleteMove {
                expected_directory_revision: u64::MAX,
                data_group: movement.data_group,
                job_id: movement.job_id,
                observation: MembershipObservation {
                    uniform: true,
                    voters: movement.target_voters,
                    replacement_endpoint: movement.add_node,
                    membership: CommittedLogPosition {
                        term: u64::MAX,
                        leader_node_id: NodeId::new(u64::MAX).unwrap(),
                        index: u64::MAX,
                    },
                },
            },
        ];
        for command in commands {
            let entry = crate::storage::metadata_entry_bytes(&command).unwrap();
            let frame = crate::rpc::metadata_append_bytes(&command).unwrap();
            config.storage.max_entry_bytes = entry;
            config.peers.max_frame_bytes = frame;
            validate_metadata_candidate(&command, &config.storage, config.peers).unwrap();
            config.storage.max_entry_bytes -= 1;
            assert!(validate_metadata_candidate(&command, &config.storage, config.peers).is_err());
            config.storage.max_entry_bytes = entry;
            config.peers.max_frame_bytes -= 1;
            assert!(validate_metadata_candidate(&command, &config.storage, config.peers).is_err());
        }
    }
}
