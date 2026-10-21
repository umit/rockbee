//! TOML is a startup input. Runtime membership still comes from durable Raft state.

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use rockbee_raft_node::{
    config::{JoinConfig, JoinSeed, NodeConfig},
    rpc::PeerLimits,
    storage::{OpenMode, StorageOptions},
};
use rockbee_raft_protocol::{
    NodeId, ShardConfig,
    routing::{ClusterDirectory, NodeEndpoint},
    transport::server::ServerConfig,
};
use serde::Deserialize;

use super::{Arguments, Startup, parse_cluster_id};

/// Storage creation and bootstrap remain deliberate command-line actions.
pub(super) fn load(
    path: &Path,
    open_mode: OpenMode,
    initialize: bool,
) -> Result<Arguments, String> {
    let contents = std::fs::read_to_string(path)
        .map_err(|error| format!("cannot read node config {}: {error}", path.display()))?;
    let file: NodeFileConfig = toml::from_str(&contents).map_err(|error| {
        // Display includes source lines. Report the diagnostic without dumping the file.
        format!(
            "invalid node config {}: {}",
            path.display(),
            error.message()
        )
    })?;
    let config = match file {
        NodeFileConfig::Bootstrap(file) => file.into_startup(path, open_mode, initialize),
        NodeFileConfig::Join(file) => file.into_startup(path, open_mode, initialize),
    }
    .map_err(|error| format!("invalid node config {}: {error}", path.display()))?;
    Ok(Arguments { config, initialize })
}

#[derive(Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
enum NodeFileConfig {
    Bootstrap(BootstrapFileConfig),
    Join(JoinFileConfig),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BootstrapFileConfig {
    cluster_id: String,
    node_id: u64,
    data_dir: PathBuf,
    #[serde(default = "default_shards")]
    shards: u16,
    #[serde(default)]
    hash_seed: u64,
    nodes: Vec<NodeEndpointConfig>,
}

impl BootstrapFileConfig {
    fn into_startup(
        self,
        path: &Path,
        open_mode: OpenMode,
        initialize: bool,
    ) -> Result<Startup, String> {
        let cluster_id = parse_cluster_id(&self.cluster_id)?;
        let shards =
            ShardConfig::new(self.shards, self.hash_seed).map_err(|error| error.to_string())?;
        let nodes = self
            .nodes
            .into_iter()
            .map(NodeEndpointConfig::into_endpoint)
            .collect::<Result<_, _>>()?;
        let directory = ClusterDirectory::initial(cluster_id, shards, nodes)
            .map_err(|error| error.to_string())?;
        let node_id = NodeId::new(self.node_id).map_err(|error| error.to_string())?;
        if initialize && directory.nodes.first().map(|node| node.node_id) != Some(node_id) {
            return Err("--initialize is allowed only on the lowest configured node ID".into());
        }
        let data_directory = resolve_data_directory(path, self.data_dir)?;
        NodeConfig::new(directory, node_id, data_directory, open_mode)
            .map(Startup::Bootstrap)
            .map_err(|error| error.to_string())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JoinFileConfig {
    cluster_id: String,
    data_dir: PathBuf,
    #[serde(default = "default_shards")]
    shards: u16,
    #[serde(default)]
    hash_seed: u64,
    #[serde(default = "default_join_timeout_ms")]
    startup_timeout_ms: u64,
    local_node: NodeEndpointConfig,
    seeds: Vec<SeedConfig>,
}

impl JoinFileConfig {
    fn into_startup(
        self,
        path: &Path,
        open_mode: OpenMode,
        initialize: bool,
    ) -> Result<Startup, String> {
        if initialize {
            return Err("join mode cannot use --initialize".into());
        }
        let config = JoinConfig {
            local_endpoint: self.local_node.into_endpoint()?,
            cluster_id: parse_cluster_id(&self.cluster_id)?,
            shard_config: ShardConfig::new(self.shards, self.hash_seed)
                .map_err(|error| error.to_string())?,
            seeds: self
                .seeds
                .into_iter()
                .map(SeedConfig::into_seed)
                .collect::<Result<_, _>>()?,
            startup_timeout: Duration::from_millis(self.startup_timeout_ms),
            data_directory: resolve_data_directory(path, self.data_dir)?,
            open_mode,
            storage: StorageOptions::default(),
            peers: PeerLimits::default(),
            server: ServerConfig::default(),
        };
        config.validate().map_err(|error| error.to_string())?;
        Ok(Startup::Join(config))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeEndpointConfig {
    node_id: u64,
    peer_address: SocketAddr,
    data_address: SocketAddr,
}

impl NodeEndpointConfig {
    fn into_endpoint(self) -> Result<NodeEndpoint, String> {
        Ok(NodeEndpoint {
            node_id: NodeId::new(self.node_id).map_err(|error| error.to_string())?,
            peer_address: self.peer_address,
            data_address: self.data_address,
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SeedConfig {
    node_id: u64,
    address: SocketAddr,
}

impl SeedConfig {
    fn into_seed(self) -> Result<JoinSeed, String> {
        Ok(JoinSeed {
            node_id: NodeId::new(self.node_id).map_err(|error| error.to_string())?,
            address: self.address,
        })
    }
}

fn resolve_data_directory(config_path: &Path, data_dir: PathBuf) -> Result<PathBuf, String> {
    // Check before joining, otherwise an empty path silently becomes the config directory.
    if data_dir.as_os_str().is_empty() {
        return Err("data_dir cannot be empty".into());
    }
    if data_dir.is_absolute() {
        return Ok(data_dir);
    }
    let parent = config_path.parent().ok_or("config path must name a file")?;
    Ok(parent.join(data_dir))
}

fn default_shards() -> u16 {
    4
}

fn default_join_timeout_ms() -> u64 {
    10_000
}
