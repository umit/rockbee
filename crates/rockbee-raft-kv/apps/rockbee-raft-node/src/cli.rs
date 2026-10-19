//! Process arguments only; the library's NodeConfig owns semantic validation.
use std::{net::SocketAddr, path::PathBuf};

use rockbee_raft_node::{config::NodeConfig, storage::OpenMode};
use rockbee_raft_protocol::{
    ClusterId, NodeId, ShardConfig,
    routing::{ClusterDirectory, NodeEndpoint},
};

pub(crate) struct Arguments {
    pub config: NodeConfig,
    pub initialize: bool,
}

pub(crate) const USAGE: &str = "Usage: rockbee-raft-node --node-id ID --cluster-id 32_HEX \
  --data-dir PATH (--create | --open) \
  --node ID@PEER_ADDRESS@DATA_ADDRESS [--node ...] \
  [--shards COUNT] [--hash-seed SEED] [--initialize]\n\n\
Every node uses the same three --node entries and immutable shard configuration.\n\
Use --create only for new directories; --open requires existing durable state.\n\
Use --initialize on the lowest node ID to initialize groups and publish metadata.";

pub(crate) fn parse(mut values: impl Iterator<Item = String>) -> Result<Option<Arguments>, String> {
    let mut node_id = None;
    let mut cluster_id = None;
    let mut data_directory = None;
    let mut open_mode = None;
    let mut nodes = Vec::new();
    let mut shards = 4;
    let mut hash_seed = 0;
    let mut initialize = false;
    while let Some(flag) = values.next() {
        match flag.as_str() {
            "--help" | "-h" => return Ok(None),
            "--node-id" => node_id = Some(parse_node_id(&value(&mut values, &flag)?)?),
            "--cluster-id" => cluster_id = Some(parse_cluster_id(&value(&mut values, &flag)?)?),
            "--data-dir" => data_directory = Some(PathBuf::from(value(&mut values, &flag)?)),
            "--create" | "--open" => {
                if open_mode.is_some() {
                    return Err("specify exactly one of --create or --open".into());
                }
                open_mode = Some(if flag == "--create" {
                    OpenMode::CreateNew
                } else {
                    OpenMode::OpenExisting
                });
            }
            "--node" => nodes.push(parse_node(&value(&mut values, &flag)?)?),
            "--shards" => {
                shards = value(&mut values, &flag)?
                    .parse()
                    .map_err(|_| "invalid shard count")?
            }
            "--hash-seed" => {
                hash_seed = value(&mut values, &flag)?
                    .parse()
                    .map_err(|_| "invalid hash seed")?
            }
            "--initialize" => initialize = true,
            unknown => return Err(format!("unknown argument: {unknown}")),
        }
    }
    let directory = ClusterDirectory::initial(
        cluster_id.ok_or("--cluster-id is required")?,
        ShardConfig::new(shards, hash_seed).map_err(|error| error.to_string())?,
        nodes,
    )
    .map_err(|error| error.to_string())?;
    let node_id = node_id.ok_or("--node-id is required")?;
    if initialize && directory.nodes.first().map(|node| node.node_id) != Some(node_id) {
        return Err("--initialize is allowed only on the lowest configured node ID".into());
    }
    let config = NodeConfig::new(
        directory,
        node_id,
        data_directory.ok_or("--data-dir is required")?,
        open_mode.ok_or("choose --create for new state or --open for existing state")?,
    )
    .map_err(|error| error.to_string())?;
    Ok(Some(Arguments { config, initialize }))
}

fn value(values: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    values.next().ok_or_else(|| format!("{flag} needs a value"))
}

fn parse_node_id(value: &str) -> Result<NodeId, String> {
    NodeId::new(
        value
            .parse()
            .map_err(|_| "node ID must be an unsigned integer")?,
    )
    .map_err(|error| error.to_string())
}

fn parse_node(value: &str) -> Result<NodeEndpoint, String> {
    let mut parts = value.split('@');
    let id = parts.next().ok_or("node requires ID@PEER@DATA")?;
    let peer = parts.next().ok_or("node requires ID@PEER@DATA")?;
    let data = parts.next().ok_or("node requires ID@PEER@DATA")?;
    if parts.next().is_some() {
        return Err("node requires exactly ID@PEER@DATA".into());
    }
    Ok(NodeEndpoint {
        node_id: parse_node_id(id)?,
        peer_address: peer
            .parse::<SocketAddr>()
            .map_err(|_| "invalid peer address")?,
        data_address: data
            .parse::<SocketAddr>()
            .map_err(|_| "invalid backend address")?,
    })
}

fn parse_cluster_id(value: &str) -> Result<ClusterId, String> {
    if value.len() != 32 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("cluster ID must contain exactly 32 hexadecimal characters".into());
    }
    let mut bytes = [0; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        // ASCII validation above makes these UTF-8 boundaries safe.
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|error| error.to_string())?;
    }
    ClusterId::new(bytes).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arguments() -> Vec<String> {
        [
            "--node-id",
            "1",
            "--cluster-id",
            "01010101010101010101010101010101",
            "--data-dir",
            "/tmp/example-db",
            "--create",
            "--node",
            "3@127.0.0.1:7103@127.0.0.1:8103",
            "--node",
            "1@127.0.0.1:7101@127.0.0.1:8101",
            "--node",
            "2@127.0.0.1:7102@127.0.0.1:8102",
            "--initialize",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    }

    #[test]
    fn creates_canonical_initial_directory_and_requires_explicit_open_intent() {
        let parsed = parse(arguments().into_iter())
            .expect("valid arguments")
            .expect("start request");
        assert!(parsed.initialize);
        assert_eq!(parsed.config.directory.nodes[0].node_id.get(), 1);
        assert_eq!(parsed.config.directory.placements.len(), 5);
        assert!(matches!(parsed.config.open_mode, OpenMode::CreateNew));
        let missing = arguments().into_iter().filter(|value| value != "--create");
        assert!(parse(missing).is_err());
        let mut conflicting = arguments();
        conflicting.push("--open".into());
        assert!(parse(conflicting.into_iter()).is_err());
    }

    #[test]
    fn rejects_unknown_flags_missing_values_and_non_ascii_cluster_ids_without_panic() {
        assert!(parse(["--unknown".into()].into_iter()).is_err());
        assert!(parse(["--node-id".into()].into_iter()).is_err());
        let mut unicode = arguments();
        unicode[3] = "é".repeat(16);
        assert!(parse(unicode.into_iter()).is_err());
        assert!(
            parse(["--help".into()].into_iter())
                .expect("help")
                .is_none()
        );
    }
}
