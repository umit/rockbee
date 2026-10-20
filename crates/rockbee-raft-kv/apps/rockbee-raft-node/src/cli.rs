//! Process arguments only; the library's NodeConfig owns semantic validation.
use std::{net::SocketAddr, path::PathBuf, time::Duration};

use rockbee_raft_node::{
    config::{JoinConfig, JoinSeed, NodeConfig},
    rpc::PeerLimits,
    storage::{OpenMode, StorageOptions},
};
use rockbee_raft_protocol::transport::server::ServerConfig;
use rockbee_raft_protocol::{
    ClusterId, NodeId, ShardConfig,
    routing::{ClusterDirectory, NodeEndpoint},
};

pub(crate) struct Arguments {
    pub config: Startup,
    pub initialize: bool,
}

pub(crate) enum Startup {
    Bootstrap(NodeConfig),
    Join(JoinConfig),
}

pub(crate) const USAGE: &str =
    "Usage: rockbee-raft-node --cluster-id 32_HEX --data-dir PATH (--create | --open)
  Bootstrap: --node-id ID --node ID@PEER@DATA [--node ...] [--initialize]
  Join: --join --local-node ID@PEER@DATA --seed ID@BACKEND [--seed ...]
  Shared: [--shards COUNT] [--hash-seed SEED]
  Join: [--startup-timeout-ms MILLIS] (1..60000, default10000)
Every bootstrap node uses the same three --node entries and immutable shards.
Join seeds must include an available metadata leader (normally all ABC backends).
Use --create only for new directories; --open requires existing durable state.";

pub(crate) fn parse(mut values: impl Iterator<Item = String>) -> Result<Option<Arguments>, String> {
    let mut node_id = None;
    let mut cluster_id = None;
    let mut data_directory = None;
    let mut open_mode = None;
    let mut nodes = Vec::new();
    let mut shards = 4;
    let mut hash_seed = 0;
    let mut initialize = false;
    let mut join = false;
    let mut local_endpoint = None;
    let mut seeds = Vec::new();
    let mut startup_timeout = None;
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
            "--join" => join = true,
            "--local-node" => local_endpoint = Some(parse_node(&value(&mut values, &flag)?)?),
            "--seed" => seeds.push(parse_seed(&value(&mut values, &flag)?)?),
            "--startup-timeout-ms" => {
                startup_timeout = Some(Duration::from_millis(
                    value(&mut values, &flag)?
                        .parse()
                        .map_err(|_| "invalid startup timeout")?,
                ))
            }
            unknown => return Err(format!("unknown argument: {unknown}")),
        }
    }
    let cluster_id = cluster_id.ok_or("--cluster-id is required")?;
    let shard_config = ShardConfig::new(shards, hash_seed).map_err(|error| error.to_string())?;
    let data_directory = data_directory.ok_or("--data-dir is required")?;
    let open_mode =
        open_mode.ok_or("choose --create for new state or --open for existing state")?;
    let config = if join {
        if initialize || !nodes.is_empty() || node_id.is_some() {
            return Err(
                "--join cannot use --initialize, --node or --node-id; supply --local-node".into(),
            );
        }
        let config = JoinConfig {
            local_endpoint: local_endpoint.ok_or("--join requires --local-node ID@PEER@DATA")?,
            cluster_id,
            shard_config,
            seeds,
            startup_timeout: startup_timeout.unwrap_or(Duration::from_secs(10)),
            data_directory,
            open_mode,
            storage: StorageOptions::default(),
            peers: PeerLimits::default(),
            server: ServerConfig::default(),
        };
        config.validate().map_err(|error| error.to_string())?;
        Startup::Join(config)
    } else {
        if local_endpoint.is_some() || !seeds.is_empty() || startup_timeout.is_some() {
            return Err("--local-node, --seed and --startup-timeout-ms require --join".into());
        }
        let directory = ClusterDirectory::initial(cluster_id, shard_config, nodes)
            .map_err(|error| error.to_string())?;
        let node_id = node_id.ok_or("--node-id is required")?;
        if initialize && directory.nodes.first().map(|node| node.node_id) != Some(node_id) {
            return Err("--initialize is allowed only on the lowest configured node ID".into());
        }
        Startup::Bootstrap(
            NodeConfig::new(directory, node_id, data_directory, open_mode)
                .map_err(|error| error.to_string())?,
        )
    };
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

fn parse_seed(value: &str) -> Result<JoinSeed, String> {
    let (id, address) = value.split_once('@').ok_or("seed requires ID@BACKEND")?;
    Ok(JoinSeed {
        node_id: parse_node_id(id)?,
        address: address
            .parse()
            .map_err(|_| "invalid seed backend address")?,
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
    #![allow(clippy::panic, clippy::unwrap_used)]
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
        let Startup::Bootstrap(config) = parsed.config else {
            panic!("expected bootstrap configuration")
        };
        assert_eq!(config.directory.nodes[0].node_id.get(), 1);
        assert_eq!(config.directory.placements.len(), 5);
        assert!(matches!(config.open_mode, OpenMode::CreateNew));
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
    fn join_arguments() -> Vec<String> {
        [
            "--join",
            "--local-node",
            "1@127.0.0.1:7101@127.0.0.1:8101",
            "--cluster-id",
            "01010101010101010101010101010101",
            "--data-dir",
            "/tmp/join-example-db",
            "--create",
            "--seed",
            "10@127.0.0.1:8110",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    }

    #[test]
    fn explicit_join_parses_one_local_identity_and_bounded_backend_seeds() {
        let parsed = parse(join_arguments().into_iter()).unwrap().unwrap();
        let Startup::Join(config) = parsed.config else {
            panic!("expected join configuration")
        };
        assert_eq!(config.local_endpoint.node_id.get(), 1);
        assert_eq!(config.seeds[0].node_id.get(), 10);
        assert!(!parsed.initialize);
        for conflicting in [
            vec!["--initialize"],
            vec!["--node", "2@127.0.0.1:7102@127.0.0.1:8102"],
            vec!["--node-id", "1"],
            vec!["--open"],
            vec!["--startup-timeout-ms", "0"],
        ] {
            let mut values = join_arguments();
            values.extend(conflicting.into_iter().map(str::to_owned));
            assert!(parse(values.into_iter()).is_err());
        }
        for flag in ["--local-node", "--seed"] {
            let values = join_arguments();
            let index = values.iter().position(|value| value == flag).unwrap();
            let values = values
                .into_iter()
                .enumerate()
                .filter_map(|(position, value)| {
                    if position == index || position == index + 1 {
                        None
                    } else {
                        Some(value)
                    }
                });
            assert!(parse(values).is_err());
        }
    }
}
