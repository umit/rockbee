//! Gateway startup arguments and TOML configuration-file parsing.

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use rockbee_raft_gateway::{GatewayConfig, SeedNode};
use rockbee_raft_protocol::{
    ClusterId, NodeId, ShardConfig,
    transport::{LinkConfig, server::ServerConfig},
};
use serde::Deserialize;

#[derive(Default)]
struct ServerOverrides {
    request_bytes: Option<usize>,
    response_bytes: Option<usize>,
    max_connections: Option<usize>,
}

pub(super) fn arguments(
    values: impl Iterator<Item = String>,
) -> Result<Option<GatewayConfig>, String> {
    let values: Vec<_> = values.collect();
    if values.iter().any(|value| value == "--config") {
        return parse_config_arguments(values.into_iter());
    }
    parse_inline_arguments(values.into_iter())
}

fn parse_config_arguments(
    mut values: impl Iterator<Item = String>,
) -> Result<Option<GatewayConfig>, String> {
    let mut path = None;
    while let Some(flag) = values.next() {
        match flag.as_str() {
            "--help" | "-h" => return Ok(None),
            "--config" => {
                if path.is_some() {
                    return Err("specify --config only once".into());
                }
                path = Some(PathBuf::from(parse_value(&mut values, "--config")?));
            }
            _ => return Err(format!("--config cannot be combined with {flag}")),
        }
    }
    let path = path.ok_or("--config requires a file path")?;
    load_config(&path).map(Some)
}

fn load_config(path: &Path) -> Result<GatewayConfig, String> {
    let contents = std::fs::read_to_string(path)
        .map_err(|error| format!("cannot read gateway config {}: {error}", path.display()))?;
    let file: GatewayFileConfig = toml::from_str(&contents).map_err(|error| {
        // Display includes source lines. Avoid writing configuration contents to stderr.
        format!(
            "invalid gateway config {}: {}",
            path.display(),
            error.message()
        )
    })?;
    file.into_gateway_config()
        .map_err(|error| format!("invalid gateway config {}: {error}", path.display()))
}

fn parse_inline_arguments(
    mut values: impl Iterator<Item = String>,
) -> Result<Option<GatewayConfig>, String> {
    let mut listen = None;
    let mut cluster_id = None;
    let mut seeds = Vec::new();
    let mut shards = 4;
    let mut hash_seed = 0;
    let mut server_overrides = ServerOverrides::default();
    while let Some(flag) = values.next() {
        match flag.as_str() {
            "--help" | "-h" => return Ok(None),
            "--listen" => {
                listen = Some(
                    parse_value(&mut values, "--listen")?
                        .parse()
                        .map_err(|_| "--listen must be host:port")?,
                )
            }
            "--cluster-id" => {
                cluster_id = Some(parse_cluster_id(&parse_value(
                    &mut values,
                    "--cluster-id",
                )?)?)
            }
            "--seed" => seeds.push(parse_seed(&parse_value(&mut values, "--seed")?)?),
            "--shards" => {
                shards = parse_value(&mut values, "--shards")?
                    .parse()
                    .map_err(|_| "--shards must be an integer")?
            }
            "--hash-seed" => {
                hash_seed = parse_value(&mut values, "--hash-seed")?
                    .parse()
                    .map_err(|_| "--hash-seed must be an unsigned integer")?
            }
            "--server-request-mib" => {
                server_overrides.request_bytes = Some(parse_server_budget_mib(&mut values, &flag)?);
            }
            "--server-response-mib" => {
                server_overrides.response_bytes =
                    Some(parse_server_budget_mib(&mut values, &flag)?);
            }
            "--max-connections" => {
                server_overrides.max_connections = Some(
                    parse_value(&mut values, &flag)?
                        .parse()
                        .map_err(|_| "--max-connections must be an unsigned integer")?,
                );
            }
            unexpected => return Err(format!("unknown argument: {unexpected}")),
        }
    }
    let cluster_id = cluster_id.ok_or("--cluster-id is required")?;
    let listen_address = listen.ok_or("--listen is required")?;
    build_config(
        cluster_id,
        listen_address,
        shards,
        hash_seed,
        seeds,
        server_overrides,
    )
    .map(Some)
}

fn build_config(
    cluster_id: ClusterId,
    listen_address: SocketAddr,
    shards: u16,
    hash_seed: u64,
    seeds: Vec<SeedNode>,
    server_overrides: ServerOverrides,
) -> Result<GatewayConfig, String> {
    let mut server = ServerConfig::default();
    if let Some(request_bytes) = server_overrides.request_bytes {
        server.request_bytes = request_bytes;
    }
    if let Some(response_bytes) = server_overrides.response_bytes {
        server.response_bytes = response_bytes;
    }
    if let Some(max_connections) = server_overrides.max_connections {
        server.max_connections = max_connections;
    }
    server.validate().map_err(|error| error.to_string())?;

    let config = GatewayConfig {
        cluster_id,
        expected_shard_config: ShardConfig::new(shards, hash_seed)
            .map_err(|error| error.to_string())?,
        seeds,
        listen_address,
        backend_link: LinkConfig::default(),
        server,
        startup_timeout: Duration::from_secs(5),
    };
    Ok(config)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GatewayFileConfig {
    cluster_id: String,
    listen: SocketAddr,
    #[serde(default = "default_shards")]
    shards: u16,
    #[serde(default)]
    hash_seed: u64,
    seeds: Vec<SeedFileConfig>,
}

impl GatewayFileConfig {
    fn into_gateway_config(self) -> Result<GatewayConfig, String> {
        let cluster_id = parse_cluster_id(&self.cluster_id)
            .map_err(|error| format!("invalid cluster_id: {error}"))?;
        let seeds = self
            .seeds
            .into_iter()
            .map(SeedFileConfig::into_seed)
            .collect::<Result<_, _>>()?;
        let config = build_config(
            cluster_id,
            self.listen,
            self.shards,
            self.hash_seed,
            seeds,
            ServerOverrides::default(),
        )?;
        config.validate().map_err(|error| error.to_string())?;
        Ok(config)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SeedFileConfig {
    node_id: u64,
    address: SocketAddr,
}

impl SeedFileConfig {
    fn into_seed(self) -> Result<SeedNode, String> {
        Ok(SeedNode {
            node_id: NodeId::new(self.node_id).map_err(|error| error.to_string())?,
            address: self.address,
        })
    }
}

fn default_shards() -> u16 {
    4
}

fn parse_value(values: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    values.next().ok_or_else(|| format!("{flag} needs a value"))
}

fn parse_server_budget_mib(
    values: &mut impl Iterator<Item = String>,
    flag: &str,
) -> Result<usize, String> {
    let mib = parse_value(values, flag)?
        .parse::<usize>()
        .map_err(|_| format!("{flag} must be an unsigned MiB value"))?;
    mib.checked_mul(1024 * 1024)
        .ok_or_else(|| format!("{flag} MiB value overflows byte budget"))
}

fn parse_seed(value: &str) -> Result<SeedNode, String> {
    let (id, address) = value
        .split_once('@')
        .ok_or("--seed must be node-id@host:port")?;
    Ok(SeedNode {
        node_id: NodeId::new(
            id.parse()
                .map_err(|_| "seed node ID must be an unsigned integer")?,
        )
        .map_err(|error| error.to_string())?,
        address: address
            .parse::<SocketAddr>()
            .map_err(|_| "seed address must be host:port")?,
    })
}

fn parse_cluster_id(value: &str) -> Result<ClusterId, String> {
    let input = value.as_bytes();
    if input.len() != 32 || !input.iter().all(u8::is_ascii_hexdigit) {
        return Err("--cluster-id must be exactly 32 hexadecimal characters".to_owned());
    }
    let mut bytes = [0; 16];
    for (index, output) in bytes.iter_mut().enumerate() {
        let high = hex_digit(input[index * 2]).ok_or("--cluster-id must be hexadecimal")?;
        let low = hex_digit(input[index * 2 + 1]).ok_or("--cluster-id must be hexadecimal")?;
        *output = (high << 4) | low;
    }
    ClusterId::new(bytes).map_err(|error| error.to_string())
}

const fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

pub(super) const USAGE: &str = "Usage: rockbee-raft-gateway --config PATH\nOr: rockbee-raft-gateway --listen HOST:PORT --cluster-id 32_HEX\n\
  --seed NODE_ID@HOST:PORT [--seed NODE_ID@HOST:PORT ...]\n\
  [--shards COUNT] [--hash-seed SEED] [--server-request-mib MIB]\n\
  [--server-response-mib MIB] [--max-connections COUNT]\n\n\
Starts only after a backend node returns and validates the replicated metadata directory.\n\
Seeds are bootstrap addresses, not trusted routing state.\n\
Server MiB options set aggregate client-to-gateway request/response admission budgets\n\
(each defaults to 64 MiB); --max-connections defaults to 64.";
