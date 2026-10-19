//! Thin process boundary for the Rockbee Raft gateway library.

use std::{net::SocketAddr, process::ExitCode, time::Duration};

use rockbee_raft_gateway::{GatewayConfig, GatewayRuntime, SeedNode};
use rockbee_raft_protocol::{
    ClusterId, NodeId, ShardConfig,
    transport::{LinkConfig, server::ServerConfig},
};

#[tokio::main]
async fn main() -> ExitCode {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_micros()
        .init();
    let config = match arguments(std::env::args().skip(1)) {
        Ok(Some(config)) => config,
        Ok(None) => {
            println!("{}", usage());
            return ExitCode::SUCCESS;
        }
        Err(error) => {
            eprintln!("{error}\n\n{}", usage());
            return ExitCode::FAILURE;
        }
    };
    // Install signal handlers before opening listeners or contacting seeds, so
    // every owned runtime resource has one orderly shutdown path.
    let mut shutdown = match Shutdown::install() {
        Ok(shutdown) => shutdown,
        Err(error) => {
            log::error!("cannot install shutdown signals: {error}");
            return ExitCode::FAILURE;
        }
    };
    let mut gateway = match GatewayRuntime::start(config).await {
        Ok(gateway) => gateway,
        Err(error) => {
            log::error!("gateway startup failed: {error}");
            return ExitCode::FAILURE;
        }
    };
    let shutdown_error = shutdown.wait().await.err();
    log::info!("gateway shutdown requested");
    gateway.close().await;
    if let Some(error) = shutdown_error {
        log::error!("shutdown signal failed: {error}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

fn arguments(mut values: impl Iterator<Item = String>) -> Result<Option<GatewayConfig>, String> {
    let mut listen = None;
    let mut cluster_id = None;
    let mut seeds = Vec::new();
    let mut shards = 4;
    let mut hash_seed = 0;
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
            unexpected => return Err(format!("unknown argument: {unexpected}")),
        }
    }
    Ok(Some(GatewayConfig {
        cluster_id: cluster_id.ok_or("--cluster-id is required")?,
        expected_shard_config: ShardConfig::new(shards, hash_seed)
            .map_err(|error| error.to_string())?,
        seeds,
        listen_address: listen.ok_or("--listen is required")?,
        backend_link: LinkConfig::default(),
        server: ServerConfig::default(),
        startup_timeout: Duration::from_secs(5),
    }))
}

fn parse_value(values: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    values.next().ok_or_else(|| format!("{flag} needs a value"))
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

#[cfg(unix)]
struct Shutdown {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl Shutdown {
    fn install() -> Result<Self, std::io::Error> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
        })
    }

    async fn wait(&mut self) -> Result<(), std::io::Error> {
        tokio::select! {
            _ = self.interrupt.recv() => Ok(()),
            _ = self.terminate.recv() => Ok(()),
        }
    }
}

#[cfg(not(unix))]
struct Shutdown;

#[cfg(not(unix))]
impl Shutdown {
    fn install() -> Result<Self, std::io::Error> {
        Ok(Self)
    }

    async fn wait(&mut self) -> Result<(), std::io::Error> {
        tokio::signal::ctrl_c().await
    }
}

fn usage() -> &'static str {
    "Usage: rockbee-raft-gateway --listen HOST:PORT --cluster-id 32_HEX\n\
  --seed NODE_ID@HOST:PORT [--seed NODE_ID@HOST:PORT ...]\n\
  [--shards COUNT] [--hash-seed SEED]\n\n\
Starts only after a backend node returns and validates the replicated metadata directory.\n\
Seeds are bootstrap addresses, not trusted routing state."
}
