use crate::{Result, RpcConfig};
use rockbee_protocol::INTERNAL_MAX_IN_FLIGHT;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;

pub const USAGE: &str = "Usage: rockbee-gateway [--addr HOST:PORT] [--writer HOST:PORT] [--reader HOST:PORT ...] [--upstream-max-in-flight COUNT] [--upstream-max-pending COUNT] [--rpc-max-connections COUNT] [--rpc-global-max-in-flight COUNT] [--rpc-read-timeout-ms MILLISECONDS] [--rpc-write-timeout-ms MILLISECONDS] [--config PATH] [--check-config]";

pub struct Config {
    pub address: String,
    pub writer: String,
    pub readers: Vec<String>,
    pub upstream: UpstreamConfig,
    pub rpc: RpcConfig,
    pub check_config: bool,
}

impl Config {
    pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Option<Self>> {
        let mut cli = CliConfig::default();
        let mut args = args.into_iter();
        while let Some(flag) = args.next() {
            if flag == "--help" || flag == "-h" {
                return Ok(None);
            }
            if flag == "--check-config" {
                if cli.check_config {
                    return Err(USAGE.into());
                }
                cli.check_config = true;
                continue;
            }
            let value = args
                .next()
                .filter(|v| !v.is_empty() && !v.starts_with("--"))
                .ok_or(USAGE)?;
            match flag.as_str() {
                "--upstream-max-in-flight" if cli.max_in_flight.is_none() => {
                    cli.max_in_flight = Some(value.parse().map_err(|_| USAGE)?);
                }
                "--upstream-max-pending" if cli.max_pending.is_none() => {
                    cli.max_pending = Some(value.parse().map_err(|_| USAGE)?);
                }
                "--addr" if cli.address.is_none() => cli.address = Some(value),
                "--writer" if cli.writer.is_none() => cli.writer = Some(value),
                "--reader" => cli.readers.push(value),
                "--rpc-max-connections" if cli.rpc_max_connections.is_none() => {
                    cli.rpc_max_connections = Some(value.parse().map_err(|_| USAGE)?);
                }
                "--rpc-global-max-in-flight" if cli.rpc_global_max_in_flight.is_none() => {
                    cli.rpc_global_max_in_flight = Some(value.parse().map_err(|_| USAGE)?);
                }
                "--rpc-read-timeout-ms" if cli.rpc_read_timeout_ms.is_none() => {
                    cli.rpc_read_timeout_ms = Some(value.parse().map_err(|_| USAGE)?);
                }
                "--rpc-write-timeout-ms" if cli.rpc_write_timeout_ms.is_none() => {
                    cli.rpc_write_timeout_ms = Some(value.parse().map_err(|_| USAGE)?);
                }
                "--config" if cli.config.is_none() => cli.config = Some(value.into()),
                _ => return Err(USAGE.into()),
            }
        }
        let file = match &cli.config {
            Some(path) => read_config_file(path)?,
            None => FileConfig::default(),
        };
        let writer = cli.writer.or(file.writer).ok_or(USAGE)?;
        // Supplying any --reader values replaces the file list instead of
        // mixing two topology declarations.
        let readers = if cli.readers.is_empty() {
            file.readers.unwrap_or_default()
        } else {
            cli.readers
        };
        validate_topology(&writer, &readers)?;
        let address = cli
            .address
            .or(file.addr)
            .unwrap_or_else(|| "127.0.0.1:7100".to_owned());
        validate_address(&address, true)?;
        Ok(Some(Self {
            address,
            writer,
            readers,
            upstream: UpstreamConfig::new(
                cli.max_in_flight
                    .or(file
                        .upstream
                        .as_ref()
                        .and_then(|upstream| upstream.max_in_flight))
                    .unwrap_or(INTERNAL_MAX_IN_FLIGHT),
                cli.max_pending
                    .or(file
                        .upstream
                        .as_ref()
                        .and_then(|upstream| upstream.max_pending))
                    .unwrap_or(128),
            )?,
            rpc: rpc_config(
                cli.rpc_max_connections
                    .or(file.rpc.as_ref().and_then(|rpc| rpc.max_connections)),
                cli.rpc_global_max_in_flight
                    .or(file.rpc.as_ref().and_then(|rpc| rpc.global_max_in_flight)),
                cli.rpc_read_timeout_ms
                    .or(file.rpc.as_ref().and_then(|rpc| rpc.read_timeout_ms)),
                cli.rpc_write_timeout_ms
                    .or(file.rpc.as_ref().and_then(|rpc| rpc.write_timeout_ms)),
            )?,
            check_config: cli.check_config,
        }))
    }

    /// Validated local settings; no DNS lookup or backend health check is performed.
    pub fn effective_json(&self) -> Value {
        json!({
            "role": "gateway",
            "addr": self.address,
            "writer": self.writer,
            "readers": self.readers,
            "upstream": {
                "max_in_flight": self.upstream.max_in_flight(),
                "max_pending": self.upstream.max_pending(),
            },
            "rpc": {
                "max_connections": self.rpc.max_connections(),
                "global_max_in_flight": self.rpc.global_max_in_flight(),
                "read_timeout_ms": self.rpc.read_timeout().as_millis(),
                "write_timeout_ms": self.rpc.write_timeout().as_millis(),
            },
            "check_config": self.check_config,
        })
    }
}

#[derive(Default)]
struct CliConfig {
    address: Option<String>,
    writer: Option<String>,
    readers: Vec<String>,
    max_in_flight: Option<usize>,
    max_pending: Option<usize>,
    rpc_max_connections: Option<usize>,
    rpc_global_max_in_flight: Option<usize>,
    rpc_read_timeout_ms: Option<u64>,
    rpc_write_timeout_ms: Option<u64>,
    config: Option<PathBuf>,
    check_config: bool,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    addr: Option<String>,
    writer: Option<String>,
    readers: Option<Vec<String>>,
    upstream: Option<FileUpstreamConfig>,
    rpc: Option<FileRpcConfig>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileUpstreamConfig {
    max_in_flight: Option<usize>,
    max_pending: Option<usize>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileRpcConfig {
    max_connections: Option<usize>,
    global_max_in_flight: Option<usize>,
    read_timeout_ms: Option<u64>,
    write_timeout_ms: Option<u64>,
}

fn read_config_file(path: &PathBuf) -> Result<FileConfig> {
    let contents = std::fs::read_to_string(path)
        .map_err(|error| format!("failed to read config file {}: {error}", path.display()))?;
    toml::from_str(&contents)
        .map_err(|error| format!("invalid config file {}: {error}", path.display()).into())
}

fn rpc_config(
    max_connections: Option<usize>,
    global_max_in_flight: Option<usize>,
    read_timeout_ms: Option<u64>,
    write_timeout_ms: Option<u64>,
) -> Result<RpcConfig> {
    let defaults = RpcConfig::default();
    RpcConfig::new(
        max_connections.unwrap_or(defaults.max_connections()),
        read_timeout_ms
            .map(Duration::from_millis)
            .unwrap_or(defaults.read_timeout()),
        write_timeout_ms
            .map(Duration::from_millis)
            .unwrap_or(defaults.write_timeout()),
    )?
    .with_global_max_in_flight(global_max_in_flight.unwrap_or(defaults.global_max_in_flight()))
}

pub(crate) fn validate_topology(writer: &str, readers: &[String]) -> Result<()> {
    // Yalnızca yazılan adres metinleri karşılaştırılır; DNS/rol keşfi yapılmaz.
    if readers.is_empty() {
        return Err("at least one reader is required".into());
    }
    let mut addresses = HashSet::new();
    for address in std::iter::once(writer).chain(readers.iter().map(String::as_str)) {
        validate_address(address, false)?;
        if !addresses.insert(address) {
            return Err("writer and reader addresses must be distinct".into());
        }
    }
    Ok(())
}

fn validate_address(address: &str, allow_zero_port: bool) -> Result<()> {
    let (host, port) = address
        .rsplit_once(':')
        .ok_or("expected HOST:PORT address")?;
    let valid_host = if host.starts_with('[') {
        host.strip_prefix('[')
            .and_then(|host| host.strip_suffix(']'))
            .and_then(|host| host.parse::<std::net::Ipv6Addr>().ok())
            .is_some()
    } else {
        !host.is_empty()
            && host
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b".-_".contains(&byte))
    };
    if !valid_host
        || port.is_empty()
        || !port.bytes().all(|byte| byte.is_ascii_digit())
        || !matches!(port.parse::<u16>(), Ok(port) if allow_zero_port || port > 0)
    {
        return Err(format!("invalid HOST:PORT address: {address}").into());
    }
    Ok(())
}

/// Limits apply independently to each configured backend, including connection waiters.
#[derive(Clone, Copy, Debug)]
pub struct UpstreamConfig {
    max_in_flight: usize,
    max_pending: usize,
}
impl UpstreamConfig {
    pub fn new(max_in_flight: usize, max_pending: usize) -> Result<Self> {
        if max_in_flight == 0
            || max_pending == 0
            || max_in_flight > INTERNAL_MAX_IN_FLIGHT
            || max_pending > tokio::sync::Semaphore::MAX_PERMITS
        {
            return Err("upstream active limit must be 1..=128; pending limit must be positive and fit semaphore capacity".into());
        }
        Ok(Self {
            max_in_flight,
            max_pending,
        })
    }
    pub fn max_in_flight(self) -> usize {
        self.max_in_flight
    }
    pub fn max_pending(self) -> usize {
        self.max_pending
    }
}
impl Default for UpstreamConfig {
    fn default() -> Self {
        Self {
            max_in_flight: INTERNAL_MAX_IN_FLIGHT,
            max_pending: 128,
        }
    }
}
