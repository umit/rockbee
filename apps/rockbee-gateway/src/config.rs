use crate::{Result, RpcConfig};
use rockbee_protocol::INTERNAL_MAX_IN_FLIGHT;
use serde_json::{Value, json};
use std::collections::HashSet;
use std::time::Duration;

pub const USAGE: &str = "Usage: rockbee-gateway [--addr HOST:PORT] --writer HOST:PORT --reader HOST:PORT [--reader HOST:PORT ...] [--upstream-max-in-flight COUNT] [--upstream-max-pending COUNT] [--rpc-max-connections COUNT] [--rpc-global-max-in-flight COUNT] [--rpc-read-timeout-ms MILLISECONDS] [--rpc-write-timeout-ms MILLISECONDS] [--check-config]";

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
        let mut address = None;
        let mut writer = None;
        let mut readers = Vec::new();
        let mut max_in_flight = None;
        let mut max_pending = None;
        let mut rpc_max_connections = None;
        let mut rpc_global_max_in_flight = None;
        let mut rpc_read_timeout_ms = None;
        let mut rpc_write_timeout_ms = None;
        let mut check_config = false;
        let mut args = args.into_iter();
        while let Some(flag) = args.next() {
            if flag == "--help" || flag == "-h" {
                return Ok(None);
            }
            if flag == "--check-config" {
                if check_config {
                    return Err(USAGE.into());
                }
                check_config = true;
                continue;
            }
            let value = args
                .next()
                .filter(|v| !v.is_empty() && !v.starts_with("--"))
                .ok_or(USAGE)?;
            match flag.as_str() {
                "--upstream-max-in-flight" if max_in_flight.is_none() => {
                    max_in_flight = Some(value.parse().map_err(|_| USAGE)?);
                }
                "--upstream-max-pending" if max_pending.is_none() => {
                    max_pending = Some(value.parse().map_err(|_| USAGE)?);
                }
                "--addr" if address.is_none() => address = Some(value),
                "--writer" if writer.is_none() => writer = Some(value),
                "--reader" => readers.push(value),
                "--rpc-max-connections" if rpc_max_connections.is_none() => {
                    rpc_max_connections = Some(value.parse().map_err(|_| USAGE)?);
                }
                "--rpc-global-max-in-flight" if rpc_global_max_in_flight.is_none() => {
                    rpc_global_max_in_flight = Some(value.parse().map_err(|_| USAGE)?);
                }
                "--rpc-read-timeout-ms" if rpc_read_timeout_ms.is_none() => {
                    rpc_read_timeout_ms = Some(value.parse().map_err(|_| USAGE)?);
                }
                "--rpc-write-timeout-ms" if rpc_write_timeout_ms.is_none() => {
                    rpc_write_timeout_ms = Some(value.parse().map_err(|_| USAGE)?);
                }
                _ => return Err(USAGE.into()),
            }
        }
        let writer = writer.ok_or(USAGE)?;
        validate_topology(&writer, &readers)?;
        let address = address.unwrap_or_else(|| "127.0.0.1:7100".to_owned());
        validate_address(&address, true)?;
        Ok(Some(Self {
            address,
            writer,
            readers,
            upstream: UpstreamConfig::new(
                max_in_flight.unwrap_or(INTERNAL_MAX_IN_FLIGHT),
                max_pending.unwrap_or(128),
            )?,
            rpc: rpc_config(
                rpc_max_connections,
                rpc_global_max_in_flight,
                rpc_read_timeout_ms,
                rpc_write_timeout_ms,
            )?,
            check_config,
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
