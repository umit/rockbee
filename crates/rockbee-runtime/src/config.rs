use crate::Role;
use crate::backend::Backend;
use crate::memory::MemoryConfig;
use crate::{DEFAULT_ADDR, Result, RpcConfig};
use serde_json::{Value, json};
use slatedb::config::DbReaderOptions;
use std::path::PathBuf;
use std::time::Duration;

mod file;
use file::{FileConfig, FileQueryConfig, FileReaderConfig};

pub const WRITER_USAGE: &str = "Usage: rockbee-writer [--addr HOST:PORT] [--backend localstack|file|s3] [--prefix PREFIX] [--bucket BUCKET] [--endpoint URL] [--directory PATH] [--rpc-max-connections COUNT] [--rpc-global-max-in-flight COUNT] [--rpc-read-timeout-ms MILLISECONDS] [--rpc-write-timeout-ms MILLISECONDS] [--config PATH] [--check-config]";
pub const READER_USAGE: &str = "Usage: rockbee-reader [--addr HOST:PORT] [--backend localstack|file|s3] [--prefix PREFIX] [--bucket BUCKET] [--endpoint URL] [--directory PATH] [--rpc-max-connections COUNT] [--rpc-global-max-in-flight COUNT] [--rpc-read-timeout-ms MILLISECONDS] [--rpc-write-timeout-ms MILLISECONDS] [--config PATH] [--check-config]";

/// Default usage for embedded writer configuration parsing.
pub const USAGE: &str = WRITER_USAGE;

pub fn usage_for_role(role: Role) -> &'static str {
    match role {
        Role::Writer => WRITER_USAGE,
        Role::Reader => READER_USAGE,
    }
}

pub struct Config {
    pub address: String,
    pub role: Role,
    pub prefix: String,
    pub backend: Backend,
    pub rpc: RpcConfig,
    pub memory: MemoryConfig,
    pub reader: ReaderConfig,
    pub check_config: bool,
}

/// Reader refresh settings and independently bounded SQL execution.
///
/// A refresh checks both manifest changes and new WAL data. The default is
/// deliberately taken from the pinned SlateDB release rather than duplicated
/// as a Rockbee constant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReaderConfig {
    manifest_poll_interval: Duration,
    query: QueryConfig,
}

impl Default for ReaderConfig {
    fn default() -> Self {
        Self {
            manifest_poll_interval: DbReaderOptions::default().manifest_poll_interval,
            query: QueryConfig::default(),
        }
    }
}

/// Reader SQL limits. These bound DuckDB execution rather than the process RSS.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryConfig {
    timeout: Duration,
    memory_limit_mb: u32,
    max_concurrent: usize,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_millis(4000),
            memory_limit_mb: 256,
            max_concurrent: 2,
        }
    }
}

impl QueryConfig {
    pub fn timeout(&self) -> Duration {
        self.timeout
    }
    pub fn memory_limit_mb(&self) -> u32 {
        self.memory_limit_mb
    }
    pub fn max_concurrent(&self) -> usize {
        self.max_concurrent
    }

    pub fn validate(&self) -> Result<()> {
        if self.timeout.is_zero()
            || std::time::Instant::now()
                .checked_add(self.timeout)
                .is_none()
        {
            return Err(
                "query.timeout_ms must be positive and form a representable deadline".into(),
            );
        }
        if !(1..=16384).contains(&self.memory_limit_mb) {
            return Err("query.memory_limit_mb must be in 1..=16384".into());
        }
        if !(1..=128).contains(&self.max_concurrent) {
            return Err("query.max_concurrent must be in 1..=128".into());
        }
        Ok(())
    }
}

impl ReaderConfig {
    pub fn query(&self) -> &QueryConfig {
        &self.query
    }

    pub fn manifest_poll_interval(&self) -> Duration {
        self.manifest_poll_interval
    }

    pub fn options(&self, memory: &MemoryConfig) -> DbReaderOptions {
        DbReaderOptions {
            manifest_poll_interval: self.manifest_poll_interval,
            ..memory.reader_options()
        }
    }

    pub fn validate(&self) -> Result<()> {
        self.query.validate()?;
        if self.manifest_poll_interval.is_zero() {
            return Err("reader.poll_interval_ms must be positive".into());
        }

        let checkpoint_lifetime = DbReaderOptions::default().checkpoint_lifetime;
        let minimum_checkpoint_lifetime = self
            .manifest_poll_interval
            .checked_mul(2)
            .ok_or("reader.poll_interval_ms is too large")?;
        if checkpoint_lifetime < minimum_checkpoint_lifetime {
            return Err(format!(
                "reader.poll_interval_ms must be at most {} to remain compatible with SlateDB's default managed checkpoint lifetime",
                checkpoint_lifetime.as_millis() / 2
            )
            .into());
        }
        Ok(())
    }
}

impl Config {
    /// Role-selectable configuration parsing for embedded callers; defaults to writer.
    pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Option<Self>> {
        Self::parse_with_role(args, None)
    }

    /// Parse configuration for an executable that is permanently a writer or reader.
    /// Every supplied role declaration must agree before networking or storage can start.
    pub fn parse_for_role(
        required_role: Role,
        args: impl IntoIterator<Item = String>,
    ) -> Result<Option<Self>> {
        Self::parse_with_role(args, Some(required_role))
    }

    fn parse_with_role(
        args: impl IntoIterator<Item = String>,
        required_role: Option<Role>,
    ) -> Result<Option<Self>> {
        let usage = required_role.map(usage_for_role).unwrap_or(USAGE);
        let mut cli = CliConfig::default();
        let mut args = args.into_iter();
        while let Some(flag) = args.next() {
            if flag == "--help" || flag == "-h" {
                return Ok(None);
            }
            if flag == "--check-config" {
                if cli.check_config {
                    return Err(usage.into());
                }
                cli.check_config = true;
                continue;
            }
            let value = args
                .next()
                .filter(|value| !value.is_empty() && !value.starts_with("--"))
                .ok_or(usage)?;
            match flag.as_str() {
                "--addr" if cli.address.is_none() => cli.address = Some(value),
                "--role" if cli.role.is_none() => cli.role = Some(value),
                "--backend" if cli.backend.is_none() => cli.backend = Some(value),
                "--prefix" if cli.prefix.is_none() => cli.prefix = Some(value),
                "--bucket" if cli.bucket.is_none() => cli.bucket = Some(value),
                "--endpoint" if cli.endpoint.is_none() => cli.endpoint = Some(value),
                "--directory" if cli.directory.is_none() => cli.directory = Some(value.into()),
                "--rpc-max-connections" if cli.rpc_max_connections.is_none() => {
                    cli.rpc_max_connections = Some(value.parse().map_err(|_| usage)?);
                }
                "--rpc-global-max-in-flight" if cli.rpc_global_max_in_flight.is_none() => {
                    cli.rpc_global_max_in_flight = Some(value.parse().map_err(|_| usage)?);
                }
                "--rpc-read-timeout-ms" if cli.rpc_read_timeout_ms.is_none() => {
                    cli.rpc_read_timeout_ms = Some(value.parse().map_err(|_| usage)?);
                }
                "--rpc-write-timeout-ms" if cli.rpc_write_timeout_ms.is_none() => {
                    cli.rpc_write_timeout_ms = Some(value.parse().map_err(|_| usage)?);
                }
                "--config" if cli.config.is_none() => cli.config = Some(value.into()),
                _ => return Err(usage.into()),
            }
        }
        let file = match &cli.config {
            Some(path) => read_config_file(path)?,
            None => FileConfig::default(),
        };
        validate_file_strings(&file)?;
        let address = cli
            .address
            .or(file.addr)
            .unwrap_or_else(|| default_address(required_role).to_owned());
        let role = resolve_role(
            required_role,
            cli.role.as_deref(),
            file.role.as_deref(),
            usage,
        )?;
        let prefix = cli
            .prefix
            .or(file.prefix)
            .unwrap_or_else(|| "rockbee".to_owned());
        let backend_name = cli
            .backend
            .or(file.backend)
            .unwrap_or_else(|| "localstack".to_owned());
        let bucket_supplied = cli.bucket.is_some() || file.bucket.is_some();
        let endpoint_supplied = cli.endpoint.is_some() || file.endpoint.is_some();
        let directory_supplied = cli.directory.is_some() || file.directory.is_some();
        let bucket = cli
            .bucket
            .or(file.bucket)
            .unwrap_or_else(|| "rockbee-test".to_owned());
        let endpoint = cli
            .endpoint
            .or(file.endpoint)
            .unwrap_or_else(|| "http://127.0.0.1:4566".to_owned());
        let directory = cli
            .directory
            .or(file.directory)
            .unwrap_or_else(|| PathBuf::from("./objects"));
        let backend = match backend_name.as_str() {
            "localstack" if directory_supplied => {
                return Err("--directory is only supported with --backend file".into());
            }
            "localstack" => Backend::LocalStack { bucket, endpoint },
            "s3" if endpoint_supplied => {
                return Err("--endpoint is only supported with --backend localstack".into());
            }
            "s3" if directory_supplied => {
                return Err("--directory is only supported with --backend file".into());
            }
            "s3" => Backend::S3 { bucket },
            "file" if bucket_supplied => {
                return Err("--bucket is not supported with --backend file".into());
            }
            "file" if endpoint_supplied => {
                return Err("--endpoint is only supported with --backend localstack".into());
            }
            "file" => Backend::File { directory },
            _ => return Err(usage.into()),
        };
        let rpc = rpc_config(
            cli.rpc_max_connections
                .or(file.rpc.as_ref().and_then(|rpc| rpc.max_connections)),
            cli.rpc_global_max_in_flight
                .or(file.rpc.as_ref().and_then(|rpc| rpc.global_max_in_flight)),
            cli.rpc_read_timeout_ms
                .or(file.rpc.as_ref().and_then(|rpc| rpc.read_timeout_ms)),
            cli.rpc_write_timeout_ms
                .or(file.rpc.as_ref().and_then(|rpc| rpc.write_timeout_ms)),
            usage,
        )?;
        let memory = memory_config(file.memory.as_ref())?;
        if role == Role::Writer && file.reader.is_some() {
            return Err("[reader] configuration is only supported by the reader role".into());
        }
        if role == Role::Writer && file.query.is_some() {
            return Err("[query] configuration is only supported by the reader role".into());
        }
        let mut reader = reader_config(file.reader.as_ref())?;
        reader.query = query_config(file.query.as_ref())?;
        Ok(Some(Self {
            address,
            role,
            prefix,
            backend,
            rpc,
            memory,
            reader,
            check_config: cli.check_config,
        }))
    }

    pub fn effective_json(&self) -> Value {
        let backend = match &self.backend {
            Backend::LocalStack { bucket, endpoint } => json!({
                "kind": "localstack", "bucket": bucket, "endpoint": endpoint,
            }),
            Backend::S3 { bucket } => json!({ "kind": "s3", "bucket": bucket }),
            Backend::File { directory } => json!({
                "kind": "file", "directory": directory,
            }),
        };
        let mut effective = json!({
            "role": match self.role { Role::Writer => "writer", Role::Reader => "reader" },
            "addr": self.address,
            "prefix": self.prefix,
            "backend": backend,
            "rpc": {
                "max_connections": self.rpc.max_connections(),
                "global_max_in_flight": self.rpc.global_max_in_flight(),
                "read_timeout_ms": self.rpc.read_timeout().as_millis(),
                "write_timeout_ms": self.rpc.write_timeout().as_millis(),
            },
            "memory": self.memory,
            "check_config": self.check_config,
        });
        if self.role == Role::Reader {
            effective["query"] = json!({
                "timeout_ms": self.reader.query().timeout().as_millis(),
                "memory_limit_mb": self.reader.query().memory_limit_mb(),
                "max_concurrent": self.reader.query().max_concurrent(),
            });
            effective["reader"] = json!({
                "poll_interval_ms": self.reader.manifest_poll_interval().as_millis(),
            });
        }
        effective
    }
}

#[derive(Default)]
struct CliConfig {
    address: Option<String>,
    role: Option<String>,
    prefix: Option<String>,
    backend: Option<String>,
    bucket: Option<String>,
    endpoint: Option<String>,
    directory: Option<PathBuf>,
    rpc_max_connections: Option<usize>,
    rpc_global_max_in_flight: Option<usize>,
    rpc_read_timeout_ms: Option<u64>,
    rpc_write_timeout_ms: Option<u64>,
    config: Option<PathBuf>,
    check_config: bool,
}

fn read_config_file(path: &PathBuf) -> Result<FileConfig> {
    let contents = std::fs::read_to_string(path)
        .map_err(|error| format!("failed to read config file {}: {error}", path.display()))?;
    toml::from_str(&contents)
        .map_err(|error| format!("invalid config file {}: {error}", path.display()).into())
}

fn validate_file_strings(file: &FileConfig) -> Result<()> {
    for value in [
        file.role.as_deref(),
        file.addr.as_deref(),
        file.prefix.as_deref(),
        file.backend.as_deref(),
        file.bucket.as_deref(),
        file.endpoint.as_deref(),
    ] {
        if matches!(value, Some("")) {
            return Err("config file strings must not be empty".into());
        }
    }
    if matches!(file.directory.as_deref(), Some(path) if path.as_os_str().is_empty()) {
        return Err("config file directory must not be empty".into());
    }
    Ok(())
}

fn default_address(required: Option<Role>) -> &'static str {
    match required {
        Some(Role::Writer) => "127.0.0.1:7101",
        Some(Role::Reader) => "127.0.0.1:7102",
        None => DEFAULT_ADDR,
    }
}

fn parse_role_value(role: &str, usage: &str) -> Result<Role> {
    match role {
        "writer" => Ok(Role::Writer),
        "reader" => Ok(Role::Reader),
        _ => Err(usage.into()),
    }
}

fn resolve_role(
    required: Option<Role>,
    cli: Option<&str>,
    file: Option<&str>,
    usage: &str,
) -> Result<Role> {
    let Some(required) = required else {
        return match cli.or(file) {
            Some(role) => parse_role_value(role, usage),
            None => Ok(Role::Writer),
        };
    };
    for supplied in [cli, file].into_iter().flatten() {
        if parse_role_value(supplied, usage)? != required {
            return Err("configured role conflicts with this executable".into());
        }
    }
    Ok(required)
}

fn memory_config(file: Option<&MemoryConfig>) -> Result<MemoryConfig> {
    let memory = file.cloned().unwrap_or_default();
    memory.validate()?;
    Ok(memory)
}

fn reader_config(file: Option<&FileReaderConfig>) -> Result<ReaderConfig> {
    let mut config = ReaderConfig::default();
    if let Some(poll_interval_ms) = file.and_then(|reader| reader.poll_interval_ms) {
        config.manifest_poll_interval = Duration::from_millis(poll_interval_ms);
    }
    config.validate()?;
    Ok(config)
}

fn query_config(file: Option<&FileQueryConfig>) -> Result<QueryConfig> {
    let mut config = QueryConfig::default();
    if let Some(file) = file {
        if let Some(value) = file.timeout_ms {
            config.timeout = Duration::from_millis(value);
        }
        if let Some(value) = file.memory_limit_mb {
            config.memory_limit_mb = value;
        }
        if let Some(value) = file.max_concurrent {
            config.max_concurrent = value;
        }
    }
    config.validate()?;
    Ok(config)
}

fn rpc_config(
    max_connections: Option<usize>,
    global_max_in_flight: Option<usize>,
    read_timeout_ms: Option<u64>,
    write_timeout_ms: Option<u64>,
    _usage: &str,
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

#[cfg(test)]
mod tests {
    use super::{FileReaderConfig, ReaderConfig, reader_config};
    use crate::memory::MemoryConfig;
    use std::time::Duration;

    #[test]
    fn reader_options_apply_the_resolved_poll_interval() {
        let reader = reader_config(Some(&FileReaderConfig {
            poll_interval_ms: Some(125),
        }))
        .expect("125 ms is compatible with the default checkpoint lifetime");

        let options = reader.options(&MemoryConfig::default());
        assert_eq!(options.manifest_poll_interval, Duration::from_millis(125));
        assert_eq!(options.checkpoint_lifetime, Duration::from_secs(600));
    }

    #[test]
    fn reader_default_is_the_pinned_slatedb_default() {
        assert_eq!(
            ReaderConfig::default().manifest_poll_interval(),
            Duration::from_secs(10)
        );
    }
}

#[cfg(test)]
mod query_tests {
    use super::{Config, QueryConfig};
    use crate::{Result, Role};
    use std::io::Write;

    fn parse(contents: &str, role: Role) -> Result<Config> {
        let mut file = tempfile::NamedTempFile::new()?;
        file.write_all(contents.as_bytes())?;
        Config::parse_for_role(
            role,
            [
                "--config".into(),
                file.path().display().to_string(),
                "--check-config".into(),
            ],
        )?
        .ok_or_else(|| "missing parsed query configuration".into())
    }

    #[test]
    fn query_defaults_and_nondefaults_are_visible_in_effective_configuration() -> Result<()> {
        let defaults = parse("", Role::Reader)?;
        assert_eq!(defaults.reader.query(), &QueryConfig::default());
        assert_eq!(
            defaults.effective_json()["query"],
            serde_json::json!({
                "timeout_ms": 4000, "memory_limit_mb": 256, "max_concurrent": 2,
            })
        );
        let configured = parse(
            "[query]\ntimeout_ms = 1250\nmemory_limit_mb = 512\nmax_concurrent = 4\n",
            Role::Reader,
        )?;
        assert_eq!(
            configured.effective_json()["query"],
            serde_json::json!({
                "timeout_ms": 1250, "memory_limit_mb": 512, "max_concurrent": 4,
            })
        );
        assert!(
            parse("", Role::Writer)?
                .effective_json()
                .get("query")
                .is_none()
        );
        Ok(())
    }

    #[test]
    fn query_deadline_must_be_representable() {
        let query = QueryConfig {
            timeout: std::time::Duration::MAX,
            ..QueryConfig::default()
        };
        assert!(query.validate().is_err());
    }

    #[test]
    fn invalid_query_limits_and_writer_sections_are_rejected() {
        for option in [
            "timeout_ms = 0",
            "memory_limit_mb = 0",
            "memory_limit_mb = 16385",
            "max_concurrent = 0",
            "max_concurrent = 129",
            "unknown = 1",
        ] {
            assert!(
                parse(&format!("[query]\n{option}\n"), Role::Reader).is_err(),
                "{option}"
            );
        }
        for option in [
            "memory_limit_mb = 1\nmax_concurrent = 1",
            "memory_limit_mb = 16384\nmax_concurrent = 128",
        ] {
            assert!(
                parse(&format!("[query]\n{option}\n"), Role::Reader).is_ok(),
                "{option}"
            );
        }
        let error = parse("[query]\ntimeout_ms = 4000\n", Role::Writer)
            .err()
            .expect("writer rejects query options");
        assert!(
            error
                .to_string()
                .contains("only supported by the reader role"),
            "{error}"
        );
    }
}
