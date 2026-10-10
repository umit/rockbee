use crate::storage::memory::MemoryConfig;
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FileConfig {
    pub role: Option<String>,
    pub addr: Option<String>,
    pub prefix: Option<String>,
    pub backend: Option<String>,
    pub bucket: Option<String>,
    pub endpoint: Option<String>,
    pub directory: Option<PathBuf>,
    pub rpc: Option<FileRpcConfig>,
    pub memory: Option<MemoryConfig>,
    pub reader: Option<FileReaderConfig>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FileRpcConfig {
    pub max_connections: Option<usize>,
    pub global_max_in_flight: Option<usize>,
    pub read_timeout_ms: Option<u64>,
    pub write_timeout_ms: Option<u64>,
}

/// Reader-only SlateDB options. Values are expressed in milliseconds so TOML
/// stays easy to inspect alongside the RPC timeout settings.
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FileReaderConfig {
    pub poll_interval_ms: Option<u64>,
}
