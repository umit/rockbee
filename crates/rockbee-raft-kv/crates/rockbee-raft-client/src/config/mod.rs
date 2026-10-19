//! Client deadline and connection admission settings.
use rockbee_raft_protocol::transport::LinkConfig;
use std::time::Duration;

/// Per-operation deadline and shared connection admission settings.
#[derive(Clone, Copy, Debug)]
pub struct ClientConfig {
    /// One millisecond through sixty seconds, including queue and reply waits.
    pub request_timeout: Duration,
    /// Count and logical ownership byte limits for one connection.
    pub link: LinkConfig,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(5),
            link: LinkConfig::default(),
        }
    }
}
