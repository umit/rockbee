//! Gateway construction settings and validated bootstrap seeds.

use std::{net::SocketAddr, time::Duration};

use rockbee_raft_protocol::{
    ClusterId, NodeId, ShardConfig, transport::LinkConfig, transport::server::ServerConfig,
};
use thiserror::Error;

/// A known data endpoint used only to discover the replicated metadata group.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SeedNode {
    /// Stable node identity expected from the connected backend.
    pub node_id: NodeId,
    /// Gateway-to-node data address, never the Raft peer address.
    pub address: SocketAddr,
}

/// All dependencies needed to start a gateway process.
#[derive(Clone, Debug)]
pub struct GatewayConfig {
    /// Cluster identity every node response must match.
    pub cluster_id: ClusterId,
    /// Immutable key-to-data-group mapping expected by this gateway.
    pub expected_shard_config: ShardConfig,
    /// Bounded bootstrap candidates. They are not trusted routing state.
    pub seeds: Vec<SeedNode>,
    /// Public client listener address.
    pub listen_address: SocketAddr,
    /// Each gateway-to-node connection generation's admission limits.
    pub backend_link: LinkConfig,
    /// Aggregate public-server admission limits.
    pub server: ServerConfig,
    /// One absolute budget for initial directory discovery.
    pub startup_timeout: Duration,
}

impl GatewayConfig {
    /// Validates settings before binding or connecting.
    pub fn validate(&self) -> Result<(), GatewayConfigError> {
        if self.seeds.is_empty() || self.seeds.len() > 16 {
            return Err(GatewayConfigError::Seeds);
        }
        if self.listen_address.port() == 0 || self.listen_address.ip().is_unspecified() {
            return Err(GatewayConfigError::ListenAddress);
        }
        if self.startup_timeout.is_zero() || self.startup_timeout > Duration::from_secs(60) {
            return Err(GatewayConfigError::StartupTimeout);
        }
        let mut ids = std::collections::BTreeSet::new();
        let mut addresses = std::collections::BTreeSet::new();
        for seed in &self.seeds {
            if seed.address.port() == 0
                || seed.address.ip().is_unspecified()
                || !ids.insert(seed.node_id)
                || !addresses.insert(seed.address)
            {
                return Err(GatewayConfigError::Seeds);
            }
        }
        Ok(())
    }
}

/// Startup settings rejected before any public listener is created.
#[derive(Debug, Error)]
pub enum GatewayConfigError {
    /// No usable unique bootstrap node was configured.
    #[error("gateway needs one through sixteen unique seed nodes")]
    Seeds,
    /// A public listener needs a concrete nonzero address.
    #[error("gateway listener address must be concrete and use a nonzero port")]
    ListenAddress,
    /// Directory discovery must have a finite bounded deadline.
    #[error("gateway startup timeout must be between one millisecond and sixty seconds")]
    StartupTimeout,
}
