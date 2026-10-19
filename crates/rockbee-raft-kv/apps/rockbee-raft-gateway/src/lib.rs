//! Public-client gateway for Rockbee Raft KV.
//!
//! The gateway accepts public byte-key/value requests, looks up the durable
//! metadata directory, maps each key to a fixed Raft group, and uses bounded
//! backend links to nodes. It never caches user values and never retries a
//! mutation whose backend transport might have handed it to a node.

pub mod admission;
pub mod config;
pub mod metrics;
pub mod routing;
pub mod server;

pub use config::{GatewayConfig, GatewayConfigError, SeedNode};
pub use metrics::{GatewayMetrics, GatewayMetricsSnapshot};
pub use server::{Gateway, GatewayError, GatewayRuntime};
