//! Private, bounded TCP RPC used only between Rockbee Raft storage nodes.
//!
//! This module is deliberately separate from the public client protocol. A public
//! request reaches a group's leader first; OpenRaft then uses this module to send
//! replication messages to its peers. `transport::PeerNetwork` is the OpenRaft
//! adapter and `transport::PeerServer` is the receiving side.
//!
//! For Java readers: `PeerNetworkFactory` is analogous to a factory injected into
//! each Raft group. It creates one `PeerNetwork` per remote node. The traits from
//! OpenRaft are interfaces; their three methods map directly to AppendEntries,
//! Vote, and InstallSnapshot RPCs.
//!
//! - `admission` owns process-wide connection and operation limits.
//! - `wire` owns bounded DTOs, peer headers, and postcard frames.
//! - `transport` owns the outbound OpenRaft adapter and inbound TCP server.
mod admission;
#[cfg(test)]
pub(crate) mod test_support;
mod transport;
mod wire;

pub use admission::{PeerLimits, PeerResources};
pub use transport::{PeerIdentity, PeerNetwork, PeerNetworkFactory, PeerServer};
pub use wire::PeerError;
pub(crate) use wire::metadata_append_bytes;
