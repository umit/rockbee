//! Private, bounded TCP RPC used only between Rockbee Raft storage nodes.
//!
//! This module is deliberately separate from the public client protocol. A public
//! request reaches a group's leader first; OpenRaft then uses this module to send
//! replication messages to its peers. `PeerNetwork` is the OpenRaft adapter and
//! `PeerServer` is the receiving side.
//!
//! For Java readers: `PeerNetworkFactory` is analogous to a factory injected into
//! each Raft group. It creates one `PeerNetwork` per remote node. The traits from
//! OpenRaft are interfaces; their three methods map directly to AppendEntries,
//! Vote, and InstallSnapshot RPCs.

mod admission;
mod dto;
mod error;
mod message;
mod network;
mod server;

pub use admission::{PeerLimits, PeerResources};
pub use error::PeerError;
pub use network::{PeerNetwork, PeerNetworkFactory};
pub use server::{PeerIdentity, PeerServer};
