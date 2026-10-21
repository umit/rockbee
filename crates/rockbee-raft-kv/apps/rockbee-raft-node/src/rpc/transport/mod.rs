//! TCP transport for private Raft traffic.
//!
//! `client` implements OpenRaft's outbound network traits. `server` accepts,
//! validates, and dispatches inbound peer requests.
pub(super) mod client;
mod server;

pub(super) use client::{APPEND_ENVELOPE_RESERVATION, metadata_entry_reservation};
pub use client::{PeerNetwork, PeerNetworkFactory};
pub use server::{PeerIdentity, PeerServer};
