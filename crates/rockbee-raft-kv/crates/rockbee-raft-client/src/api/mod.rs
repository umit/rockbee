//! Public byte-key/value operations and eager mutation observation.
mod client;
mod mutation;

pub use client::RockBeeRaftClient;
pub use mutation::MutationHandle;
