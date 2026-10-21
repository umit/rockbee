//! OpenRaft storage interfaces implemented using the shared RocksDB engine.
mod log;
pub(super) mod state;

pub use log::{LogReader, LogStore};
pub use state::{StateMachine, StateRead, StateReader};
