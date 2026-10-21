//! Cross-boundary size checks before a metadata command reaches storage or RPC.
use crate::{
    cluster::NodeError, control::MetadataCommand, rpc::PeerLimits, storage::StorageOptions,
};

/// Reused before every internal control proposal. Input size, durable output
/// capacity and private replication admission are distinct bounded checks.
pub(crate) fn validate_metadata_candidate(
    command: &MetadataCommand,
    storage: &StorageOptions,
    peers: PeerLimits,
) -> Result<(), NodeError> {
    command.validate()?;
    let minimum = storage.metadata_minimum_chunk_bytes()?;
    if storage.io_chunk_bytes < minimum {
        return Err(NodeError::Config(format!(
            "metadata output requires chunk bytes of at least {minimum}"
        )));
    }
    let entry_bytes = crate::storage::metadata_entry_bytes(command)?;
    if entry_bytes > storage.max_entry_bytes {
        return Err(NodeError::Config(format!(
            "metadata durable entry requires {entry_bytes} bytes"
        )));
    }
    let frame_bytes = crate::rpc::metadata_append_bytes(command)?;
    if frame_bytes > peers.max_frame_bytes {
        return Err(NodeError::Config(format!(
            "metadata private append requires {frame_bytes} frame bytes"
        )));
    }
    Ok(())
}
