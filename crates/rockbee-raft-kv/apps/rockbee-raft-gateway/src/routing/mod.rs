//! Directory-validated key routing helpers.

use rockbee_raft_protocol::{GroupId, KvRequest, ProtocolError, routing::ClusterDirectory};

/// Derives a data group solely from the operation key and persisted mapping.
pub(crate) fn group_for_request(
    directory: &ClusterDirectory,
    request: &KvRequest,
) -> Result<GroupId, ProtocolError> {
    let key = match request {
        KvRequest::Put { key, .. } | KvRequest::Get { key } | KvRequest::Delete { key } => key,
    };
    directory.shard_config.group_for_key(key)
}
