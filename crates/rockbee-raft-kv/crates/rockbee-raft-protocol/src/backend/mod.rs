//! Gateway-to-node messages, separate from public client KV and private Raft RPC.
//!
//! A cached leader hint is only a routing suggestion. The destination still
//! checks its authority before reading and classifies writes by submission.
use serde::{Deserialize, Serialize};

use crate::{
    ClusterId, GroupId, KvExpectedResponse, KvRequest, KvResponse, MutationOutcome, NodeId,
    ProtocolError, ReadFailure, WireRequest, routing::ClusterDirectory,
};

/// One node operation. Metadata is read through its own operation, not public KV.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BackendOperation {
    /// Linearizable read of the persisted directory from group zero.
    Directory,
    /// Local leader hint, which does not establish read or write authority.
    Leader {
        /// Group whose current leader should be discovered.
        group_id: GroupId,
    },
    /// Public key/value operation already routed to a logical data group.
    Kv {
        /// Destination data group; node verifies the key's configured mapping.
        group_id: GroupId,
        /// Operation carrying opaque key/value bytes.
        request: KvRequest,
    },
    /// Publishes only the receiver's immutable initial directory in group zero.
    /// No caller-supplied metadata or arbitrary reserved-key write is accepted.
    BootstrapDirectory,
}

/// Explicit destination identity prevents accidental cross-cluster/node routing.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BackendRequest {
    /// Expected cluster identity.
    pub cluster_id: ClusterId,
    /// Expected node identity, verified independently of its socket address.
    pub target_node: NodeId,
    /// Typed operation to execute.
    pub operation: BackendOperation,
}

/// Typed result of a backend operation; hints never replace actual certainty.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BackendReply {
    /// Successful metadata read or an explicit read failure.
    Directory(Result<ClusterDirectory, ReadFailure>),
    /// A local leader observation, possibly unknown.
    Leader {
        /// Group that the hint describes.
        group_id: GroupId,
        /// Last observed leader; the gateway must validate the destination again.
        leader: Option<NodeId>,
    },
    /// Operation result and an optional routing hint for later safe requests.
    Kv {
        /// Same value/absence/certainty contract as the public client.
        response: KvResponse,
        /// Candidate leader, never permission to replay an uncertain mutation.
        leader_hint: Option<NodeId>,
    },
    /// Durable initial publication, safe rejection, or uncertain submission.
    Bootstrap(MutationOutcome),
}

/// A backend response identifies the node that produced it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BackendResponse {
    /// Responding cluster identity.
    pub cluster_id: ClusterId,
    /// Responding node identity.
    pub node_id: NodeId,
    /// Typed operation result.
    pub reply: BackendReply,
}

/// Small response-matching token; no request key/value is retained here.
#[derive(Clone, Copy, Debug)]
pub struct BackendExpected {
    cluster_id: ClusterId,
    node_id: NodeId,
    kind: ExpectedKind,
}
#[derive(Clone, Copy, Debug)]
enum ExpectedKind {
    Directory,
    Leader(GroupId),
    Kv(KvExpectedResponse),
    Bootstrap,
}

impl WireRequest<BackendResponse> for BackendRequest {
    type Expected = BackendExpected;

    // A directory can contain 257 placement structs and 257*16 voter IDs, plus
    // 16 endpoint structs. Decoded Vecs and validation sets can be much larger
    // than Postcard's compact integers. Reserve 64 KiB before decoding, beyond
    // the ordinary encoded+decoded byte and metadata allowance.
    const RESPONSE_DECODE_HEADROOM: usize = 64 * 1024;

    fn validate(&self) -> Result<(), ProtocolError> {
        match &self.operation {
            BackendOperation::Directory | BackendOperation::BootstrapDirectory => Ok(()),
            BackendOperation::Leader { group_id } => validate_group(*group_id, true),
            BackendOperation::Kv { group_id, request } => {
                validate_group(*group_id, false)?;
                request.validate()
            }
        }
    }

    fn retained_bytes(&self) -> Result<usize, ProtocolError> {
        self.validate()?;
        let body = match &self.operation {
            BackendOperation::Kv { request, .. } => request.retained_bytes()?,
            _ => 0,
        };
        // Counting the inline KV body twice is deliberately conservative and
        // small; retained byte admission must cover every owned payload.
        Ok(size_of::<Self>() + body)
    }

    fn expected_response(&self) -> Self::Expected {
        BackendExpected {
            cluster_id: self.cluster_id,
            node_id: self.target_node,
            kind: match &self.operation {
                BackendOperation::Directory => ExpectedKind::Directory,
                BackendOperation::BootstrapDirectory => ExpectedKind::Bootstrap,
                BackendOperation::Leader { group_id } => ExpectedKind::Leader(*group_id),
                BackendOperation::Kv { request, .. } => {
                    ExpectedKind::Kv(request.expected_response())
                }
            },
        }
    }

    fn validate_response(
        expected: Self::Expected,
        response: &BackendResponse,
    ) -> Result<(), ProtocolError> {
        if response.cluster_id != expected.cluster_id || response.node_id != expected.node_id {
            return Err(ProtocolError::InvalidRouting(
                "backend response identity mismatch",
            ));
        }
        match (expected.kind, &response.reply) {
            (ExpectedKind::Directory, BackendReply::Directory(Ok(directory))) => {
                directory.validate()?;
                if directory.cluster_id != expected.cluster_id {
                    return Err(ProtocolError::InvalidRouting("directory cluster mismatch"));
                }
                Ok(())
            }
            (ExpectedKind::Directory, BackendReply::Directory(Err(error))) => error.0.validate(),
            (ExpectedKind::Leader(expected), BackendReply::Leader { group_id, .. })
                if expected == *group_id =>
            {
                Ok(())
            }
            (ExpectedKind::Kv(expected), BackendReply::Kv { response, .. }) => {
                KvRequest::validate_response(expected, response)
            }
            (ExpectedKind::Bootstrap, BackendReply::Bootstrap(outcome)) => outcome.validate_wire(),
            _ => Err(ProtocolError::ResponseKindMismatch),
        }
    }
}

fn validate_group(group: GroupId, allow_metadata: bool) -> Result<(), ProtocolError> {
    if group.get() > 256 || (!allow_metadata && group.is_metadata()) {
        return Err(ProtocolError::InvalidRouting("invalid destination group"));
    }
    Ok(())
}
