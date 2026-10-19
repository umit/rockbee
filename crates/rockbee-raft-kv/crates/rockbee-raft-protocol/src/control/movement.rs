use serde::{Deserialize, Serialize};

use crate::{
    GroupId, NodeId, ProtocolError,
    routing::{ClusterDirectory, NodeEndpoint},
};

/// A nonzero fixed-size identifier for one durable replica-move job.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "[u8; 16]", into = "[u8; 16]")]
pub struct MoveJobId([u8; 16]);

impl MoveJobId {
    /// Creates a job ID. All-zero bytes are reserved as an invalid sentinel.
    pub fn new(value: [u8; 16]) -> Result<Self, ProtocolError> {
        if value == [0; 16] {
            return Err(ProtocolError::InvalidIdentifier("MoveJobId"));
        }
        Ok(Self(value))
    }

    /// Returns the stable bytes persisted with the move record.
    pub const fn get(self) -> [u8; 16] {
        self.0
    }
}

impl TryFrom<[u8; 16]> for MoveJobId {
    type Error = ProtocolError;
    fn try_from(value: [u8; 16]) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<MoveJobId> for [u8; 16] {
    fn from(value: MoveJobId) -> Self {
        value.get()
    }
}

/// Operator input. It names one replacement, never an arbitrary voter set.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "MoveReplicaWire", into = "MoveReplicaWire")]
pub struct MoveReplica {
    /// Stable idempotency identity for this immutable job.
    pub job_id: MoveJobId,
    /// Data group to move; metadata group zero is excluded.
    pub data_group: GroupId,
    /// Existing voter removed from this group's final placement.
    pub remove_node: NodeId,
    /// New node's advertised peer and data endpoints.
    pub add_node: NodeEndpoint,
    /// Directory revision the operator inspected before requesting the move.
    pub expected_directory_revision: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct MoveReplicaWire {
    job_id: MoveJobId,
    data_group: GroupId,
    remove_node: NodeId,
    add_node: NodeEndpoint,
    expected_directory_revision: u64,
}

impl TryFrom<MoveReplicaWire> for MoveReplica {
    type Error = ProtocolError;
    fn try_from(value: MoveReplicaWire) -> Result<Self, Self::Error> {
        validate_data_group(value.data_group)?;
        validate_directory_revision(value.expected_directory_revision)?;
        validate_endpoint_shape(&value.add_node)?;
        if value.remove_node == value.add_node.node_id {
            return Err(ProtocolError::InvalidRouting(
                "replacement node must differ from removed node",
            ));
        }
        Ok(Self {
            job_id: value.job_id,
            data_group: value.data_group,
            remove_node: value.remove_node,
            add_node: value.add_node,
            expected_directory_revision: value.expected_directory_revision,
        })
    }
}

impl From<MoveReplica> for MoveReplicaWire {
    fn from(value: MoveReplica) -> Self {
        Self {
            job_id: value.job_id,
            data_group: value.data_group,
            remove_node: value.remove_node,
            add_node: value.add_node,
            expected_directory_revision: value.expected_directory_revision,
        }
    }
}

/// The complete fixed target derived from a validated directory.
///
/// Arrays express the selected first milestone precisely: three source voters
/// become three target voters by replacing exactly one node with D.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "MoveIntentWire", into = "MoveIntentWire")]
pub struct MoveIntent {
    /// Stable idempotency identity for this immutable move.
    pub job_id: MoveJobId,
    /// Data group whose exact three-voter placement changes once.
    pub data_group: GroupId,
    /// Source voter replaced in the permanent target.
    pub remove_node: NodeId,
    /// Replacement node and its peer/data endpoints.
    pub add_node: NodeEndpoint,
    /// Directory revision used to derive this exact placement.
    pub expected_directory_revision: u64,
    /// Canonically sorted three-voter placement observed before the move.
    pub old_voters: [NodeId; 3],
    /// Canonically sorted three-voter placement after replacing one voter.
    pub target_voters: [NodeId; 3],
}

/// Serializable representation validated before becoming a durable move intent.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct MoveIntentWire {
    job_id: MoveJobId,
    data_group: GroupId,
    remove_node: NodeId,
    add_node: NodeEndpoint,
    expected_directory_revision: u64,
    old_voters: [NodeId; 3],
    target_voters: [NodeId; 3],
}

impl TryFrom<MoveIntentWire> for MoveIntent {
    type Error = ProtocolError;

    fn try_from(value: MoveIntentWire) -> Result<Self, Self::Error> {
        let intent = Self {
            job_id: value.job_id,
            data_group: value.data_group,
            remove_node: value.remove_node,
            add_node: value.add_node,
            expected_directory_revision: value.expected_directory_revision,
            old_voters: value.old_voters,
            target_voters: value.target_voters,
        };
        intent.validate()?;
        Ok(intent)
    }
}

impl From<MoveIntent> for MoveIntentWire {
    fn from(value: MoveIntent) -> Self {
        Self {
            job_id: value.job_id,
            data_group: value.data_group,
            remove_node: value.remove_node,
            add_node: value.add_node,
            expected_directory_revision: value.expected_directory_revision,
            old_voters: value.old_voters,
            target_voters: value.target_voters,
        }
    }
}

impl MoveIntent {
    /// Validates the self-contained durable representation.
    ///
    /// This checks only facts encoded in the record. Metadata application must
    /// additionally compare its directory revision and every persisted job.
    ///
    /// # Errors
    /// Returns an error when an intent is not one canonical three-voter
    /// replacement or has invalid bounded fields.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_data_group(self.data_group)?;
        validate_directory_revision(self.expected_directory_revision)?;
        validate_endpoint_shape(&self.add_node)?;
        validate_sorted_voters(&self.old_voters)?;
        validate_sorted_voters(&self.target_voters)?;

        if !self.old_voters.contains(&self.remove_node)
            || self.old_voters.contains(&self.add_node.node_id)
            || self.add_node.node_id == self.remove_node
            || !self.target_voters.contains(&self.add_node.node_id)
            || self.target_voters.contains(&self.remove_node)
        {
            return Err(ProtocolError::InvalidRouting(
                "move voters do not describe one replacement",
            ));
        }

        let mut expected_target = self.old_voters;
        let remove_index = expected_target
            .iter()
            .position(|node| *node == self.remove_node)
            .ok_or(ProtocolError::InvalidRouting("removed voter is absent"))?;
        expected_target[remove_index] = self.add_node.node_id;
        expected_target.sort_unstable();
        if expected_target != self.target_voters {
            return Err(ProtocolError::InvalidRouting(
                "move target is not the exact replacement",
            ));
        }
        Ok(())
    }
}

impl MoveReplica {
    /// Derives the only allowed ABC-to-ABD intent from current durable routing.
    ///
    /// # Errors
    /// Returns an error when the directory is invalid/stale, the group is not a
    /// three-voter data group, endpoints conflict, or the replacement is not
    /// exactly one existing voter with one distinct node.
    pub fn derive_intent(&self, directory: &ClusterDirectory) -> Result<MoveIntent, ProtocolError> {
        directory.validate()?;
        if self.expected_directory_revision != directory.revision {
            return Err(ProtocolError::InvalidRouting(
                "invalid data group or directory revision",
            ));
        }
        validate_data_group(self.data_group)?;
        validate_directory_revision(self.expected_directory_revision)?;
        let source = directory
            .voters(self.data_group)
            .ok_or(ProtocolError::InvalidRouting("data group is not present"))?;
        let mut old_voters: [NodeId; 3] = source.try_into().map_err(|_| {
            ProtocolError::InvalidRouting("first move requires exactly three voters")
        })?;
        old_voters.sort_unstable();
        if !old_voters.contains(&self.remove_node)
            || old_voters.contains(&self.add_node.node_id)
            || self.add_node.node_id == self.remove_node
        {
            return Err(ProtocolError::InvalidRouting(
                "replacement must remove one voter and add a distinct node",
            ));
        }
        validate_new_endpoint(directory, &self.add_node)?;
        let mut target_voters = old_voters;
        let index = target_voters
            .iter()
            .position(|node| *node == self.remove_node)
            .ok_or(ProtocolError::InvalidRouting("removed voter is absent"))?;
        target_voters[index] = self.add_node.node_id;
        target_voters.sort_unstable();
        let intent = MoveIntent {
            job_id: self.job_id,
            data_group: self.data_group,
            remove_node: self.remove_node,
            add_node: self.add_node.clone(),
            expected_directory_revision: self.expected_directory_revision,
            old_voters,
            target_voters,
        };
        intent.validate()?;
        Ok(intent)
    }
}

fn validate_new_endpoint(
    directory: &ClusterDirectory,
    endpoint: &NodeEndpoint,
) -> Result<(), ProtocolError> {
    validate_endpoint_shape(endpoint)?;
    if let Some(existing) = directory.node(endpoint.node_id)
        && existing != endpoint
    {
        return Err(ProtocolError::InvalidRouting(
            "replacement node ID has conflicting endpoints",
        ));
    }
    for existing in &directory.nodes {
        if existing.node_id != endpoint.node_id
            && (existing.peer_address == endpoint.peer_address
                || existing.peer_address == endpoint.data_address
                || existing.data_address == endpoint.peer_address
                || existing.data_address == endpoint.data_address)
        {
            return Err(ProtocolError::InvalidRouting(
                "replacement endpoint conflicts with directory",
            ));
        }
    }
    Ok(())
}

fn validate_data_group(group: GroupId) -> Result<(), ProtocolError> {
    if group.is_metadata() || group.get() > 256 {
        return Err(ProtocolError::InvalidRouting(
            "move requires a configured data group",
        ));
    }
    Ok(())
}

fn validate_directory_revision(revision: u64) -> Result<(), ProtocolError> {
    if revision == 0 {
        return Err(ProtocolError::InvalidRouting(
            "move requires a nonzero directory revision",
        ));
    }
    Ok(())
}

fn validate_endpoint_shape(endpoint: &NodeEndpoint) -> Result<(), ProtocolError> {
    if endpoint.peer_address.port() == 0
        || endpoint.data_address.port() == 0
        || endpoint.peer_address.ip().is_unspecified()
        || endpoint.data_address.ip().is_unspecified()
        || endpoint.peer_address.ip().is_multicast()
        || endpoint.data_address.ip().is_multicast()
        || endpoint.peer_address == endpoint.data_address
    {
        return Err(ProtocolError::InvalidRouting(
            "invalid replacement endpoint",
        ));
    }
    Ok(())
}

fn validate_sorted_voters(voters: &[NodeId; 3]) -> Result<(), ProtocolError> {
    if voters.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(ProtocolError::InvalidRouting(
            "move voters must be sorted and unique",
        ));
    }
    Ok(())
}
