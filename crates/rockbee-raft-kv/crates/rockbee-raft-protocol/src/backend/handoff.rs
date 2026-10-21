//! Bounded predecessor-handoff messages shared by the gateway and node control.

use crate::{
    GroupId, ProtocolError,
    control::{CommittedLogPosition, MoveIntent, MoveJobId, MoveProgress, MoveRecord},
    routing::{ClusterDirectory, NodeEndpoint},
};
use serde::{Deserialize, Serialize};

/// Frozen candidate and exact predecessor executor census; never runnable.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PreparingSuccessor {
    /// Immutable successor target retained by metadata.
    pub intent: MoveIntent,
    /// Exact sorted predecessor executor census.
    pub executors: [NodeEndpoint; 4],
}

/// One receiver's durable local predecessor seal and drain receipt.
/// A distributed handoff requires every receipt and a later fence; this is not
/// a self-contained certificate.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SealReceipt {
    /// Exact endpoint that durably sealed its predecessor role.
    pub endpoint: NodeEndpoint,
    /// Data group sealed by this endpoint.
    pub data_group: GroupId,
    /// Frozen predecessor directory revision.
    pub predecessor_revision: u64,
    /// Immutable predecessor move identity.
    pub predecessor_job_id: MoveJobId,
}

/// Actual data-log fence identity, explicitly bound to both jobs and the group.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HandoffFenceReceipt {
    /// Data group fenced by the committed data-log entry.
    pub data_group: GroupId,
    /// Frozen predecessor directory revision.
    pub predecessor_revision: u64,
    /// Immutable predecessor move identity.
    pub predecessor_job_id: MoveJobId,
    /// Immutable successor move identity.
    pub successor_job_id: MoveJobId,
    /// Native committed data-log identity of the fence.
    pub committed: CommittedLogPosition,
}

/// A coherent immutable predecessor and preparing-successor snapshot.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PreparedMoveState {
    /// Current authoritative directory read with the retained handoff state.
    pub directory: ClusterDirectory,
    /// Completed immutable predecessor record.
    pub first: MoveRecord,
    /// Frozen successor preparation and predecessor executor census.
    pub preparing: PreparingSuccessor,
}

impl PreparedMoveState {
    /// Validates the coherent immutable predecessor and preparing-successor state.
    ///
    /// The directory may have advanced after preparation because unrelated groups
    /// can complete independently. This therefore checks the retained lineage
    /// and the current group's placement without requiring equal revisions.
    ///
    /// # Errors
    /// Returns an error if the directory, move lineage, or fixed executor census
    /// is malformed or does not describe the requested first successor.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.directory.validate()?;
        self.first.validate()?;
        self.preparing.intent.validate()?;

        let first = &self.first.intent;
        let preparing = &self.preparing.intent;
        if !matches!(self.first.progress, MoveProgress::Completed { .. })
            || preparing.data_group != first.data_group
            || preparing.job_id == first.job_id
            || preparing.expected_directory_revision <= first.expected_directory_revision
            || preparing.expected_directory_revision > self.directory.revision
            || preparing.old_voters != first.target_voters
            || !self
                .directory
                .voters(first.data_group)
                .is_some_and(|voters| {
                    voters.len() == preparing.old_voters.len()
                        && preparing
                            .old_voters
                            .iter()
                            .all(|voter| voters.contains(voter))
                })
        {
            return Err(ProtocolError::InvalidRouting(
                "invalid prepared move lineage",
            ));
        }
        if self.directory.node(first.add_node.node_id) != Some(&first.add_node) {
            return Err(ProtocolError::InvalidRouting(
                "prepared move predecessor endpoint mismatch",
            ));
        }
        match self.directory.node(preparing.add_node.node_id) {
            Some(endpoint) if endpoint == &preparing.add_node => {}
            Some(_) => {
                return Err(ProtocolError::InvalidRouting(
                    "prepared move successor endpoint mismatch",
                ));
            }
            None if self.directory.nodes.len() >= crate::routing::MAX_DIRECTORY_NODES => {
                return Err(ProtocolError::InvalidRouting(
                    "prepared move successor exceeds catalog capacity",
                ));
            }
            None if self.directory.nodes.iter().any(|endpoint| {
                [endpoint.peer_address, endpoint.data_address]
                    .iter()
                    .any(|address| {
                        *address == preparing.add_node.peer_address
                            || *address == preparing.add_node.data_address
                    })
            }) =>
            {
                return Err(ProtocolError::InvalidRouting(
                    "prepared move successor endpoint aliases catalog",
                ));
            }
            None => {}
        }

        let [a, b, c] = first.old_voters;
        let mut ids = [a, b, c, first.add_node.node_id];
        ids.sort_unstable();
        if ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ProtocolError::InvalidRouting(
                "invalid prepared move executor census",
            ));
        }
        let endpoint = |id| {
            self.directory
                .node(id)
                .cloned()
                .ok_or(ProtocolError::InvalidRouting(
                    "prepared move executor is absent",
                ))
        };
        let expected = [
            endpoint(ids[0])?,
            endpoint(ids[1])?,
            endpoint(ids[2])?,
            endpoint(ids[3])?,
        ];
        if self.preparing.executors != expected {
            return Err(ProtocolError::InvalidRouting(
                "prepared move executor census mismatch",
            ));
        }
        Ok(())
    }
}
