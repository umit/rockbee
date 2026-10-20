//! Bounded control replies; no request accepts a completion witness.
use crate::{GroupId, NodeId, ProtocolError, control::MoveRecord, routing::ClusterDirectory};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, net::SocketAddr};

/// A coherent authoritative group-zero view of a job and its current directory.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MoveState {
    /// Current routing, read atomically with the retained record.
    pub directory: ClusterDirectory,
    /// One lifetime record, or no accepted job.
    pub record: Option<MoveRecord>,
}

/// Scheduling is separate from durable acceptance and completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ReconcileStatus {
    /// A new receiver-owned runner was started.
    Scheduled,
    /// The same group's receiver-owned runner is already active.
    Running,
    /// Process control admission is full; no new runner was started.
    Busy,
    /// Runtime is draining; no new runner was started.
    Stopped,
}

/// Fresh applied membership, bound to its real data group.
/// Fixed arrays bound every decoded nested collection before allocation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AppliedMembership {
    /// Actual hosting group, checked against the request.
    pub data_group: GroupId,
    /// Full identity read from the same snapshot as membership.
    pub applied: ObservedLogPosition,
    /// Full identity of the applied membership entry.
    pub membership: ObservedLogPosition,
    /// First canonical three-voter configuration.
    pub voters: [NodeId; 3],
    /// Second configuration during joint consensus.
    pub joint_voters: Option<[NodeId; 3]>,
    /// Actual peer endpoints; Raft does not store backend/data addresses.
    pub peers: [Option<(NodeId, SocketAddr)>; 16],
}

impl AppliedMembership {
    /// Checks finite topology, peer uniqueness and coherent applied identity.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        let invalid = || ProtocolError::InvalidRouting("invalid applied membership observation");
        self.applied.validate()?;
        self.membership.validate()?;
        if self.data_group.is_metadata()
            || self.data_group.get() > 256
            || !position_covers(self.applied, self.membership)
            || self.voters.windows(2).any(|pair| pair[0] >= pair[1])
            || self.joint_voters.is_some_and(|voters| {
                voters == self.voters || voters.windows(2).any(|pair| pair[0] >= pair[1])
            })
        {
            return Err(invalid());
        }
        let mut ids = BTreeSet::new();
        let mut addresses = BTreeSet::new();
        for (id, address) in self.peers.iter().flatten() {
            if !ids.insert(*id)
                || !addresses.insert(*address)
                || address.port() == 0
                || address.ip().is_unspecified()
                || address.ip().is_multicast()
            {
                return Err(invalid());
            }
        }
        if self
            .voters
            .iter()
            .chain(self.joint_voters.iter().flatten())
            .any(|id| !ids.contains(id))
        {
            return Err(invalid());
        }
        Ok(())
    }

    /// The peer address for an observed membership node.
    pub fn peer(&self, node: NodeId) -> Option<SocketAddr> {
        self.peers
            .iter()
            .flatten()
            .find_map(|(id, address)| (*id == node).then_some(*address))
    }
}

fn position_covers(applied: ObservedLogPosition, required: ObservedLogPosition) -> bool {
    applied == required
        || (applied.index > required.index
            && (applied.term, applied.leader_node_id) >= (required.term, required.leader_node_id))
}

/// Full native log identity, including OpenRaft's initialization membership.
/// Only the exact genesis entry may have a zero leader ID. This is observation
/// wire data; the persisted completion proof retains its nonzero NodeId type.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObservedLogPosition {
    /// Native committed leader term.
    pub term: u64,
    /// Native leader ID; zero is permitted only at (term0,index0).
    pub leader_node_id: u64,
    /// Native log index.
    pub index: u64,
}
impl ObservedLogPosition {
    /// Validates the native genesis exception without substituting an identity.
    pub fn validate(self) -> Result<(), ProtocolError> {
        if self.leader_node_id == 0 && (self.term != 0 || self.index != 0) {
            return Err(ProtocolError::InvalidRouting(
                "zero leader ID outside native genesis",
            ));
        }
        Ok(())
    }
}
