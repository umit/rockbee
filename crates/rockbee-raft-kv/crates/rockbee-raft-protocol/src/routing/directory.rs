//! Bounded cluster placement received from the metadata group's leader.
use std::{collections::BTreeSet, fmt, marker::PhantomData, net::SocketAddr};

use serde::{
    Deserialize, Deserializer, Serialize,
    de::{self, SeqAccess, Visitor},
};

use crate::{ClusterId, GroupId, NodeId, ProtocolError, ShardConfig};

/// Maximum configured nodes in the initial prototype, including learners.
pub const MAX_DIRECTORY_NODES: usize = 16;
const MAX_DIRECTORY_GROUPS: usize = 257;

/// Addresses have separate roles: Raft traffic must not share data admission.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeEndpoint {
    /// Stable node identity; addresses alone are not node identities.
    pub node_id: NodeId,
    /// Advertised private replication listener.
    pub peer_address: SocketAddr,
    /// Advertised gateway-to-node data listener.
    pub data_address: SocketAddr,
}

/// Stable placement of one group; current leader is discovered separately.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GroupPlacement {
    /// Zero is metadata; data groups start at one.
    pub group_id: GroupId,
    /// Voters known at this directory revision, not proof of current authority.
    #[serde(deserialize_with = "deserialize_voters")]
    pub voters: Vec<NodeId>,
}

/// A replicated routing directory. Revision is not a controller fencing token.
///
/// Collection counts are bounded during deserialization. Call `validate` after
/// decoding to check cross-references, uniqueness, coverage, and addresses.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterDirectory {
    /// Identity of the cluster this placement belongs to.
    pub cluster_id: ClusterId,
    /// Nonzero configuration revision; initial static placement uses one.
    pub revision: u64,
    /// Immutable key-to-group mapping for this cluster.
    pub shard_config: ShardConfig,
    /// Bounded advertised node directory.
    #[serde(deserialize_with = "deserialize_nodes")]
    pub nodes: Vec<NodeEndpoint>,
    /// Exactly one placement for metadata and each configured data group.
    #[serde(deserialize_with = "deserialize_placements")]
    pub placements: Vec<GroupPlacement>,
}

impl ClusterDirectory {
    /// Builds the first three-voter placement, with every group on all nodes.
    ///
    /// # Errors
    /// Rejects any node count other than three, or invalid/duplicate endpoints.
    pub fn initial(
        cluster_id: ClusterId,
        shard_config: ShardConfig,
        mut nodes: Vec<NodeEndpoint>,
    ) -> Result<Self, ProtocolError> {
        if nodes.len() != 3 {
            return Err(ProtocolError::InvalidRouting(
                "initial placement needs three nodes",
            ));
        }
        nodes.sort_by_key(|node| node.node_id);
        let voters: Vec<_> = nodes.iter().map(|node| node.node_id).collect();
        let directory = Self {
            cluster_id,
            revision: 1,
            shard_config,
            nodes,
            placements: (0..=u64::from(shard_config.count()))
                .map(|group| GroupPlacement {
                    group_id: GroupId::new(group),
                    voters: voters.clone(),
                })
                .collect(),
        };
        directory.validate()?;
        Ok(directory)
    }

    /// Checks the full placement contract before using any routing information.
    ///
    /// # Errors
    /// Rejects invalid counts, duplicate IDs/endpoints, missing groups or voters,
    /// and voter references to nodes absent from this directory.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.revision == 0 || self.nodes.is_empty() || self.nodes.len() > MAX_DIRECTORY_NODES {
            return Err(ProtocolError::InvalidRouting(
                "invalid revision or node count",
            ));
        }
        let mut node_ids = BTreeSet::new();
        let mut addresses = BTreeSet::new();
        for node in &self.nodes {
            if !node_ids.insert(node.node_id) {
                return Err(ProtocolError::InvalidRouting("duplicate node ID"));
            }
            for address in [node.peer_address, node.data_address] {
                if address.port() == 0
                    || address.ip().is_unspecified()
                    || address.ip().is_multicast()
                    || !addresses.insert(address)
                {
                    return Err(ProtocolError::InvalidRouting(
                        "invalid or duplicate advertised address",
                    ));
                }
            }
        }
        if self.placements.len() != usize::from(self.shard_config.count()) + 1 {
            return Err(ProtocolError::InvalidRouting(
                "placement must cover every group",
            ));
        }
        let mut groups = BTreeSet::new();
        for placement in &self.placements {
            if placement.group_id.get() > u64::from(self.shard_config.count())
                || !groups.insert(placement.group_id)
                || placement.voters.is_empty()
                || placement.voters.len() > MAX_DIRECTORY_NODES
            {
                return Err(ProtocolError::InvalidRouting(
                    "invalid group or voter count",
                ));
            }
            let mut voters = BTreeSet::new();
            for voter in &placement.voters {
                if !node_ids.contains(voter) || !voters.insert(*voter) {
                    return Err(ProtocolError::InvalidRouting("unknown or duplicate voter"));
                }
            }
        }
        Ok(())
    }

    /// Looks up a configured node without cloning its endpoints.
    pub fn node(&self, node: NodeId) -> Option<&NodeEndpoint> {
        self.nodes.iter().find(|endpoint| endpoint.node_id == node)
    }

    /// Returns configured voters, which are routing candidates rather than leases.
    pub fn voters(&self, group: GroupId) -> Option<&[NodeId]> {
        self.placements
            .iter()
            .find(|placement| placement.group_id == group)
            .map(|placement| placement.voters.as_slice())
    }
}

fn deserialize_nodes<'de, D: Deserializer<'de>>(decoder: D) -> Result<Vec<NodeEndpoint>, D::Error> {
    deserialize_limited::<D, NodeEndpoint, MAX_DIRECTORY_NODES>(decoder)
}
fn deserialize_voters<'de, D: Deserializer<'de>>(decoder: D) -> Result<Vec<NodeId>, D::Error> {
    deserialize_limited::<D, NodeId, MAX_DIRECTORY_NODES>(decoder)
}
fn deserialize_placements<'de, D: Deserializer<'de>>(
    decoder: D,
) -> Result<Vec<GroupPlacement>, D::Error> {
    deserialize_limited::<D, GroupPlacement, MAX_DIRECTORY_GROUPS>(decoder)
}

// Count limits belong in the decoder, before Vec reserves from an untrusted
// size hint. Also reject an extra element before constructing its value when
// a deserializer does not provide a size hint.
fn deserialize_limited<'de, D, T, const LIMIT: usize>(decoder: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct Limited<T, const LIMIT: usize>(PhantomData<T>);
    impl<'de, T: Deserialize<'de>, const LIMIT: usize> Visitor<'de> for Limited<T, LIMIT> {
        type Value = Vec<T>;
        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(formatter, "a sequence of at most {LIMIT} elements")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Vec<T>, A::Error> {
            if sequence.size_hint().is_some_and(|count| count > LIMIT) {
                return Err(de::Error::custom("directory collection exceeds limit"));
            }
            let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or(0));
            for _ in 0..LIMIT {
                let Some(value) = sequence.next_element()? else {
                    return Ok(values);
                };
                values.push(value);
            }
            if sequence.next_element::<RejectExtra>()?.is_some() {
                return Err(de::Error::custom("directory collection exceeds limit"));
            }
            Ok(values)
        }
    }
    decoder.deserialize_seq(Limited::<T, LIMIT>(PhantomData))
}

struct RejectExtra;
impl<'de> Deserialize<'de> for RejectExtra {
    fn deserialize<D: Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
        Err(de::Error::custom("directory collection exceeds limit"))
    }
}
