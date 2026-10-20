//! Fresh coherent applied membership and full-identity catch-up checks.
use super::*;

impl NodeService {
    pub(crate) async fn observe_membership(
        &self,
        data_group: GroupId,
        deadline: Instant,
    ) -> Result<AppliedMembership, ReadFailure> {
        let group = self
            .groups
            .get(&data_group)
            .filter(|_| !data_group.is_metadata())
            .ok_or_else(|| read_error(FailureReason::WrongGroup, "data group not hosted"))?;
        let vote = group.raft.metrics().borrow().vote;
        let required = authority_barrier(data_group, group, deadline).await?;
        let (applied, membership) = group
            .reader
            .read_membership()
            .await
            .map_err(|error| read_error(FailureReason::Internal, error))?;
        check_read_deadline(deadline)?;
        check_applied_covers(applied, required)?;
        check_applied_covers(applied, *membership.log_id())?;
        let metrics = group.raft.metrics();
        let current = metrics.borrow();
        if current.vote != vote
            || current.current_leader != Some(self.identity.node_id.get())
            || !vote.committed
        {
            return Err(read_error(
                FailureReason::NotLeader,
                "authority changed during membership observation",
            ));
        }
        let configs = membership.membership().get_joint_config();
        if configs.is_empty() || configs.len() > 2 {
            return Err(read_error(
                FailureReason::InvalidRequest,
                "unsupported voter topology",
            ));
        }
        let voters = voter_triple(&configs[0])?;
        let joint_voters = configs.get(1).map(voter_triple).transpose()?;
        let mut peers = [None; 16];
        for (index, (id, node)) in membership.nodes().enumerate() {
            let slot = peers.get_mut(index).ok_or_else(|| {
                read_error(FailureReason::InvalidRequest, "too many membership nodes")
            })?;
            *slot = Some((
                NodeId::new(*id).map_err(|error| read_error(FailureReason::Internal, error))?,
                node.addr
                    .parse()
                    .map_err(|error| read_error(FailureReason::Internal, error))?,
            ));
        }
        let result = AppliedMembership {
            data_group,
            applied: position(applied.ok_or_else(|| {
                read_error(FailureReason::Unavailable, "group has no applied identity")
            })?)?,
            membership: position(membership.log_id().ok_or_else(|| {
                read_error(
                    FailureReason::Unavailable,
                    "group has no applied membership",
                )
            })?)?,
            voters,
            joint_voters,
            peers,
        };
        result
            .validate()
            .map_err(|error| read_error(FailureReason::Internal, error))?;
        Ok(result)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Topology {
    Source,
    Joint,
    Target,
}

pub(super) fn topology(
    observed: &AppliedMembership,
    intent: &MoveIntent,
) -> Result<Topology, ReadFailure> {
    observed
        .validate()
        .map_err(|error| read_error(FailureReason::InvalidRequest, error))?;
    if observed.data_group != intent.data_group {
        return Err(read_error(
            FailureReason::InvalidRequest,
            "membership observation belongs to another group",
        ));
    }
    if let Some(peer) = observed.peer(intent.add_node.node_id)
        && peer != intent.add_node.peer_address
    {
        return Err(read_error(
            FailureReason::InvalidRequest,
            "replacement endpoint conflicts",
        ));
    }
    match (observed.voters, observed.joint_voters) {
        (voters, None) if voters == intent.old_voters => Ok(Topology::Source),
        (voters, None) if voters == intent.target_voters => Ok(Topology::Target),
        (old, Some(target)) if old == intent.old_voters && target == intent.target_voters => {
            Ok(Topology::Joint)
        }
        _ => Err(read_error(
            FailureReason::InvalidRequest,
            "actual membership conflicts with lifetime move",
        )),
    }
}

pub(super) fn require_target(
    observed: &AppliedMembership,
    intent: &MoveIntent,
) -> Result<(), ReadFailure> {
    if topology(observed, intent)? != Topology::Target
        || observed.peer(intent.add_node.node_id) != Some(intent.add_node.peer_address)
    {
        return Err(read_error(
            FailureReason::InvalidRequest,
            "fresh applied membership is not uniform target",
        ));
    }
    Ok(())
}

pub(super) fn covers(applied: Option<LogId>, required: Option<LogId>) -> bool {
    check_applied_covers(applied, required).is_ok()
}

pub(super) fn position(log_id: LogId) -> Result<ObservedLogPosition, ReadFailure> {
    let position = ObservedLogPosition {
        term: log_id.leader_id.term,
        leader_node_id: log_id.leader_id.node_id,
        index: log_id.index,
    };
    position
        .validate()
        .map_err(|error| read_error(FailureReason::Internal, error))?;
    Ok(position)
}

pub(super) fn completion_position(
    position: ObservedLogPosition,
) -> Result<CommittedLogPosition, ReadFailure> {
    position
        .validate()
        .map_err(|error| read_error(FailureReason::Internal, error))?;
    Ok(CommittedLogPosition {
        term: position.term,
        leader_node_id: NodeId::new(position.leader_node_id)
            .map_err(|error| read_error(FailureReason::InvalidRequest, error))?,
        index: position.index,
    })
}

pub(super) fn voter_triple(voters: &BTreeSet<u64>) -> Result<[NodeId; 3], ReadFailure> {
    let ids: Vec<_> = voters
        .iter()
        .copied()
        .map(NodeId::new)
        .collect::<Result<_, _>>()
        .map_err(|error| read_error(FailureReason::Internal, error))?;
    ids.try_into().map_err(|_| {
        read_error(
            FailureReason::InvalidRequest,
            "finite move requires exactly three voters",
        )
    })
}

pub(super) fn check_promotion_authority(
    expected: openraft::Vote<u64>,
    current: openraft::Vote<u64>,
    leader: Option<u64>,
    local: u64,
) -> Result<(), ReadFailure> {
    if !expected.committed || current != expected || leader != Some(local) {
        return Err(read_error(
            FailureReason::NotLeader,
            "captured catch-up authority changed before promotion",
        ));
    }
    Ok(())
}
