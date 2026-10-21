//! Receiver-owned handoff primitives; no successor coordinator or activation.
use super::{observation::position, *};
use crate::control::MetadataView;
use rockbee_raft_protocol::{
    backend::{HandoffFenceReceipt, PreparedMoveState, SealReceipt},
    routing::{ClusterDirectory, NodeEndpoint},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct HandoffIdentity {
    pub(super) data_group: GroupId,
    pub(super) predecessor_revision: u64,
    pub(super) predecessor_job_id: MoveJobId,
    pub(super) successor_job_id: MoveJobId,
}

impl NodeService {
    /// Authority and the complete projection come from one coherent G0 read.
    /// This read deliberately does not acquire publication admission recursively.
    pub(crate) async fn read_prepared_move(
        &self,
        data_group: GroupId,
        deadline: Instant,
    ) -> Result<Option<Box<PreparedMoveState>>, ReadFailure> {
        self.movement.check_running(deadline)?;
        if data_group.is_metadata() || data_group.get() > u64::from(self.shard_config.count()) {
            return Err(read_error(
                FailureReason::WrongGroup,
                "unconfigured data group",
            ));
        }
        let origin = self.bootstrap_origin.as_ref().ok_or_else(|| {
            read_error(
                FailureReason::NotLeader,
                "join nodes have no metadata authority",
            )
        })?;
        let group = self
            .groups
            .get(&GroupId::new(0))
            .ok_or_else(|| read_error(FailureReason::Unavailable, "metadata group not hosted"))?;
        let required = authority_barrier(GroupId::new(0), group, deadline).await?;
        let read = group
            .reader
            .read_metadata()
            .await
            .map_err(|error| read_error(FailureReason::Internal, error))?;
        check_read_deadline(deadline)?;
        check_applied_covers(read.applied, required)?;
        prepared_from_view(read.view, origin, data_group)
    }

    pub(crate) async fn seal_predecessor(
        &self,
        data_group: GroupId,
        predecessor_revision: u64,
        predecessor_job_id: MoveJobId,
        successor_job_id: MoveJobId,
        deadline: Instant,
    ) -> Result<SealReceipt, ReadFailure> {
        self.movement.check_running(deadline)?;
        let _admission = Arc::clone(&self.movement.publication)
            .try_acquire_owned()
            .map_err(|_| {
                read_error(
                    FailureReason::Unavailable,
                    "handoff control admission is full or closed",
                )
            })?;
        let identity = HandoffIdentity {
            data_group,
            predecessor_revision,
            predecessor_job_id,
            successor_job_id,
        };
        let state = self.fetch_prepared_move(data_group, deadline).await?;
        let endpoint = prepared_executor(&state, identity, self.identity.node_id)?;
        let group = self
            .groups
            .get(&data_group)
            .ok_or_else(|| read_error(FailureReason::WrongGroup, "data group not hosted"))?;
        self.movement.check_running(deadline)?;
        info!(
            "raft_handoff_seal_admitted node={} group={} predecessor_revision={} predecessor={:?} successor={:?}",
            self.identity.node_id.get(),
            data_group.get(),
            predecessor_revision,
            predecessor_job_id.get(),
            successor_job_id.get()
        );
        #[cfg(test)]
        let observation = self
            .movement
            .handoff_seal
            .lock()
            .expect("seal observation slot poisoned")
            .as_ref()
            .and_then(|observer| observer.enter(identity));
        // Accepted server handlers own this await even after caller EOF/deadline.
        // Seal waits the whole native membership owner, then sync-persists.
        let result = group
            .reader
            .membership_control()
            .seal(crate::storage::MovementIdentity {
                revision: predecessor_revision,
                job_id: predecessor_job_id,
            })
            .await;
        #[cfg(test)]
        if let Some(observer) = observation {
            observer.finish(&result);
        }
        if let Err(error) = result {
            warn!(
                "raft_handoff_seal_failed node={} group={} predecessor_revision={} predecessor={:?} successor={:?} error={error}",
                self.identity.node_id.get(),
                data_group.get(),
                predecessor_revision,
                predecessor_job_id.get(),
                successor_job_id.get()
            );
            return Err(read_error(FailureReason::Unavailable, error));
        }
        info!(
            "raft_handoff_seal_durable node={} group={} predecessor_revision={} predecessor={:?} successor={:?}",
            self.identity.node_id.get(),
            data_group.get(),
            predecessor_revision,
            predecessor_job_id.get(),
            successor_job_id.get()
        );
        Ok(SealReceipt {
            endpoint,
            data_group,
            predecessor_revision,
            predecessor_job_id,
        })
    }

    pub(crate) async fn commit_handoff_fence(
        &self,
        data_group: GroupId,
        predecessor_revision: u64,
        predecessor_job_id: MoveJobId,
        successor_job_id: MoveJobId,
        deadline: Instant,
    ) -> Result<HandoffFenceReceipt, ReadFailure> {
        self.movement.check_running(deadline)?;
        let _admin = Arc::clone(&self.movement.publication)
            .try_acquire_owned()
            .map_err(|_| {
                read_error(
                    FailureReason::Unavailable,
                    "handoff control admission is full or closed",
                )
            })?;
        let identity = HandoffIdentity {
            data_group,
            predecessor_revision,
            predecessor_job_id,
            successor_job_id,
        };
        let state = self.fetch_prepared_move(data_group, deadline).await?;
        prepared_executor(&state, identity, self.identity.node_id)?;
        let group = self
            .groups
            .get(&data_group)
            .ok_or_else(|| read_error(FailureReason::WrongGroup, "data group not hosted"))?;
        self.movement.check_running(deadline)?;
        if self.leader_hint(data_group) != Some(self.identity.node_id) {
            return Err(read_error(
                FailureReason::NotLeader,
                "not the observed data leader",
            ));
        }
        let _native = group.mutation_admission.try_acquire().map_err(|error| {
            let reason = match error {
                tokio::sync::TryAcquireError::NoPermits => FailureReason::Overloaded,
                tokio::sync::TryAcquireError::Closed => FailureReason::Unavailable,
            };
            read_error(reason, "group native mutation admission unavailable")
        })?;
        // This is an internal log write, not a public KV proposal. Do not call
        // mutate() or increment its public-KV observations. No timeout may drop
        // the native future; any invoked error leaves effects uncertain.
        info!(
            "raft_handoff_fence_submitted node={} group={} predecessor_revision={} predecessor={:?} successor={:?}",
            self.identity.node_id.get(),
            data_group.get(),
            predecessor_revision,
            predecessor_job_id.get(),
            successor_job_id.get()
        );
        let result = group
            .raft
            .client_write(Command::HandoffFence {
                predecessor_revision,
                predecessor_job_id,
                successor_job_id,
            })
            .await;
        if matches!(&result, Err(openraft::error::RaftError::Fatal(_))) {
            group.mutation_admission.close();
        }
        let response = result.map_err(|error| {
            warn!("raft_handoff_fence_uncertain node={} group={} predecessor_revision={} predecessor={:?} successor={:?} error={error}",
                self.identity.node_id.get(), data_group.get(), predecessor_revision,
                predecessor_job_id.get(), successor_job_id.get());
            read_error(FailureReason::Unavailable, error)
        })?;
        if response.data != ApplyResult::HandoffFenced {
            return Err(read_error(
                FailureReason::Internal,
                "unexpected handoff fence apply result",
            ));
        }
        let (applied, _) = group
            .reader
            .read_membership()
            .await
            .map_err(|error| read_error(FailureReason::Internal, error))?;
        check_applied_covers(applied, Some(response.log_id))?;
        let committed = completion_position(position(response.log_id)?)?;
        info!(
            "raft_handoff_fence_committed node={} group={} predecessor_revision={} predecessor={:?} successor={:?} log_id={:?}",
            self.identity.node_id.get(),
            data_group.get(),
            predecessor_revision,
            predecessor_job_id.get(),
            successor_job_id.get(),
            response.log_id
        );
        Ok(HandoffFenceReceipt {
            data_group,
            predecessor_revision,
            predecessor_job_id,
            successor_job_id,
            committed,
        })
    }

    async fn fetch_prepared_move(
        &self,
        data_group: GroupId,
        deadline: Instant,
    ) -> Result<Box<PreparedMoveState>, ReadFailure> {
        loop {
            self.movement.check_running(deadline)?;
            if let Some(BackendReply::PreparedMoveState {
                result: Ok(state), ..
            }) = self
                .metadata_request(BackendOperation::GetPreparedMove { data_group }, deadline)
                .await
            {
                let state = state.ok_or_else(|| {
                    read_error(FailureReason::InvalidRequest, "successor is not preparing")
                })?;
                if state.directory.cluster_id != self.identity.cluster_id
                    || state.directory.shard_config != self.shard_config
                {
                    return Err(read_error(
                        FailureReason::InvalidRequest,
                        "metadata cluster/hash identity mismatch",
                    ));
                }
                return Ok(state);
            }
            sleep_until(deadline.min(Instant::now() + Duration::from_millis(25))).await;
        }
    }
}

/// Projects only Preparing from one already-coherent metadata read.
pub(super) fn prepared_from_view(
    view: MetadataView,
    origin: &ClusterDirectory,
    data_group: GroupId,
) -> Result<Option<Box<PreparedMoveState>>, ReadFailure> {
    let preparing = view.prepared_successor(data_group).cloned();
    let first = view.first_job(data_group).cloned();
    let directory = compatible_directory(view, origin)?
        .ok_or_else(|| read_error(FailureReason::Unavailable, "directory not initialized"))?;
    let Some(preparing) = preparing else {
        return Ok(None);
    };
    let first = first.ok_or_else(|| {
        read_error(
            FailureReason::Internal,
            "preparing successor has no predecessor",
        )
    })?;
    let state = PreparedMoveState {
        directory,
        first,
        preparing,
    };
    state
        .validate()
        .map_err(|error| read_error(FailureReason::Internal, error))?;
    Ok(Some(Box::new(state)))
}

/// Binds the request and local executor to the authoritative frozen record.
pub(super) fn prepared_executor(
    state: &PreparedMoveState,
    identity: HandoffIdentity,
    local: NodeId,
) -> Result<NodeEndpoint, ReadFailure> {
    state
        .validate()
        .map_err(|error| read_error(FailureReason::InvalidRequest, error))?;
    let first = &state.first.intent;
    if first.data_group != identity.data_group
        || first.expected_directory_revision != identity.predecessor_revision
        || first.job_id != identity.predecessor_job_id
        || state.preparing.intent.job_id != identity.successor_job_id
    {
        return Err(read_error(
            FailureReason::InvalidRequest,
            "handoff identity differs from authoritative preparation",
        ));
    }
    state
        .preparing
        .executors
        .iter()
        .find(|endpoint| endpoint.node_id == local)
        .cloned()
        .ok_or_else(|| {
            read_error(
                FailureReason::InvalidRequest,
                "receiver is outside predecessor executor census",
            )
        })
}
