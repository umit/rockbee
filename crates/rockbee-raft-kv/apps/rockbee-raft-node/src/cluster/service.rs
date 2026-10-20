//! Backend operations translate to explicit authority checks and Raft calls.
use std::{collections::BTreeMap, num::NonZeroU64, sync::Arc, time::Duration};

#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use log::{debug, trace, warn};
use openraft::{
    Raft,
    error::{CheckIsLeaderError, RaftError},
};
use rockbee_raft_protocol::{
    Failure, FailureReason, GroupId, KvRequest, KvResponse, MutationOutcome, NodeId, ReadFailure,
    ShardConfig, WireRequest,
    backend::{BackendOperation, BackendReply, BackendRequest, BackendResponse, BootstrapOutcome},
    routing::ClusterDirectory,
    transport::server::RequestHandler,
};
use tokio::{
    sync::{Semaphore, TryAcquireError},
    time::{Instant, sleep_until},
};

use crate::{
    config::validate_metadata_candidate,
    control::{MetadataCommand, MetadataResult, MetadataView},
    rpc::{PeerIdentity, PeerLimits},
    storage::{StateReader, StorageOptions},
    types::{ApplyResult, Command, LogId, TypeConfig},
};

#[derive(Clone)]
pub(super) struct HostedGroup {
    pub raft: Raft<TypeConfig>,
    pub reader: StateReader,
    /// Held by actual native KV calls, including after their callers disappear.
    pub mutation_admission: Arc<Semaphore>,
}

/// Test-only actual Raft proposal observations for one hosted group.
#[cfg(all(test, feature = "diagnostics"))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ProposalSnapshot {
    pub(crate) entered: usize,
    pub(crate) completed: usize,
    pub(crate) active: usize,
    pub(crate) dropped_before_completion: usize,
}

/// Captured synchronously on a real Fatal return while its permit is owned.
#[cfg(all(test, feature = "diagnostics"))]
#[derive(Clone, Debug)]
pub(crate) struct FatalAdmissionSnapshot {
    pub(crate) closed: bool,
    pub(crate) available: usize,
    pub(crate) native_active: usize,
    pub(crate) error: String,
}

#[cfg(all(test, feature = "diagnostics"))]
struct ProposalCounters {
    entered: AtomicUsize,
    completed: AtomicUsize,
    active: AtomicUsize,
    dropped_before_completion: AtomicUsize,
    fatal: std::sync::Mutex<Option<FatalAdmissionSnapshot>>,
}

#[cfg(all(test, feature = "diagnostics"))]
impl ProposalCounters {
    fn snapshot(&self) -> ProposalSnapshot {
        ProposalSnapshot {
            entered: self.entered.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            active: self.active.load(Ordering::Relaxed),
            dropped_before_completion: self.dropped_before_completion.load(Ordering::Relaxed),
        }
    }
}

#[cfg(all(test, feature = "diagnostics"))]
struct ProposalGuard<'a> {
    counters: &'a ProposalCounters,
    completed: bool,
}

#[cfg(all(test, feature = "diagnostics"))]
impl ProposalGuard<'_> {
    fn complete(&mut self) {
        self.counters.completed.fetch_add(1, Ordering::Relaxed);
        self.completed = true;
    }
}

#[cfg(all(test, feature = "diagnostics"))]
impl Drop for ProposalGuard<'_> {
    fn drop(&mut self) {
        self.counters.active.fetch_sub(1, Ordering::Relaxed);
        if !self.completed {
            self.counters
                .dropped_before_completion
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// The node's application service; peer RPC and public client requests stay separate.
#[derive(Clone)]
pub struct NodeService {
    pub(super) identity: PeerIdentity,
    pub(super) shard_config: ShardConfig,
    pub(super) bootstrap_origin: Option<ClusterDirectory>,
    pub(super) storage: StorageOptions,
    pub(super) peers: PeerLimits,
    pub(super) groups: BTreeMap<GroupId, HostedGroup>,
    pub(super) metadata_seeds: Vec<crate::config::JoinSeed>,
    pub(super) movement: Arc<super::movement::MoveRunners>,
    /// Test-only observation of public KV proposal admission. Metadata bootstrap
    /// deliberately bypasses this scalar so a response-loss fixture can count
    /// the one actual user mutation across the three replicas.
    #[cfg(test)]
    client_write_calls: Arc<AtomicUsize>,
    /// Fixed to the finite hosted-group map; never keyed by request or value.
    #[cfg(all(test, feature = "diagnostics"))]
    proposals: Arc<BTreeMap<GroupId, ProposalCounters>>,
}

impl NodeService {
    pub(super) fn new(
        identity: PeerIdentity,
        shard_config: ShardConfig,
        bootstrap_origin: Option<ClusterDirectory>,
        storage: StorageOptions,
        peers: PeerLimits,
        groups: BTreeMap<GroupId, HostedGroup>,
        metadata_seeds: Vec<crate::config::JoinSeed>,
    ) -> Self {
        #[cfg(all(test, feature = "diagnostics"))]
        let proposals = Arc::new(
            groups
                .keys()
                .copied()
                .map(|id| {
                    (
                        id,
                        ProposalCounters {
                            entered: AtomicUsize::new(0),
                            completed: AtomicUsize::new(0),
                            active: AtomicUsize::new(0),
                            dropped_before_completion: AtomicUsize::new(0),
                            fatal: std::sync::Mutex::new(None),
                        },
                    )
                })
                .collect(),
        );
        Self {
            identity,
            shard_config,
            bootstrap_origin,
            storage,
            peers,
            groups,
            metadata_seeds,
            movement: Arc::new(super::movement::MoveRunners::new()),
            #[cfg(test)]
            client_write_calls: Arc::new(AtomicUsize::new(0)),
            #[cfg(all(test, feature = "diagnostics"))]
            proposals,
        }
    }

    /// Returns only actual KV `client_write` entries for a fixture-owned node.
    #[cfg(test)]
    pub(crate) fn test_client_write_calls(&self) -> usize {
        self.client_write_calls.load(Ordering::Relaxed)
    }

    /// Returns actual `client_write` entry/completion state for one hosted group.
    #[cfg(all(test, feature = "diagnostics"))]
    pub(crate) fn test_proposals(&self, group: GroupId) -> ProposalSnapshot {
        self.proposals
            .get(&group)
            .map(ProposalCounters::snapshot)
            .expect("test proposal group must be hosted")
    }

    #[cfg(all(test, feature = "diagnostics"))]
    pub(crate) fn test_fatal_admission(&self, group: GroupId) -> Option<FatalAdmissionSnapshot> {
        self.proposals[&group]
            .fatal
            .lock()
            .expect("fatal observation lock poisoned")
            .clone()
    }

    pub(super) fn leader_hint(&self, group: GroupId) -> Option<NodeId> {
        let id = self
            .groups
            .get(&group)?
            .raft
            .metrics()
            .borrow()
            .current_leader?;
        NodeId::new(id).ok()
    }

    async fn execute(&self, request: BackendRequest, deadline: Instant) -> BackendReply {
        let failure = if request.cluster_id != self.identity.cluster_id {
            Some(failure(FailureReason::WrongCluster, "wrong cluster"))
        } else if request.target_node != self.identity.node_id {
            Some(failure(FailureReason::InvalidRequest, "wrong target node"))
        } else if let Err(error) = request.validate() {
            Some(failure(FailureReason::InvalidRequest, error))
        } else if deadline <= Instant::now() {
            Some(failure(
                FailureReason::Deadline,
                "request expired before service submission",
            ))
        } else {
            None
        };
        if let Some(failure) = failure {
            return rejection(&request.operation, failure);
        }
        match request.operation {
            BackendOperation::MoveReplica(request) => {
                let data_group = request.data_group;
                let job_id = request.job_id;
                BackendReply::Move {
                    data_group,
                    job_id,
                    result: self.accept_move(request, deadline).await,
                }
            }
            BackendOperation::GetMove { data_group } => BackendReply::MoveState {
                data_group,
                result: self.read_move(data_group, deadline).await.map(Box::new),
            },
            BackendOperation::ReconcileMove { data_group, job_id } => BackendReply::Reconcile {
                data_group,
                job_id,
                result: self.schedule_move(data_group, job_id).await,
            },
            BackendOperation::ObserveMembership { data_group } => BackendReply::Membership {
                data_group,
                result: self
                    .observe_membership(data_group, deadline)
                    .await
                    .map(Box::new),
            },
            BackendOperation::PublishMove { data_group, job_id } => BackendReply::Move {
                data_group,
                job_id,
                result: self.publish_move(data_group, job_id, deadline).await,
            },
            BackendOperation::Directory => {
                BackendReply::Directory(self.read_directory(deadline).await)
            }
            BackendOperation::BootstrapDirectory => {
                BackendReply::Bootstrap(self.bootstrap_directory(deadline).await)
            }
            BackendOperation::Leader { group_id } => BackendReply::Leader {
                group_id,
                leader: self.leader_hint(group_id),
            },
            BackendOperation::Kv { group_id, request } => {
                let response = self.execute_kv(group_id, request, deadline).await;
                BackendReply::Kv {
                    response,
                    leader_hint: self.leader_hint(group_id),
                }
            }
        }
    }

    async fn read_directory(&self, deadline: Instant) -> Result<ClusterDirectory, ReadFailure> {
        let origin = self.bootstrap_origin.as_ref().ok_or_else(|| {
            ReadFailure(failure(
                FailureReason::Unavailable,
                "join nodes do not host metadata authority",
            ))
        })?;
        let group = self.groups.get(&GroupId::new(0)).ok_or_else(|| {
            ReadFailure(failure(
                FailureReason::Unavailable,
                "metadata group not hosted",
            ))
        })?;
        let required = authority_barrier(GroupId::new(0), group, deadline).await?;
        let read = group
            .reader
            .read_metadata()
            .await
            .map_err(|error| ReadFailure(failure(FailureReason::Internal, error)))?;
        check_read_deadline(deadline)?;
        check_applied_covers(read.applied, required)?;
        compatible_directory(read.view, origin)?.ok_or_else(|| {
            ReadFailure(failure(
                FailureReason::Unavailable,
                "directory is not initialized",
            ))
        })
    }

    async fn bootstrap_directory(&self, deadline: Instant) -> BootstrapOutcome {
        let Some(origin) = &self.bootstrap_origin else {
            return BootstrapOutcome::NotSubmitted(failure(
                FailureReason::Unavailable,
                "join nodes do not host metadata authority",
            ));
        };
        let id = GroupId::new(0);
        let Some(group) = self.groups.get(&id) else {
            return BootstrapOutcome::NotSubmitted(failure(
                FailureReason::Unavailable,
                "metadata group not hosted",
            ));
        };
        let command = MetadataCommand::InitializeDirectoryIfAbsent {
            initial_directory: origin.clone(),
        };
        self.submit_bootstrap_initialize(group, command, deadline)
            .await
    }

    async fn submit_bootstrap_initialize(
        &self,
        group: &HostedGroup,
        command: MetadataCommand,
        deadline: Instant,
    ) -> BootstrapOutcome {
        if let Err(error) = validate_metadata_candidate(&command, &self.storage, self.peers) {
            return BootstrapOutcome::NotSubmitted(failure(FailureReason::Internal, error));
        }
        if deadline <= Instant::now() {
            return BootstrapOutcome::NotSubmitted(failure(
                FailureReason::Deadline,
                "expired before conditional initialization submission",
            ));
        }
        if self.leader_hint(GroupId::new(0)) != Some(self.identity.node_id) {
            return BootstrapOutcome::NotSubmitted(failure(
                FailureReason::NotLeader,
                "not the observed metadata leader before submission",
            ));
        }
        // Keep the accepted Raft work owned until resolution, including after
        // transport loss or deadline expiry. A conditional conflict is applied
        // work, never a pre-submission rejection or a successful initialization.
        let result = group
            .raft
            .client_write(Command::Metadata(Box::new(command)))
            .await;
        bootstrap_submission_result(result.map(|response| response.data))
    }

    async fn execute_kv(
        &self,
        group_id: GroupId,
        request: KvRequest,
        deadline: Instant,
    ) -> KvResponse {
        let operation = match &request {
            KvRequest::Get { .. } => "get",
            KvRequest::Put { .. } => "put",
            KvRequest::Delete { .. } => "delete",
        };
        trace!(
            "raft_kv_request node={} group={} op={}",
            self.identity.node_id.get(),
            group_id.get(),
            operation,
        );
        let key = match &request {
            KvRequest::Put { key, .. } | KvRequest::Get { key } | KvRequest::Delete { key } => key,
        };
        if self.shard_config.group_for_key(key).ok() != Some(group_id) {
            return kv_rejection(
                &request,
                failure(
                    FailureReason::WrongGroup,
                    "key does not belong to requested data group",
                ),
            );
        }
        let Some(group) = self.groups.get(&group_id) else {
            return kv_rejection(
                &request,
                failure(FailureReason::WrongGroup, "group not hosted"),
            );
        };
        match request {
            KvRequest::Get { key } => match strong_read(group_id, group, key, deadline).await {
                Ok(value) => KvResponse::Value(value),
                Err(error) => KvResponse::ReadError(error),
            },
            KvRequest::Put { key, value } => KvResponse::Mutation(
                self.mutate(group_id, group, Command::Put { key, value }, deadline)
                    .await,
            ),
            KvRequest::Delete { key } => KvResponse::Mutation(
                self.mutate(group_id, group, Command::Delete { key }, deadline)
                    .await,
            ),
        }
    }

    async fn mutate(
        &self,
        id: GroupId,
        group: &HostedGroup,
        command: Command,
        deadline: Instant,
    ) -> MutationOutcome {
        if deadline <= Instant::now() {
            debug!(
                "raft_mutation_expired node={} group={}",
                self.identity.node_id.get(),
                id.get()
            );
            return MutationOutcome::Rejected(failure(
                FailureReason::Deadline,
                "expired before Raft submission",
            ));
        }
        if self.leader_hint(id) != Some(self.identity.node_id) {
            debug!(
                "raft_mutation_not_leader node={} group={}",
                self.identity.node_id.get(),
                id.get()
            );
            return MutationOutcome::Rejected(failure(
                FailureReason::NotLeader,
                "not the observed leader before submission",
            ));
        }
        let _admission = match group.mutation_admission.try_acquire() {
            Ok(permit) => permit,
            Err(TryAcquireError::NoPermits) => {
                return MutationOutcome::Rejected(failure(
                    FailureReason::Overloaded,
                    "group native mutation capacity is full",
                ));
            }
            Err(TryAcquireError::Closed) => {
                return MutationOutcome::Rejected(failure(
                    FailureReason::Unavailable,
                    "group native mutation admission is closed",
                ));
            }
        };
        // After invoking client_write, even ForwardToLeader can refer to a
        // previously appended proposal. No error here proves safe rejection.
        // Do not cancel this future on the caller's timeout: the server's global
        // request permit remains held until the actual Raft call resolves.
        // Count after every local rejection guard but immediately before the
        // real public KV submission. This is not a Raft-log-entry count and
        // does not include metadata initialization.
        #[cfg(test)]
        self.client_write_calls.fetch_add(1, Ordering::Relaxed);
        #[cfg(all(test, feature = "diagnostics"))]
        let mut proposal = self.enter_proposal(id);
        let result = group.raft.client_write(command).await;
        // Seal the group before this call's permit can be recycled. An invoked
        // proposal still has Unknown certainty on error, including Fatal.
        if matches!(&result, Err(RaftError::Fatal(_))) {
            group.mutation_admission.close();
            #[cfg(all(test, feature = "diagnostics"))]
            self.record_fatal_admission(id, group, format!("{result:?}"));
        }
        #[cfg(all(test, feature = "diagnostics"))]
        proposal.complete();
        match result {
            Ok(_) => {
                trace!(
                    "raft_mutation_committed node={} group={}",
                    self.identity.node_id.get(),
                    id.get()
                );
                MutationOutcome::Committed
            }
            Err(error) => {
                warn!(
                    "raft_mutation_unknown node={} group={} error={}",
                    self.identity.node_id.get(),
                    id.get(),
                    error
                );
                MutationOutcome::Unknown(failure(FailureReason::Unavailable, error))
            }
        }
    }

    #[cfg(all(test, feature = "diagnostics"))]
    fn record_fatal_admission(&self, id: GroupId, group: &HostedGroup, error: String) {
        let counters = &self.proposals[&id];
        counters
            .fatal
            .lock()
            .expect("fatal observation lock poisoned")
            .get_or_insert_with(|| FatalAdmissionSnapshot {
                closed: group.mutation_admission.is_closed(),
                available: group.mutation_admission.available_permits(),
                native_active: counters.active.load(Ordering::Relaxed),
                error,
            });
    }

    #[cfg(all(test, feature = "diagnostics"))]
    fn enter_proposal(&self, group: GroupId) -> ProposalGuard<'_> {
        let counters = self
            .proposals
            .get(&group)
            .expect("actual proposal group must be hosted");
        counters.entered.fetch_add(1, Ordering::Relaxed);
        counters.active.fetch_add(1, Ordering::Relaxed);
        ProposalGuard {
            counters,
            completed: false,
        }
    }
}

impl RequestHandler for NodeService {
    type Request = BackendRequest;
    type Response = BackendResponse;
    async fn handle(&self, request: BackendRequest, deadline: Instant) -> BackendResponse {
        BackendResponse {
            cluster_id: self.identity.cluster_id,
            node_id: self.identity.node_id,
            reply: self.execute(request, deadline).await,
        }
    }
}

async fn strong_read(
    id: GroupId,
    group: &HostedGroup,
    key: Bytes,
    deadline: Instant,
) -> Result<Option<Bytes>, ReadFailure> {
    let required = authority_barrier(id, group, deadline).await?;
    let view = group
        .reader
        .read_local(key)
        .await
        .map_err(|error| ReadFailure(failure(FailureReason::Internal, error)))?;
    check_read_deadline(deadline)?;
    check_applied_covers(view.applied, required)?;
    Ok(view.value)
}

pub(super) async fn authority_barrier(
    id: GroupId,
    group: &HostedGroup,
    deadline: Instant,
) -> Result<Option<LogId>, ReadFailure> {
    let mut attempt = 0_u32;
    let required = loop {
        check_read_deadline(deadline)?;
        attempt = attempt.saturating_add(1);
        let started = log::log_enabled!(log::Level::Debug).then(Instant::now);
        trace!(
            "raft_read_barrier_start group={} attempt={attempt}",
            id.get()
        );
        // A single quorum probe uses OpenRaft's heartbeat-sized RPC budget.
        // A temporary missed probe need not consume the whole application read.
        // Keep the submitted call owned until it resolves, even if the caller
        // disconnects; the server still holds its accepted-operation permit.
        match group.raft.ensure_linearizable().await {
            Ok(required) => {
                check_read_deadline(deadline)?;
                if attempt > 1 {
                    debug!(
                        "raft_read_barrier_recovered group={} attempts={attempt} attempt_elapsed_us={} remaining_us={}",
                        id.get(),
                        started.map_or(0, |time| time.elapsed().as_micros()),
                        deadline
                            .saturating_duration_since(Instant::now())
                            .as_micros()
                    );
                }
                break required;
            }
            Err(RaftError::APIError(CheckIsLeaderError::QuorumNotEnough(_))) => {
                debug!(
                    "raft_read_barrier_retry group={} attempt={attempt} reason=quorum_not_enough attempt_elapsed_us={} remaining_us={}",
                    id.get(),
                    started.map_or(0, |time| time.elapsed().as_micros()),
                    deadline
                        .saturating_duration_since(Instant::now())
                        .as_micros()
                );
                check_read_deadline(deadline)?;
                let retry_delay = Duration::from_millis(group.raft.config().heartbeat_interval);
                sleep_until(deadline.min(Instant::now() + retry_delay)).await;
            }
            Err(error) => {
                let reason = if error.forward_to_leader().is_some() {
                    FailureReason::NotLeader
                } else {
                    FailureReason::Unavailable
                };
                debug!(
                    "raft_read_barrier_failed group={} attempt={attempt} reason={reason:?} error={error}",
                    id.get()
                );
                return Err(ReadFailure(failure(reason, error)));
            }
        }
    };
    Ok(required)
}

pub(super) fn check_applied_covers(
    applied: Option<LogId>,
    required: Option<LogId>,
) -> Result<(), ReadFailure> {
    let covers = match (applied, required) {
        (_, None) => true,
        (Some(applied), Some(required)) => {
            (applied.index > required.index && applied.leader_id >= required.leader_id)
                || applied == required
        }
        (None, Some(_)) => false,
    };
    if !covers {
        return Err(ReadFailure(failure(
            FailureReason::Internal,
            "state view does not cover the read barrier",
        )));
    }
    Ok(())
}

pub(super) fn compatible_directory(
    view: MetadataView,
    configured_origin: &ClusterDirectory,
) -> Result<Option<ClusterDirectory>, ReadFailure> {
    let compatible = match view.origin() {
        Some(origin) => origin == configured_origin,
        // The validated loader permits origin-less legacy state only at
        // canonical revision one without jobs. Initialization must still adopt it.
        None => view
            .directory()
            .is_none_or(|current| current == configured_origin),
    };
    if !compatible {
        return Err(ReadFailure(failure(
            FailureReason::Internal,
            "stored immutable origin differs from configured bootstrap",
        )));
    }
    Ok(view.into_directory())
}

fn bootstrap_apply_result(result: ApplyResult) -> BootstrapOutcome {
    match result {
        ApplyResult::Metadata(
            MetadataResult::Initialized { revision }
            | MetadataResult::AlreadyInitialized { revision },
        ) => match NonZeroU64::new(revision) {
            Some(revision) => BootstrapOutcome::Ready { revision },
            None => BootstrapOutcome::Unknown(failure(
                FailureReason::Internal,
                "initialization returned zero revision",
            )),
        },
        ApplyResult::Metadata(MetadataResult::Conflict { reason }) => {
            BootstrapOutcome::Conflict(failure(
                FailureReason::InvalidRequest,
                format!("conditional initialization conflict: {reason:?}"),
            ))
        }
        _ => BootstrapOutcome::Unknown(failure(
            FailureReason::Internal,
            "unexpected conditional initialization apply result",
        )),
    }
}

fn bootstrap_submission_result(
    result: Result<ApplyResult, impl std::fmt::Display>,
) -> BootstrapOutcome {
    // The Link deadline controls delivery. A known durable result retains its
    // certainty even if the owned client_write operation resolved late.
    match result {
        Ok(result) => bootstrap_apply_result(result),
        Err(error) => BootstrapOutcome::Unknown(failure(FailureReason::Unavailable, error)),
    }
}

pub(super) fn check_read_deadline(deadline: Instant) -> Result<(), ReadFailure> {
    if deadline <= Instant::now() {
        return Err(ReadFailure(failure(
            FailureReason::Deadline,
            "read deadline expired",
        )));
    }
    Ok(())
}

fn rejection(operation: &BackendOperation, error: Failure) -> BackendReply {
    match operation {
        BackendOperation::MoveReplica(request) => BackendReply::Move {
            data_group: request.data_group,
            job_id: request.job_id,
            result: rockbee_raft_protocol::control::AdminResult::Unknown {
                message: error.message,
            },
        },
        BackendOperation::GetMove { data_group } => BackendReply::MoveState {
            data_group: *data_group,
            result: Err(ReadFailure(error)),
        },
        BackendOperation::ObserveMembership { data_group } => BackendReply::Membership {
            data_group: *data_group,
            result: Err(ReadFailure(error)),
        },
        BackendOperation::ReconcileMove { data_group, job_id } => BackendReply::Reconcile {
            data_group: *data_group,
            job_id: *job_id,
            result: Err(ReadFailure(error)),
        },
        BackendOperation::PublishMove { data_group, job_id } => BackendReply::Move {
            data_group: *data_group,
            job_id: *job_id,
            result: rockbee_raft_protocol::control::AdminResult::Unknown {
                message: error.message,
            },
        },
        BackendOperation::Directory => BackendReply::Directory(Err(ReadFailure(error))),
        BackendOperation::BootstrapDirectory => {
            BackendReply::Bootstrap(BootstrapOutcome::NotSubmitted(error))
        }
        BackendOperation::Leader { group_id } => BackendReply::Leader {
            group_id: *group_id,
            leader: None,
        },
        BackendOperation::Kv { request, .. } => BackendReply::Kv {
            response: kv_rejection(request, error),
            leader_hint: None,
        },
    }
}

fn kv_rejection(request: &KvRequest, error: Failure) -> KvResponse {
    match request {
        KvRequest::Get { .. } => KvResponse::ReadError(ReadFailure(error)),
        KvRequest::Put { .. } | KvRequest::Delete { .. } => {
            KvResponse::Mutation(MutationOutcome::Rejected(error))
        }
    }
}

pub(super) fn failure(reason: FailureReason, message: impl std::fmt::Display) -> Failure {
    // At most256 Unicode scalars occupy at most1024 UTF-8 bytes.
    Failure {
        reason,
        message: message.to_string().chars().take(256).collect(),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::{
        control::{ConflictReason, DIRECTORY_KEY, MembershipObservation, ORIGIN_KEY},
        storage::{OpenMode, Storage, StorageIdentity},
        types::Entry,
    };
    use openraft::{CommittedLeaderId, EntryPayload, storage::RaftStateMachine};
    use rockbee_raft_protocol::{
        ClusterId, ShardConfig,
        control::{CommittedLogPosition, MoveJobId, MoveReplica},
        routing::NodeEndpoint,
    };

    fn log_id(term: u64, index: u64) -> LogId {
        LogId::new(CommittedLeaderId::new(term, 1), index)
    }

    fn endpoint(id: u64) -> NodeEndpoint {
        NodeEndpoint {
            node_id: NodeId::new(id).unwrap(),
            peer_address: format!("127.0.0.1:{}", 9000 + id).parse().unwrap(),
            data_address: format!("127.0.0.1:{}", 10000 + id).parse().unwrap(),
        }
    }

    fn directory() -> ClusterDirectory {
        ClusterDirectory::initial(
            ClusterId::new([27; 16]).unwrap(),
            ShardConfig::new(1, 19).unwrap(),
            (1..=3).map(endpoint).collect(),
        )
        .unwrap()
    }

    fn entry(index: u64, command: Command) -> Entry {
        Entry {
            log_id: log_id(3, index),
            payload: EntryPayload::Normal(command),
        }
    }

    fn initialize(directory: ClusterDirectory) -> Command {
        Command::Metadata(Box::new(MetadataCommand::InitializeDirectoryIfAbsent {
            initial_directory: directory,
        }))
    }

    #[test]
    fn read_coverage_checks_complete_identity_at_equal_index() {
        let required = Some(log_id(3, 8));
        assert!(check_applied_covers(required, required).is_ok());
        assert!(check_applied_covers(Some(log_id(4, 9)), required).is_ok());
        assert!(check_applied_covers(Some(log_id(4, 8)), required).is_err());
        assert!(check_applied_covers(Some(log_id(3, 7)), required).is_err());
        assert!(check_applied_covers(None, required).is_err());
        assert!(check_applied_covers(None, None).is_ok());
    }

    #[test]
    fn read_coverage_rejects_higher_index_with_older_leader() {
        assert!(
            check_applied_covers(Some(log_id(2, 9)), Some(log_id(3, 8))).is_err(),
            "a larger applied index cannot contradict committed leader history"
        );
    }

    #[test]
    fn bootstrap_known_apply_result_keeps_certainty_after_deadline() {
        let expired = Instant::now();
        assert!(expired <= Instant::now());
        for result in [
            MetadataResult::Initialized { revision: 1 },
            MetadataResult::AlreadyInitialized { revision: 2 },
        ] {
            assert!(matches!(
                bootstrap_submission_result(Ok::<_, &str>(ApplyResult::Metadata(result))),
                BootstrapOutcome::Ready { .. }
            ));
        }
        assert!(matches!(
            bootstrap_submission_result(Ok::<_, &str>(ApplyResult::Metadata(
                MetadataResult::Conflict {
                    reason: ConflictReason::BootstrapMismatch
                }
            )),),
            BootstrapOutcome::Conflict(_)
        ));
    }

    #[test]
    fn bootstrap_certainty_uses_actual_apply_result_and_submission_boundary() {
        for result in [
            MetadataResult::Initialized { revision: 1 },
            MetadataResult::AlreadyInitialized { revision: 2 },
        ] {
            assert!(matches!(
                bootstrap_apply_result(ApplyResult::Metadata(result)),
                BootstrapOutcome::Ready { .. }
            ));
        }
        assert!(matches!(
            bootstrap_apply_result(ApplyResult::Metadata(MetadataResult::Conflict {
                reason: ConflictReason::BootstrapMismatch
            })),
            BootstrapOutcome::Conflict(_)
        ));
        for result in [
            ApplyResult::Applied,
            ApplyResult::Metadata(MetadataResult::Initialized { revision: 0 }),
            ApplyResult::Metadata(MetadataResult::Accepted {
                data_group: GroupId::new(1),
                job_id: MoveJobId::new([1; 16]).unwrap(),
            }),
        ] {
            assert!(matches!(
                bootstrap_apply_result(result),
                BootstrapOutcome::Unknown(_)
            ));
        }
        let forward = openraft::error::ForwardToLeader::<u64, openraft::BasicNode>::empty();
        assert!(matches!(
            bootstrap_submission_result(Err(forward)),
            BootstrapOutcome::Unknown(_)
        ));
        assert!(matches!(
            bootstrap_submission_result(Err("storage failure")),
            BootstrapOutcome::Unknown(_)
        ));
        assert!(matches!(
            rejection(
                &BackendOperation::BootstrapDirectory,
                failure(FailureReason::Deadline, "expired")
            ),
            BackendReply::Bootstrap(BootstrapOutcome::NotSubmitted(_))
        ));
    }

    #[tokio::test]
    async fn coherent_directory_fixture_adopts_legacy_and_preserves_revision_two_on_rebootstrap() {
        // Trusted state-machine fixture: this is not live learner movement.
        let initial = directory();
        let root = tempfile::tempdir().unwrap();
        let storage = Storage::open(
            root.path().join("db"),
            StorageIdentity {
                cluster_id: initial.cluster_id,
                node_id: NodeId::new(1).unwrap(),
                shard_config: initial.shard_config,
            },
            StorageOptions::default(),
            OpenMode::CreateNew,
        )
        .await
        .unwrap();
        let mut stores = storage.open_group(GroupId::new(0)).unwrap();
        stores
            .state_machine
            .apply([entry(
                0,
                Command::Put {
                    key: Bytes::from_static(DIRECTORY_KEY),
                    value: Bytes::from(rockbee_raft_protocol::encode_frame(&initial).unwrap()),
                },
            )])
            .await
            .unwrap();
        let legacy = stores.state_machine.reader().read_metadata().await.unwrap();
        assert_eq!(legacy.applied, Some(log_id(3, 0)));
        assert!(legacy.view.origin().is_none());
        assert_eq!(
            compatible_directory(legacy.view, &initial).unwrap(),
            Some(initial.clone())
        );
        let results = stores
            .state_machine
            .apply([entry(1, initialize(initial.clone()))])
            .await
            .unwrap();
        assert_eq!(
            bootstrap_apply_result(results[0].clone()),
            BootstrapOutcome::Ready {
                revision: NonZeroU64::new(1).unwrap(),
            }
        );
        let movement = MoveReplica {
            job_id: MoveJobId::new([1; 16]).unwrap(),
            data_group: GroupId::new(1),
            remove_node: NodeId::new(3).unwrap(),
            add_node: endpoint(4),
            expected_directory_revision: 1,
        }
        .derive_intent(&initial)
        .unwrap();
        stores
            .state_machine
            .apply([
                entry(
                    2,
                    Command::Metadata(Box::new(MetadataCommand::BeginMove {
                        intent: movement.clone(),
                    })),
                ),
                entry(
                    3,
                    Command::Metadata(Box::new(MetadataCommand::CompleteMove {
                        expected_directory_revision: 1,
                        data_group: movement.data_group,
                        job_id: movement.job_id,
                        observation: MembershipObservation {
                            uniform: true,
                            voters: movement.target_voters,
                            replacement_endpoint: movement.add_node.clone(),
                            membership: CommittedLogPosition {
                                term: 9,
                                leader_node_id: NodeId::new(2).unwrap(),
                                index: 99,
                            },
                        },
                    })),
                ),
            ])
            .await
            .unwrap();
        let read = stores.state_machine.reader().read_metadata().await.unwrap();
        assert_eq!(read.applied, Some(log_id(3, 3)));
        assert_eq!(read.view.origin(), Some(&initial));
        assert!(read.view.job(movement.data_group).is_some());
        let current = compatible_directory(read.view, &initial).unwrap().unwrap();
        assert_eq!(current.revision, 2);
        assert_eq!(
            current.voters(movement.data_group).unwrap(),
            movement.target_voters
        );
        let results = stores
            .state_machine
            .apply([entry(4, initialize(initial.clone()))])
            .await
            .unwrap();
        assert_eq!(
            bootstrap_apply_result(results[0].clone()),
            BootstrapOutcome::Ready {
                revision: NonZeroU64::new(2).unwrap(),
            }
        );
        let read = stores.state_machine.reader().read_metadata().await.unwrap();
        assert_eq!(read.applied, Some(log_id(3, 4)));
        assert_eq!(
            compatible_directory(read.view, &initial).unwrap(),
            Some(current)
        );
        let mut incompatible = initial.clone();
        incompatible.nodes[2].peer_address.set_port(19999);
        let read = stores.state_machine.reader().read_metadata().await.unwrap();
        assert!(compatible_directory(read.view, &incompatible).is_err());
        let results = stores
            .state_machine
            .apply([entry(5, initialize(incompatible))])
            .await
            .unwrap();
        assert!(matches!(
            bootstrap_apply_result(results[0].clone()),
            BootstrapOutcome::Conflict(_)
        ));
        assert_eq!(
            stores
                .state_machine
                .reader()
                .read_metadata()
                .await
                .unwrap()
                .applied,
            Some(log_id(3, 5))
        );
        stores
            .state_machine
            .apply([entry(
                6,
                Command::Delete {
                    key: Bytes::from_static(ORIGIN_KEY),
                },
            )])
            .await
            .unwrap();
        assert!(
            stores.state_machine.reader().read_metadata().await.is_err(),
            "later revision without origin is corruption"
        );
    }
}
