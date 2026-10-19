//! Backend operations translate to explicit authority checks and Raft calls.
use std::{collections::BTreeMap, time::Duration};

use bytes::Bytes;
use log::{debug, trace, warn};
use openraft::{
    Raft,
    error::{CheckIsLeaderError, RaftError},
};
use rockbee_raft_protocol::{
    Failure, FailureReason, GroupId, KvRequest, KvResponse, MutationOutcome, NodeId, ReadFailure,
    WireRequest,
    backend::{BackendOperation, BackendReply, BackendRequest, BackendResponse},
    routing::ClusterDirectory,
    transport::server::RequestHandler,
};
use tokio::time::{Instant, sleep_until};

use crate::{
    rpc::PeerIdentity,
    storage::StateReader,
    types::{Command, TypeConfig},
};

pub(super) const DIRECTORY_KEY: &[u8] = b"rockbee/internal/initial-directory/v1";

pub(super) struct HostedGroup {
    pub raft: Raft<TypeConfig>,
    pub reader: StateReader,
}

/// The node's application service; peer RPC and public client requests stay separate.
pub struct NodeService {
    identity: PeerIdentity,
    directory: ClusterDirectory,
    pub(super) groups: BTreeMap<GroupId, HostedGroup>,
}

impl NodeService {
    pub(super) fn new(
        identity: PeerIdentity,
        directory: ClusterDirectory,
        groups: BTreeMap<GroupId, HostedGroup>,
    ) -> Self {
        Self {
            identity,
            directory,
            groups,
        }
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
        let group = self.groups.get(&GroupId::new(0)).ok_or_else(|| {
            ReadFailure(failure(
                FailureReason::Unavailable,
                "metadata group not hosted",
            ))
        })?;
        let value = strong_read(
            GroupId::new(0),
            group,
            Bytes::from_static(DIRECTORY_KEY),
            deadline,
        )
        .await?
        .ok_or_else(|| {
            ReadFailure(failure(
                FailureReason::Unavailable,
                "directory is not initialized",
            ))
        })?;
        self.validate_stored_directory(&value)
    }

    fn validate_stored_directory(&self, value: &[u8]) -> Result<ClusterDirectory, ReadFailure> {
        let directory: ClusterDirectory = rockbee_raft_protocol::decode_frame(value)
            .map_err(|error| ReadFailure(failure(FailureReason::Internal, error)))?;
        directory
            .validate()
            .map_err(|error| ReadFailure(failure(FailureReason::Internal, error)))?;
        if directory != self.directory {
            return Err(ReadFailure(failure(
                FailureReason::Internal,
                "stored directory differs from configured immutable bootstrap",
            )));
        }
        Ok(directory)
    }

    async fn bootstrap_directory(&self, deadline: Instant) -> MutationOutcome {
        let id = GroupId::new(0);
        let Some(group) = self.groups.get(&id) else {
            return MutationOutcome::Rejected(failure(
                FailureReason::Unavailable,
                "metadata group not hosted",
            ));
        };
        match strong_read(id, group, Bytes::from_static(DIRECTORY_KEY), deadline).await {
            Ok(Some(value)) => {
                return match self.validate_stored_directory(&value) {
                    Ok(_) => MutationOutcome::Committed,
                    Err(error) => MutationOutcome::Rejected(error.0),
                };
            }
            Err(error) => return MutationOutcome::Rejected(error.0),
            Ok(None) => {}
        }
        let value = match rockbee_raft_protocol::encode_frame(&self.directory) {
            Ok(value) => Bytes::from(value),
            Err(error) => {
                return MutationOutcome::Rejected(failure(FailureReason::Internal, error));
            }
        };
        // This temporary bootstrap protocol is safe only while every possible
        // proposal has the same immutable configured bytes. Before allowing
        // directory changes, replace this with an apply-time put-if-absent.
        self.mutate(
            id,
            group,
            Command::Put {
                key: Bytes::from_static(DIRECTORY_KEY),
                value,
            },
            deadline,
        )
        .await
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
        if self.directory.shard_config.group_for_key(key).ok() != Some(group_id) {
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
        // After invoking client_write, even ForwardToLeader can refer to a
        // previously appended proposal. No error here proves safe rejection.
        // Do not cancel this future on the caller's timeout: the server's global
        // request permit remains held until the actual Raft call resolves.
        match group.raft.client_write(command).await {
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
    let view = group
        .reader
        .read_local(key)
        .await
        .map_err(|error| ReadFailure(failure(FailureReason::Internal, error)))?;
    check_read_deadline(deadline)?;
    if view.applied.map(|id| id.index) < required.map(|id| id.index) {
        return Err(ReadFailure(failure(
            FailureReason::Internal,
            "state view does not cover the read barrier",
        )));
    }
    Ok(view.value)
}

fn check_read_deadline(deadline: Instant) -> Result<(), ReadFailure> {
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
        BackendOperation::Directory => BackendReply::Directory(Err(ReadFailure(error))),
        BackendOperation::BootstrapDirectory => {
            BackendReply::Bootstrap(MutationOutcome::Rejected(error))
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

fn failure(reason: FailureReason, message: impl std::fmt::Display) -> Failure {
    // At most256 Unicode scalars occupy at most1024 UTF-8 bytes.
    Failure {
        reason,
        message: message.to_string().chars().take(256).collect(),
    }
}
