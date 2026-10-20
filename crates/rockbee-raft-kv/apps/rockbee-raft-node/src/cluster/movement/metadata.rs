//! Conditional acceptance and receiver-owned final placement publication.
use super::*;

impl NodeService {
    /// Acceptance checks retained identity before deriving from mutable placement.
    pub(crate) async fn accept_move(&self, request: MoveReplica, deadline: Instant) -> AdminResult {
        let Ok(_permit) = Arc::clone(&self.movement.publication).try_acquire_owned() else {
            return unknown("metadata control admission is full or closed");
        };
        let result = self.accept_move_inner(request, deadline).await;
        result.unwrap_or_else(|error| unknown(error.0.message))
    }

    async fn accept_move_inner(
        &self,
        request: MoveReplica,
        deadline: Instant,
    ) -> Result<AdminResult, ReadFailure> {
        self.movement.check_running(deadline)?;
        let state = self.read_move(request.data_group, deadline).await?;
        if let Some(record) = state.record {
            return Ok(if matches_request(&record.intent, &request) {
                record_result(record)
            } else {
                conflict("a different lifetime move is already retained")
            });
        }
        let intent = match request.derive_intent(&state.directory) {
            Ok(intent) => intent,
            Err(error) => return Ok(conflict(error)),
        };
        let command = MetadataCommand::BeginMove {
            intent: intent.clone(),
        };
        let result = self.propose_metadata(command, deadline).await?;
        match result {
            MetadataResult::Accepted { .. } => {
                info!(
                    "raft_move_accepted group={} job={:?}",
                    intent.data_group.get(),
                    intent.job_id.get()
                );
                Ok(AdminResult::Accepted { intent })
            }
            MetadataResult::AlreadyAccepted { .. } => Ok(AdminResult::AlreadyAccepted { intent }),
            MetadataResult::Conflict { reason } => Ok(conflict(format!(
                "conditional acceptance conflict: {reason:?}"
            ))),
            _ => Err(read_error(
                FailureReason::Internal,
                "unexpected move acceptance result",
            )),
        }
    }

    /// A current leader barrier and one coherent metadata snapshot establish authority.
    pub(crate) async fn read_move(
        &self,
        data_group: GroupId,
        deadline: Instant,
    ) -> Result<MoveState, ReadFailure> {
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
        let record = read.view.job(data_group).cloned();
        let directory = compatible_directory(read.view, origin)?
            .ok_or_else(|| read_error(FailureReason::Unavailable, "directory not initialized"))?;
        Ok(MoveState { directory, record })
    }

    pub(crate) async fn publish_move(
        &self,
        data_group: GroupId,
        job_id: MoveJobId,
        deadline: Instant,
    ) -> AdminResult {
        let Ok(_permit) = Arc::clone(&self.movement.publication).try_acquire_owned() else {
            return unknown("metadata control admission is full or closed");
        };
        self.publish_move_inner(data_group, job_id, deadline)
            .await
            .unwrap_or_else(|error| unknown(error.0.message))
    }

    async fn publish_move_inner(
        &self,
        data_group: GroupId,
        job_id: MoveJobId,
        deadline: Instant,
    ) -> Result<AdminResult, ReadFailure> {
        loop {
            self.movement.check_running(deadline)?;
            let state = self.read_move(data_group, deadline).await?;
            let record = state
                .record
                .ok_or_else(|| read_error(FailureReason::InvalidRequest, "unknown move"))?;
            if record.intent.job_id != job_id {
                return Ok(conflict("different lifetime job retained"));
            }
            if matches!(record.progress, MoveProgress::Completed { .. }) {
                return Ok(record_result(record));
            }
            let intent = &record.intent;
            let mut routes = state.directory.nodes.clone();
            if !routes
                .iter()
                .any(|node| node.node_id == intent.add_node.node_id)
            {
                routes.push(intent.add_node.clone());
            }
            let mut observation = None;
            for endpoint in routes {
                self.movement.check_running(deadline)?;
                let attempt = deadline.min(Instant::now() + Duration::from_millis(500));
                let reply = self
                    .backend_request(
                        endpoint.node_id,
                        endpoint.data_address,
                        BackendOperation::ObserveMembership { data_group },
                        attempt,
                    )
                    .await;
                if let Some(BackendReply::Membership {
                    data_group: actual,
                    result: Ok(proof),
                }) = reply
                {
                    if actual != data_group || proof.data_group != data_group {
                        return Err(read_error(
                            FailureReason::InvalidRequest,
                            "membership response group mismatch",
                        ));
                    }
                    require_target(&proof, intent)?;
                    observation = Some(MembershipObservation {
                        uniform: true,
                        voters: intent.target_voters,
                        replacement_endpoint: intent.add_node.clone(),
                        membership: completion_position(proof.membership)?,
                    });
                    break;
                }
            }
            let observation = observation.ok_or_else(|| {
                read_error(
                    FailureReason::Unavailable,
                    "no authoritative target membership observation",
                )
            })?;
            let result = self
                .propose_metadata(
                    MetadataCommand::CompleteMove {
                        expected_directory_revision: state.directory.revision,
                        data_group,
                        job_id,
                        observation,
                    },
                    deadline,
                )
                .await?;
            match result {
                MetadataResult::Completed { .. } | MetadataResult::AlreadyCompleted { .. } => {
                    let state = self.read_move(data_group, deadline).await?;
                    let record = state.record.ok_or_else(|| {
                        read_error(FailureReason::Internal, "completed move missing")
                    })?;
                    info!(
                        "raft_move_published group={} revision={}",
                        data_group.get(),
                        state.directory.revision
                    );
                    return Ok(record_result(record));
                }
                MetadataResult::Conflict {
                    reason: ConflictReason::RevisionMismatch,
                } => continue,
                MetadataResult::Conflict { reason } => {
                    return Ok(conflict(format!(
                        "conditional publication conflict: {reason:?}"
                    )));
                }
                _ => {
                    return Err(read_error(
                        FailureReason::Internal,
                        "unexpected publication result",
                    ));
                }
            }
        }
    }

    async fn propose_metadata(
        &self,
        command: MetadataCommand,
        deadline: Instant,
    ) -> Result<MetadataResult, ReadFailure> {
        validate_metadata_candidate(&command, &self.storage, self.peers)
            .map_err(|error| read_error(FailureReason::InvalidRequest, error))?;
        self.movement.check_running(deadline)?;
        let group = self
            .groups
            .get(&GroupId::new(0))
            .ok_or_else(|| read_error(FailureReason::NotLeader, "metadata group not hosted"))?;
        if self.leader_hint(GroupId::new(0)) != Some(self.identity.node_id) {
            return Err(read_error(
                FailureReason::NotLeader,
                "not the metadata leader",
            ));
        }
        match group
            .raft
            .client_write(Command::Metadata(Box::new(command)))
            .await
            .map_err(|error| read_error(FailureReason::Unavailable, error))?
            .data
        {
            ApplyResult::Metadata(result) => Ok(result),
            _ => Err(read_error(
                FailureReason::Internal,
                "unexpected metadata apply response",
            )),
        }
    }
}

fn matches_request(intent: &MoveIntent, request: &MoveReplica) -> bool {
    intent.job_id == request.job_id
        && intent.data_group == request.data_group
        && intent.remove_node == request.remove_node
        && intent.add_node == request.add_node
        && intent.expected_directory_revision == request.expected_directory_revision
}

fn record_result(record: MoveRecord) -> AdminResult {
    if matches!(record.progress, MoveProgress::Completed { .. }) {
        AdminResult::Completed { record }
    } else {
        AdminResult::AlreadyAccepted {
            intent: record.intent,
        }
    }
}

fn conflict(message: impl std::fmt::Display) -> AdminResult {
    AdminResult::Conflict {
        message: message.to_string().chars().take(256).collect(),
    }
}

fn unknown(message: impl std::fmt::Display) -> AdminResult {
    AdminResult::Unknown {
        message: message.to_string().chars().take(256).collect(),
    }
}
