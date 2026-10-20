//! Concrete source/joint/target reconciliation and native membership ownership.
use super::*;

impl NodeService {
    pub(super) async fn reconcile_move(
        &self,
        data_group: GroupId,
        job_id: MoveJobId,
        deadline: Instant,
    ) -> Result<(), ReadFailure> {
        self.movement.check_running(deadline)?;
        let state = self.fetch_move(data_group, deadline).await?;
        let record = state
            .record
            .ok_or_else(|| read_error(FailureReason::InvalidRequest, "move is not accepted"))?;
        if record.intent.job_id != job_id {
            return Err(read_error(
                FailureReason::InvalidRequest,
                "different lifetime job is retained",
            ));
        }
        let group = self
            .groups
            .get(&data_group)
            .ok_or_else(|| read_error(FailureReason::WrongGroup, "group not hosted"))?;
        if matches!(record.progress, MoveProgress::Completed { .. }) {
            return seal_completed_move(group, &record.intent).await;
        }
        let intent = record.intent;
        let observed = self.observe_membership(data_group, deadline).await?;
        match topology(&observed, &intent)? {
            Topology::Source => {
                #[cfg(test)]
                stale_source::pause_source(self, &intent, &observed).await;
                self.ensure_learner(group, &intent, &observed, deadline)
                    .await?;
                let caught_up_vote = self.wait_for_catchup(group, &intent, deadline).await?;
                #[cfg(test)]
                pause_once(&self.movement.after_catchup).await;
                let observed = self.observe_membership(data_group, deadline).await?;
                if topology(&observed, &intent)? != Topology::Target {
                    self.promote(group, &intent, Some(caught_up_vote), deadline)
                        .await?;
                }
            }
            Topology::Joint => self.promote(group, &intent, None, deadline).await?,
            Topology::Target => {}
        }
        let observed = self.observe_membership(data_group, deadline).await?;
        require_target(&observed, &intent)?;
        info!(
            "raft_move_uniform group={} membership={:?}",
            data_group.get(),
            observed.membership
        );
        #[cfg(test)]
        pause_once(&self.movement.before_publication).await;
        // No witness crosses this request boundary. Group zero independently
        // reads the retained intent and obtains a new data authority observation.
        loop {
            self.movement.check_running(deadline)?;
            if let Some(BackendReply::Move { result, .. }) = self
                .metadata_request(
                    BackendOperation::PublishMove { data_group, job_id },
                    deadline,
                )
                .await
            {
                match result {
                    AdminResult::Completed { .. } => {
                        return seal_completed_move(group, &intent).await;
                    }
                    AdminResult::Conflict { message } => {
                        return Err(read_error(FailureReason::InvalidRequest, message));
                    }
                    _ => {}
                }
            }
            sleep_until(deadline.min(Instant::now() + Duration::from_millis(25))).await;
        }
    }

    async fn ensure_learner(
        &self,
        group: &HostedGroup,
        intent: &MoveIntent,
        observed: &AppliedMembership,
        deadline: Instant,
    ) -> Result<(), ReadFailure> {
        if let Some(address) = observed.peer(intent.add_node.node_id) {
            if address != intent.add_node.peer_address {
                return Err(read_error(
                    FailureReason::InvalidRequest,
                    "replacement peer conflicts with retained target",
                ));
            }
            return Ok(());
        }
        self.movement.check_running(deadline)?;
        // The gate is independent from RocksDB writes. Re-read the durable
        // local seal after acquiring it, including for a stale Source runner.
        let _native_membership = group
            .reader
            .membership_control()
            .begin(movement_identity(intent))
            .await
            .map_err(|error| read_error(FailureReason::Unavailable, error))?;
        self.movement.check_running(deadline)?;
        info!(
            "raft_move_add_learner group={} node={}",
            intent.data_group.get(),
            intent.add_node.node_id.get()
        );
        // Direct await owns the actual native future beyond caller disconnection.
        #[cfg(test)]
        let native_observation = self
            .movement
            .native_learner
            .lock()
            .expect("native learner observation slot poisoned")
            .as_ref()
            .and_then(|observer| {
                observer.enter(
                    stale_source::SourceIdentity {
                        node: self.identity.node_id,
                        group: intent.data_group,
                        job: intent.job_id,
                        learner: intent.add_node.node_id,
                        peer: intent.add_node.peer_address,
                    },
                    false,
                )
            });
        let result = group
            .raft
            .add_learner(
                intent.add_node.node_id.get(),
                BasicNode::new(intent.add_node.peer_address.to_string()),
                false,
            )
            .await;
        #[cfg(test)]
        if let Some(observer) = native_observation {
            observer.finish(&result);
        }
        result.map_err(|error| read_error(FailureReason::Unavailable, error))?;
        Ok(())
    }

    async fn wait_for_catchup(
        &self,
        group: &HostedGroup,
        intent: &MoveIntent,
        deadline: Instant,
    ) -> Result<openraft::Vote<u64>, ReadFailure> {
        self.movement.check_running(deadline)?;
        let vote = group.raft.metrics().borrow().vote;
        let required = authority_barrier(intent.data_group, group, deadline)
            .await?
            .ok_or_else(|| {
                read_error(
                    FailureReason::Internal,
                    "catch-up barrier has no applied identity",
                )
            })?;
        let (applied, membership) = group
            .reader
            .read_membership()
            .await
            .map_err(|error| read_error(FailureReason::Internal, error))?;
        check_applied_covers(applied, Some(required))?;
        if membership
            .membership()
            .get_node(&intent.add_node.node_id.get())
            .is_none_or(|node| node.addr != intent.add_node.peer_address.to_string())
        {
            return Err(read_error(
                FailureReason::InvalidRequest,
                "replacement missing or peer changed at catch-up barrier",
            ));
        }
        loop {
            self.movement.check_running(deadline)?;
            {
                let metrics = group.raft.metrics();
                let current = metrics.borrow();
                if current.vote != vote
                    || current.current_leader != Some(self.identity.node_id.get())
                    || !vote.committed
                {
                    return Err(read_error(
                        FailureReason::NotLeader,
                        "leader authority changed during catch-up",
                    ));
                }
                if current
                    .membership_config
                    .membership()
                    .get_node(&intent.add_node.node_id.get())
                    .is_none_or(|node| node.addr != intent.add_node.peer_address.to_string())
                {
                    return Err(read_error(
                        FailureReason::InvalidRequest,
                        "replacement missing or peer changed during catch-up",
                    ));
                }
                let matched = current
                    .replication
                    .as_ref()
                    .and_then(|replication| replication.get(&intent.add_node.node_id.get()))
                    .copied()
                    .flatten();
                if covers(matched, Some(required)) {
                    info!(
                        "raft_move_caught_up group={} node={} barrier={required}",
                        intent.data_group.get(),
                        intent.add_node.node_id.get()
                    );
                    return Ok(vote);
                }
            }
            sleep_until(deadline.min(Instant::now() + Duration::from_millis(10))).await;
        }
    }

    async fn promote(
        &self,
        group: &HostedGroup,
        intent: &MoveIntent,
        expected_vote: Option<openraft::Vote<u64>>,
        deadline: Instant,
    ) -> Result<(), ReadFailure> {
        self.movement.check_running(deadline)?;
        // Retain this permit through BOTH native membership steps. Seal must
        // wait for the real future, even after its request observer disconnects.
        let _native_membership = group
            .reader
            .membership_control()
            .begin(movement_identity(intent))
            .await
            .map_err(|error| read_error(FailureReason::Unavailable, error))?;
        self.movement.check_running(deadline)?;
        if let Some(expected_vote) = expected_vote {
            let metrics = group.raft.metrics();
            let current = metrics.borrow();
            check_promotion_authority(
                expected_vote,
                current.vote,
                current.current_leader,
                self.identity.node_id.get(),
            )?;
        }
        let target: BTreeSet<_> = intent.target_voters.iter().map(|id| id.get()).collect();
        info!(
            "raft_move_promote group={} target={target:?}",
            intent.data_group.get()
        );
        // Own both native steps inside change_membership until resolution.
        // Every executor has this same lifetime target; none can restore ABC.
        #[cfg(test)]
        let native_observation = self
            .movement
            .native_membership
            .lock()
            .expect("native observation slot poisoned")
            .as_ref()
            .and_then(|observer| {
                observer.enter(native_membership_observation::NativeMembershipIdentity {
                    node: self.identity.node_id,
                    group: intent.data_group,
                    job: intent.job_id,
                    target: target.clone(),
                })
            });
        let result = group.raft.change_membership(target, false).await;
        #[cfg(test)]
        if let Some(observer) = native_observation {
            observer.finish(&result);
        }
        result.map_err(|error| read_error(FailureReason::Unavailable, error))?;
        Ok(())
    }
}

fn movement_identity(intent: &MoveIntent) -> crate::storage::MovementIdentity {
    crate::storage::MovementIdentity {
        revision: intent.expected_directory_revision,
        job_id: intent.job_id,
    }
}

/// Completion closes this node's old-job admission only. Accepting a successor
/// still requires every possible executor plus a committed data-group fence.
async fn seal_completed_move(group: &HostedGroup, intent: &MoveIntent) -> Result<(), ReadFailure> {
    group
        .reader
        .membership_control()
        .seal(movement_identity(intent))
        .await
        .map_err(|error| read_error(FailureReason::Unavailable, error))?;
    info!(
        "raft_move_local_sealed group={} revision={} job={:?}",
        intent.data_group.get(),
        intent.expected_directory_revision,
        intent.job_id.get()
    );
    Ok(())
}
