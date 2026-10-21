//! Bounded per-group task ownership, admission and cancellation-safe draining.
use super::*;

pub(super) struct RunningMove {
    pub(super) job_id: MoveJobId,
    pub(super) task: JoinHandle<()>,
}

pub(in crate::cluster) struct MoveRunners {
    pub(super) tasks: Mutex<BTreeMap<GroupId, RunningMove>>,
    pub(super) active: Arc<Semaphore>,
    #[cfg(test)]
    pub(super) before_source_learner: Mutex<Option<Arc<tests::stale_source::SourcePause>>>,
    #[cfg(test)]
    pub(super) native_learner: Mutex<Option<Arc<tests::stale_source::NativeLearnerObservation>>>,
    // Separate pool avoids runner -> colocated metadata publication deadlock.
    pub(super) publication: Arc<Semaphore>,
    pub(super) stopping: AtomicBool,
    #[cfg(test)]
    pub(super) after_catchup: Mutex<Option<Arc<TestPause>>>,
    #[cfg(test)]
    pub(super) before_publication: Mutex<Option<Arc<TestPause>>>,
    #[cfg(test)]
    pub(super) native_membership:
        Mutex<Option<Arc<tests::native_membership_observation::NativeMembershipObservation>>>,
    #[cfg(test)]
    pub(super) handoff_seal: Mutex<Option<Arc<tests::handoff::SealObservation>>>,
}

impl MoveRunners {
    pub(in crate::cluster) fn new() -> Self {
        Self {
            tasks: Mutex::new(BTreeMap::new()),
            active: Arc::new(Semaphore::new(4)),
            publication: Arc::new(Semaphore::new(2)),
            #[cfg(test)]
            before_source_learner: Mutex::new(None),
            #[cfg(test)]
            native_learner: Mutex::new(None),
            stopping: AtomicBool::new(false),
            #[cfg(test)]
            after_catchup: Mutex::new(None),
            #[cfg(test)]
            before_publication: Mutex::new(None),
            #[cfg(test)]
            native_membership: Mutex::new(None),
            #[cfg(test)]
            handoff_seal: Mutex::new(None),
        }
    }
    pub(in crate::cluster) fn stop(&self) {
        self.stopping.store(true, Ordering::Release);
        self.active.close();
        self.publication.close();
    }
    pub(in crate::cluster) async fn drain(&self) {
        loop {
            let finished = {
                let mut tasks = match self.tasks.lock() {
                    Ok(tasks) => tasks,
                    Err(_) => return,
                };
                let ids: Vec<_> = tasks
                    .iter()
                    .filter_map(|(id, task)| task.task.is_finished().then_some(*id))
                    .collect();
                let finished: Vec<_> = ids.into_iter().filter_map(|id| tasks.remove(&id)).collect();
                if tasks.is_empty() && finished.is_empty() {
                    return;
                }
                finished
            };
            // Only completed handles leave the registry. Their join is already
            // ready; cancelling drain never detaches a pending native future.
            for task in finished {
                if let Err(error) = task.task.await {
                    warn!("raft_move_runner_join_failed error={error}");
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    pub(super) fn check_running(&self, deadline: Instant) -> Result<(), ReadFailure> {
        if self.stopping.load(Ordering::Acquire) {
            return Err(read_error(
                FailureReason::Unavailable,
                "movement runtime is draining",
            ));
        }
        check_read_deadline(deadline)
    }
}

impl NodeService {
    pub(crate) async fn schedule_move(
        &self,
        data_group: GroupId,
        job_id: MoveJobId,
    ) -> Result<ReconcileStatus, ReadFailure> {
        if data_group.is_metadata() || !self.groups.contains_key(&data_group) {
            return Err(read_error(
                FailureReason::WrongGroup,
                "data group not hosted",
            ));
        }
        if self.leader_hint(data_group) != Some(self.identity.node_id) {
            return Err(read_error(
                FailureReason::NotLeader,
                "not the observed data leader",
            ));
        }
        loop {
            let finished = {
                let mut tasks = self.movement.tasks.lock().map_err(|_| {
                    read_error(FailureReason::Internal, "movement task registry poisoned")
                })?;
                if self.movement.stopping.load(Ordering::Acquire) {
                    return Ok(ReconcileStatus::Stopped);
                }
                match tasks.get(&data_group) {
                    Some(running) if !running.task.is_finished() => {
                        if running.job_id != job_id {
                            return Err(read_error(
                                FailureReason::InvalidRequest,
                                "a different job owns this group's active runner",
                            ));
                        }
                        return Ok(ReconcileStatus::Running);
                    }
                    Some(_) => tasks.remove(&data_group),
                    None => {
                        let Ok(permit) = Arc::clone(&self.movement.active).try_acquire_owned()
                        else {
                            return Ok(ReconcileStatus::Busy);
                        };
                        let service = self.clone();
                        let task = tokio::spawn(async move {
                            let _permit = permit;
                            let result = service
                                .reconcile_move(data_group, job_id, Instant::now() + RUN_BUDGET)
                                .await;
                            if let Err(error) = result {
                                warn!(
                                    "raft_move_runner_pending node={} group={} job={:?} error={}",
                                    service.identity.node_id.get(),
                                    data_group.get(),
                                    job_id.get(),
                                    error.0.message
                                );
                            }
                        });
                        tasks.insert(data_group, RunningMove { job_id, task });
                        return Ok(ReconcileStatus::Scheduled);
                    }
                }
            };
            // Join every completed task, then recheck the slot: another caller
            // can schedule while this caller waits outside the registry lock.
            if let Some(task) = finished
                && let Err(error) = task.task.await
            {
                warn!("raft_move_runner_join_failed error={error}");
            }
        }
    }
}
