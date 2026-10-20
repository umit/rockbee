//! Completed movement across four real CLI deaths; no exact-phase fault hook.
use std::{fs, io::Write, os::unix::process::ExitStatusExt, sync::Arc};

use rockbee_raft_protocol::{
    GroupId,
    backend::{AppliedMembership, MoveState, ObservedLogPosition, ReconcileStatus},
    control::{AdminResult, MoveIntent, MoveJobId, MoveProgress, MoveReplica},
};
use tokio::signal::unix::{SignalKind, signal};

use super::*;

const MOVED: GroupId = GroupId::new(1);
const SIBLING: GroupId = GroupId::new(2);
type BackendLink = Link<BackendRequest, BackendResponse>;

macro_rules! require {
    ($condition:expr, $($message:tt)+) => {
        if !$condition { return Err(format!($($message)+)); }
    };
}

struct Scenario {
    cluster: ProcessCluster,
    // Retain any in-flight Link for common signal/deadline cleanup, too.
    links: Vec<BackendLink>,
    gateway: Option<GatewayRuntime>,
    client: Option<RockBeeRaftClient>,
    receipt: fs::File,
    deadline: Instant,
}

impl Scenario {
    fn record(&mut self, message: impl std::fmt::Display) -> Result<(), String> {
        writeln!(self.receipt, "{message}").map_err(|error| error.to_string())?;
        self.receipt.flush().map_err(|error| error.to_string())
    }

    fn remaining(&self) -> Result<(), String> {
        require!(
            Instant::now() < self.deadline,
            "completed-move process scenario deadline exceeded"
        );
        Ok(())
    }

    async fn backend(
        &mut self,
        endpoint: &NodeEndpoint,
        operation: BackendOperation,
    ) -> Option<BackendReply> {
        let deadline = self
            .deadline
            .min(Instant::now() + Duration::from_millis(500));
        let link = BackendLink::connect(endpoint.data_address, LinkConfig::default(), deadline)
            .await
            .ok()?;
        self.links.push(link);
        let link = self.links.last()?;
        let response = match link.try_submit(
            BackendRequest {
                cluster_id: self.cluster.directory.cluster_id,
                target_node: endpoint.node_id,
                operation,
            },
            deadline,
        ) {
            Ok(pending) => pending.await.ok(),
            Err(_) => None,
        };
        // The typed Link validates cluster/node/group/job, then joins on close.
        link.close().await;
        self.links.pop();
        response.map(|response| response.reply)
    }

    async fn state(&mut self) -> Result<(NodeEndpoint, MoveState), String> {
        loop {
            self.remaining()?;
            for endpoint in self.cluster.directory.nodes.clone() {
                if let Some(BackendReply::MoveState {
                    result: Ok(state), ..
                }) = self
                    .backend(&endpoint, BackendOperation::GetMove { data_group: MOVED })
                    .await
                {
                    return Ok((endpoint, *state));
                }
            }
            sleep(Duration::from_millis(20)).await;
        }
    }

    async fn membership(
        &mut self,
        group: GroupId,
        endpoints: &[NodeEndpoint],
    ) -> Result<(NodeEndpoint, AppliedMembership), String> {
        loop {
            self.remaining()?;
            for endpoint in endpoints {
                if let Some(BackendReply::Membership {
                    result: Ok(proof), ..
                }) = self
                    .backend(
                        endpoint,
                        BackendOperation::ObserveMembership { data_group: group },
                    )
                    .await
                {
                    proof.validate().map_err(|error| error.to_string())?;
                    return Ok((endpoint.clone(), *proof));
                }
            }
            sleep(Duration::from_millis(20)).await;
        }
    }

    async fn exact_read(
        &mut self,
        name: &str,
        key: &Bytes,
        expected: Option<&Bytes>,
    ) -> Result<(), String> {
        loop {
            self.remaining()?;
            let result = self
                .client
                .as_ref()
                .ok_or("public client missing")?
                .get(key.clone())
                .await;
            match result {
                Ok(value) => {
                    self.record(format!("read name={name} key={key:?} result={value:?}"))?;
                    require!(
                        value.as_ref() == expected,
                        "{name}: successful GET mismatch: expected {expected:?}, got {value:?}"
                    );
                    return Ok(());
                }
                Err(_) => sleep(Duration::from_millis(20)).await,
            }
        }
    }

    async fn put(&mut self, name: &str, key: &Bytes, value: &Bytes) -> Result<(), String> {
        self.remaining()?;
        let result = self
            .client
            .as_ref()
            .ok_or("public client missing")?
            .put(key.clone(), value.clone())
            .map_err(|error| error.to_string())?
            .await;
        self.record(format!(
            "put name={name} key={key:?} outcome={result:?} replay=false"
        ))?;
        require!(
            result == MutationOutcome::Committed,
            "{name}: original public PUT must commit, got {result:?}; not replayed"
        );
        Ok(())
    }

    async fn delete(&mut self, name: &str, key: &Bytes) -> Result<(), String> {
        self.remaining()?;
        let result = self
            .client
            .as_ref()
            .ok_or("public client missing")?
            .delete(key.clone())
            .map_err(|error| error.to_string())?
            .await;
        self.record(format!(
            "delete name={name} key={key:?} outcome={result:?} replay=false"
        ))?;
        require!(
            result == MutationOutcome::Committed,
            "{name}: original public DELETE must commit, got {result:?}; not replayed"
        );
        Ok(())
    }

    fn crash_index(&mut self, index: usize, id: u64, phase: &str) -> Result<(), String> {
        let node = &mut self.cluster.nodes[index];
        let pid = node.child.id();
        require!(
            node.child
                .try_wait()
                .map_err(|error| error.to_string())?
                .is_none(),
            "node {id} PID {pid} exited before requested crash"
        );
        // This is Child::kill + Child::wait, never a graceful stop or port lookup.
        let status = node.crash();
        self.record(format!("crash phase={phase} node={id} pid={pid} child_kill=true waited=true status={status} signal={:?}", status.signal()))?;
        require!(
            status.signal() == Some(9),
            "node {id} must be reaped after actual SIGKILL, got {status}"
        );
        Ok(())
    }

    async fn cleanup(&mut self) -> Result<(), String> {
        // Called after the scenario future is dropped, including signal/deadline.
        let mut errors = Vec::new();
        for link in &self.links {
            link.close().await;
        }
        self.links.clear();
        if let Some(client) = self.client.take() {
            client.close().await;
        }
        if let Some(mut gateway) = self.gateway.take() {
            gateway.close().await;
        }
        for (index, id) in [2, 3, 1, 4].into_iter().enumerate() {
            let Some(node) = self.cluster.nodes.get_mut(index) else {
                continue;
            };
            let pid = node.child.id();
            let result = match node.child.try_wait() {
                Ok(Some(status)) => Ok(format!("already_reaped={status}")),
                Ok(None) => node
                    .try_stop_gracefully()
                    .await
                    .map(|()| "sigterm=ok".to_owned()),
                Err(error) => Err(error.to_string()),
            };
            node.reap();
            if let Err(error) = self.record(format!(
                "cleanup node={id} pid={pid} result={result:?} reaped=true"
            )) {
                errors.push(format!("cleanup receipt: {error}"));
            }
            if let Err(error) = result {
                errors.push(error);
            }
        }
        if let Err(error) = self.record("cleanup backend_links_joined=true public_client_closed=true gateway_joined=true all_owned_children_reaped=true") {
            errors.push(format!("cleanup receipt: {error}"));
        }
        require!(errors.is_empty(), "graceful cleanup errors: {errors:?}");
        Ok(())
    }
}

fn covers(applied: ObservedLogPosition, required: ObservedLogPosition) -> bool {
    applied == required
        || (applied.index > required.index
            && (applied.term, applied.leader_node_id) >= (required.term, required.leader_node_id))
}

fn exact_target(
    proof: &AppliedMembership,
    intent: &MoveIntent,
    baseline: ObservedLogPosition,
    state: &MoveState,
) -> Result<(), String> {
    proof.validate().map_err(|error| error.to_string())?;
    require!(
        proof.data_group == MOVED
            && proof.voters == intent.target_voters
            && proof.joint_voters.is_none(),
        "expected authoritative uniform ABD: {proof:?}"
    );
    require!(
        proof.peer(intent.remove_node).is_none()
            && proof.peers.iter().flatten().count() == intent.target_voters.len(),
        "removed C or an unexpected learner remains in final replication membership: {proof:?}"
    );
    for id in intent.target_voters {
        let endpoint = state.directory.node(id).ok_or("target endpoint missing")?;
        require!(
            proof.peer(id) == Some(endpoint.peer_address),
            "target peer endpoint changed for {id:?}"
        );
    }
    require!(
        covers(proof.applied, baseline),
        "applied target does not cover full baseline identity"
    );
    let record = state.record.as_ref().ok_or("completed record missing")?;
    let MoveProgress::Completed { membership } = &record.progress else {
        return Err("move is not completed".to_owned());
    };
    let published = ObservedLogPosition {
        term: membership.term,
        leader_node_id: membership.leader_node_id.get(),
        index: membership.index,
    };
    require!(
        covers(proof.membership, published) && covers(proof.applied, published),
        "target does not cover published membership identity"
    );
    Ok(())
}

fn expected_directory(initial: &ClusterDirectory, replacement: &NodeEndpoint) -> ClusterDirectory {
    let mut directory = initial.clone();
    directory.revision = 2;
    directory.nodes.push(replacement.clone());
    directory
        .placements
        .iter_mut()
        .find(|placement| placement.group_id == MOVED)
        .unwrap()
        .voters = [1, 2, 4].map(|id| NodeId::new(id).unwrap()).to_vec();
    directory.validate().unwrap();
    directory
}

fn keys(shards: ShardConfig, group: GroupId, count: usize) -> Vec<Bytes> {
    (0..10_000)
        .map(|nonce| Bytes::from(format!("process-move/{}/{nonce}", group.get())))
        .filter(|key| shards.group_for_key(key).unwrap() == group)
        .take(count)
        .collect()
}

async fn complete(
    scenario: &mut Scenario,
    request: &MoveReplica,
    intent: &MoveIntent,
) -> Result<MoveState, String> {
    loop {
        let (_, state) = scenario.state().await?;
        let record = state.record.as_ref().ok_or("accepted job disappeared")?;
        require!(
            &record.intent == intent,
            "immutable intent changed: {record:?}"
        );
        if matches!(record.progress, MoveProgress::Completed { .. }) {
            return Ok(state);
        }
        let mut endpoints = scenario.cluster.directory.nodes.clone();
        endpoints.push(request.add_node.clone());
        let (leader, _) = scenario.membership(MOVED, &endpoints).await?;
        let reply = scenario
            .backend(
                &leader,
                BackendOperation::ReconcileMove {
                    data_group: MOVED,
                    job_id: request.job_id,
                },
            )
            .await;
        scenario.record(format!(
            "reconcile node={} job={:?} reply={reply:?}",
            leader.node_id.get(),
            request.job_id
        ))?;
        // Re-read durable state before each same-job scheduling attempt. A
        // changed authority is rediscovered through a fresh typed observation.
        sleep(Duration::from_millis(50)).await;
    }
}

async fn run(scenario: &mut Scenario, endpoint: NodeEndpoint) -> Result<(), String> {
    let initial = scenario.cluster.directory.clone();
    scenario
        .cluster
        .wait_until_expected_directory_is_published(&initial, &mut scenario.links)
        .await?;
    scenario.cluster.nodes.push(spawn_join(
        scenario.cluster.root.path(),
        &initial,
        &endpoint,
        "--create",
    ));
    scenario.record(format!(
        "spawn node=4 pid={} mode=join-create endpoint={endpoint:?}",
        scenario.cluster.nodes[3].child.id()
    ))?;
    wait_join_listener_with_links(
        &mut scenario.cluster.nodes[3],
        &endpoint,
        initial.cluster_id,
        &mut scenario.links,
    )
    .await?;
    let client = scenario
        .cluster
        .connect_client_with_gateway(&mut scenario.gateway)
        .await?;
    scenario.client = Some(client);
    let moved = keys(initial.shard_config, MOVED, 3);
    let sibling = keys(initial.shard_config, SIBLING, 1).remove(0);
    require!(
        moved.len() == 3,
        "bounded key selection must produce three moved-group keys"
    );
    let value = Bytes::from_static(b"retained across CLI crash and reopen");
    scenario
        .exact_read("initial-absent", &moved[0], None)
        .await?;
    scenario.put("retained-baseline", &moved[0], &value).await?;
    scenario
        .put("tombstone-baseline", &moved[1], &value)
        .await?;
    scenario.delete("tombstone-baseline", &moved[1]).await?;
    scenario
        .exact_read("sibling-initial-absent", &sibling, None)
        .await?;
    scenario.put("sibling-baseline", &sibling, &value).await?;
    scenario
        .exact_read("retained-baseline", &moved[0], Some(&value))
        .await?;
    scenario
        .exact_read("tombstone-baseline", &moved[1], None)
        .await?;
    let (_, baseline) = scenario.membership(MOVED, &initial.nodes).await?;
    require!(
        baseline.voters.as_slice()
            == initial
                .voters(MOVED)
                .ok_or("initial moved placement missing")?
            && baseline.joint_voters.is_none(),
        "baseline must be authoritative uniform ABC: {baseline:?}"
    );
    scenario.record(format!("baseline_authoritative={baseline:?}"))?;
    let request = MoveReplica {
        job_id: MoveJobId::new([119; 16]).unwrap(),
        data_group: MOVED,
        remove_node: NodeId::new(3).unwrap(),
        add_node: endpoint.clone(),
        expected_directory_revision: 1,
    };
    let intent = request
        .derive_intent(&initial)
        .map_err(|error| error.to_string())?;
    let (metadata, before) = scenario.state().await?;
    require!(
        before.directory == initial && before.record.is_none(),
        "initial durable metadata is not pristine"
    );
    let accepted = scenario
        .backend(&metadata, BackendOperation::MoveReplica(request.clone()))
        .await;
    scenario.record(format!(
        "accept original_node={} request={request:?} reply={accepted:?}",
        metadata.node_id.get()
    ))?;
    // Even if the acceptance reply is lost, GetMove identifies the one retained
    // immutable job. Never submit a second MoveReplica to conceal uncertainty.
    let (_, accepted_state) = scenario.state().await?;
    require!(
        accepted_state
            .record
            .as_ref()
            .is_some_and(|record| record.intent == intent),
        "original acceptance must leave the exact durable job: {accepted_state:?}"
    );
    let completed = complete(scenario, &request, &intent).await?;
    let expected = expected_directory(&initial, &endpoint);
    require!(
        completed.directory == expected,
        "completed placement/catalog must be exactly revision-two ABD; metadata and all sibling placements unchanged: {completed:?}"
    );
    let (_, proof) = scenario.membership(MOVED, &expected.nodes).await?;
    exact_target(&proof, &intent, baseline.applied, &completed)?;
    scenario.record(format!(
        "completed_state={completed:?}\ncompleted_authoritative={proof:?}"
    ))?;
    scenario
        .exact_read("completed-retained", &moved[0], Some(&value))
        .await?;
    scenario
        .exact_read("completed-tombstone", &moved[1], None)
        .await?;
    scenario
        .exact_read("completed-sibling", &sibling, Some(&value))
        .await?;

    // Actual vector order is B,C,A,D. There is no outstanding public mutation.
    for (index, id) in [2, 3, 1, 4].into_iter().enumerate() {
        scenario.crash_index(index, id, "completed-all-four")?;
    }
    scenario.record(
        "offline_D_inspection=not_performed no_claim_of_follower_applied_precrash_durability=true",
    )?;
    scenario.cluster.nodes.clear();
    // Retain the original three-node bootstrap configuration and original paths.
    scenario.cluster.spawn_nodes("--open");
    for (index, id) in [2, 3, 1].into_iter().enumerate() {
        scenario.record(format!(
            "reopen node={id} pid={} mode=open original_bootstrap=true",
            scenario.cluster.nodes[index].child.id()
        ))?;
    }
    scenario
        .cluster
        .wait_until_expected_directory_is_published(&expected, &mut scenario.links)
        .await?;
    // D discovers metadata before opening its old DB, so ABC is authoritative first.
    scenario.cluster.nodes.push(spawn_join(
        scenario.cluster.root.path(),
        &initial,
        &endpoint,
        "--open",
    ));
    scenario.record(format!(
        "reopen node=4 pid={} mode=join-open original_path=true endpoint={endpoint:?}",
        scenario.cluster.nodes[3].child.id()
    ))?;
    wait_join_listener_with_links(
        &mut scenario.cluster.nodes[3],
        &endpoint,
        initial.cluster_id,
        &mut scenario.links,
    )
    .await?;
    let (_, recovered) = scenario.state().await?;
    require!(
        recovered == completed,
        "recovered durable MoveState changed: expected {completed:?}, got {recovered:?}"
    );
    let (leader, proof) = scenario.membership(MOVED, &expected.nodes).await?;
    exact_target(&proof, &intent, baseline.applied, &recovered)?;
    scenario.record(format!(
        "recovered_state={recovered:?}\nrecovered_authoritative={proof:?}"
    ))?;
    let reconciled = scenario
        .backend(
            &leader,
            BackendOperation::ReconcileMove {
                data_group: MOVED,
                job_id: request.job_id,
            },
        )
        .await;
    require!(
        matches!(
            reconciled,
            Some(BackendReply::Reconcile {
                result: Ok(ReconcileStatus::Scheduled | ReconcileStatus::Running),
                ..
            })
        ),
        "completed same-job reconcile must be admitted: {reconciled:?}"
    );
    scenario.record(format!("completed_same_job_reconcile={reconciled:?}"))?;
    let (_, unchanged) = scenario.state().await?;
    require!(
        unchanged == completed,
        "completed reconcile changed lifetime metadata"
    );
    scenario
        .exact_read(
            "recovered-retained-original-client",
            &moved[0],
            Some(&value),
        )
        .await?;
    scenario
        .exact_read("recovered-tombstone-original-client", &moved[1], None)
        .await?;

    scenario.crash_index(2, 1, "reopened-D-required")?;
    let survivors: Vec<_> = expected
        .nodes
        .iter()
        .filter(|node| node.node_id.get() != 1)
        .cloned()
        .collect();
    let (_, proof) = scenario.membership(MOVED, &survivors).await?;
    exact_target(&proof, &intent, baseline.applied, &completed)?;
    scenario.record(format!("B_D_required_authoritative={proof:?}"))?;
    scenario
        .exact_read("D-dependent-fresh-absent", &moved[2], None)
        .await?;
    scenario.put("D-dependent-fresh", &moved[2], &value).await?;
    scenario
        .exact_read("D-dependent-fresh", &moved[2], Some(&value))
        .await?;
    scenario.delete("D-dependent-fresh", &moved[2]).await?;
    scenario
        .exact_read("D-dependent-fresh-deleted", &moved[2], None)
        .await?;
    scenario
        .exact_read("D-dependent-retained", &moved[0], Some(&value))
        .await?;
    scenario
        .exact_read("D-dependent-tombstone", &moved[1], None)
        .await?;
    scenario
        .exact_read("BC-sibling", &sibling, Some(&value))
        .await?;
    scenario.put("BC-sibling", &sibling, &value).await?;
    scenario
        .exact_read("BC-sibling-after-put", &sibling, Some(&value))
        .await?;
    let (metadata, state) = scenario.state().await?;
    require!(
        state == completed,
        "B+C metadata quorum lost exact completed state"
    );
    let directory = scenario
        .backend(&metadata, BackendOperation::Directory)
        .await;
    require!(
        directory == Some(BackendReply::Directory(Ok(expected.clone()))),
        "fresh B+C directory read changed: {directory:?}"
    );
    let conflicting = MoveReplica {
        job_id: MoveJobId::new([120; 16]).unwrap(),
        data_group: MOVED,
        remove_node: endpoint.node_id,
        add_node: initial.node(NodeId::new(3).unwrap()).unwrap().clone(),
        expected_directory_revision: 2,
    };
    conflicting
        .derive_intent(&expected)
        .map_err(|error| format!("reverse candidate must independently be valid: {error}"))?;
    let conflict = scenario
        .backend(&metadata, BackendOperation::MoveReplica(conflicting))
        .await;
    scenario.record(format!("valid_reverse_candidate_reply={conflict:?}"))?;
    require!(
        matches!(
            conflict,
            Some(BackendReply::Move {
                result: AdminResult::Conflict { .. },
                ..
            })
        ),
        "new lifetime job must conflict: {conflict:?}"
    );
    let (_, unchanged) = scenario.state().await?;
    require!(
        unchanged == completed,
        "conflicting new job changed retained metadata"
    );
    scenario.record(format!("final_unchanged_state={unchanged:?}"))?;
    scenario.record("scenario=PASS completed_move_preserved=true value_preserved=true tombstone_preserved=true original_gateway_client=true reopened_D_required_for_committed_mutations=true")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn four_cli_processes_recover_completed_move_after_kill_and_reopen() {
    // Signal registrations precede every child, including partial startup.
    let mut interrupt = signal(SignalKind::interrupt()).unwrap();
    let mut terminate = signal(SignalKind::terminate()).unwrap();
    let parent =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../reports/task-7-process-recovery");
    fs::create_dir_all(&parent).unwrap();
    let report = tempfile::Builder::new()
        .prefix("completed-")
        .tempdir_in(parent)
        .unwrap()
        .keep();
    eprintln!("PROCESS_RECOVERY_REPORT={}", report.display());
    let receipt = fs::File::create(report.join("receipts.txt")).unwrap();
    // Reserve D before ABC launches; retain its exact endpoints across reopen.
    let peer = PortReservation::bind("127.0.0.1:0").unwrap();
    let data = PortReservation::bind("127.0.0.1:0").unwrap();
    let endpoint = NodeEndpoint {
        node_id: NodeId::new(4).unwrap(),
        peer_address: peer.local_addr().unwrap(),
        data_address: data.local_addr().unwrap(),
    };
    let mut scenario = Scenario {
        cluster: ProcessCluster::start_with_report(Some(report.clone())),
        links: Vec::new(),
        gateway: None,
        client: None,
        receipt,
        deadline: Instant::now() + Duration::from_secs(180),
    };
    for (index, id) in [2, 3, 1].into_iter().enumerate() {
        scenario
            .record(format!(
                "spawn node={id} pid={} mode=create",
                scenario.cluster.nodes[index].child.id()
            ))
            .unwrap();
    }
    drop(peer);
    drop(data);
    let deadline = scenario.deadline;
    // The parent retains the fixture while the assertion task runs. Panics in
    // existing startup helpers cannot bypass explicit client/gateway cleanup.
    let owner = Arc::new(tokio::sync::Mutex::new(scenario));
    let task_owner = Arc::clone(&owner);
    let mut task = tokio::spawn(async move {
        let mut scenario = task_owner.lock().await;
        run(&mut scenario, endpoint).await
    });
    let outcome = tokio::select! {
        outcome = &mut task => Some(outcome),
        _ = interrupt.recv() => None,
        _ = terminate.recv() => None,
        _ = tokio::time::sleep_until(deadline) => None,
    };
    let result = match outcome {
        Some(Ok(result)) => result,
        Some(Err(error)) => Err(format!("assertion task failed: {error}")),
        None => {
            task.abort();
            let _ = task.await;
            Err(
                "parent signal or 180-second deadline; assertion task joined before owned cleanup"
                    .to_owned(),
            )
        }
    };
    let mut scenario = owner.lock().await;
    let _ = scenario.record(format!("scenario_result={result:?}"));
    let cleanup = scenario.cleanup().await;
    let _ = scenario.record(format!("cleanup_result={cleanup:?}"));
    drop(scenario);
    drop(owner); // Reap exact handles and preserve all four logs before DB removal.
    assert!(
        result.is_ok() && cleanup.is_ok(),
        "completed-move process recovery failed: {result:?}; cleanup={cleanup:?}; report={}",
        report.display()
    );
}
