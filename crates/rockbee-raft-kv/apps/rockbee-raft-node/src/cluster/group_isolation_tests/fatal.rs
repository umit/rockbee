//! An injected append persistence error drives actual OpenRaft Fatal handling.

use super::{
    HEALTHY, OBSERVATION_LIMIT, Resources, STALLED, authority, healthy_crud, key, message, wait_for,
};
use bytes::Bytes;
use rockbee_raft_protocol::{FailureReason, GroupId, MutationOutcome};
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::Mutex,
    time::{Instant, sleep},
};

const CLOSED_GATE_MESSAGE: &str = "group native mutation admission is closed";

async fn elect_b(resources: &Resources) -> Result<(), String> {
    let node = &resources.nodes[1];
    let group = &node.service.groups[&STALLED];
    // Only B starts elections. C still votes; automatic retries allow A's old
    // leader lease to expire without treating an elapsed sleep as readiness.
    group.raft.runtime_config().elect(true);
    group.raft.trigger().elect().await.map_err(message)?;
    wait_for("B observes itself as G1 leader", || {
        node.service.leader_hint(STALLED) == Some(node.service.identity.node_id)
    })
    .await?;
    let applied = super::authority_barrier(STALLED, group, Instant::now() + Duration::from_secs(5))
        .await
        .map_err(|error| format!("B G1 authority after A Fatal: {error:?}"))?;
    log::info!(
        "raft_fatal_failover_ready group={} leader={} authority_applied={applied:?} directory_revision={} candidates={:?}",
        STALLED.get(),
        node.service.identity.node_id.get(),
        resources.directory.revision,
        resources.directory.voters(STALLED),
    );
    Ok(())
}

async fn verify_new_mutation_failover(resources: &Resources, client: usize) -> Result<(), String> {
    elect_b(resources).await?;
    let a = &resources.nodes[0];
    if resources
        .directory
        .voters(STALLED)
        .and_then(|voters| voters.first())
        != Some(&a.service.identity.node_id)
        || !a.service.groups[&STALLED].mutation_admission.is_closed()
    {
        return Err("failover requires unchanged A-first routing and A's closed G1 gate".into());
    }
    let before = [0, 1, 2].map(|index| resources.nodes[index].service.test_proposals(STALLED));
    if before[0].entered != 1 || before[1].entered != 0 || before[2].entered != 0 {
        return Err(format!("original Unknown must not be replayed: {before:?}"));
    }
    let new_key = key(&resources.directory, STALLED, 2);
    let new_value = Bytes::from_static(b"new-mutation-after-local-fatal");
    log::info!("raft_fatal_failover_before proposals={before:?}");
    let outcome = resources.clients[client]
        .put(new_key.clone(), new_value.clone())
        .map_err(message)?
        .await;
    let read = resources.clients[client].get(new_key).await;
    let after = [0, 1, 2].map(|index| resources.nodes[index].service.test_proposals(STALLED));
    // Preserve the actual failing outcome and counters before returning through
    // the existing outer owner, which closes every resource before asserting.
    log::info!(
        "raft_fatal_failover_observed outcome={outcome:?} read={read:?} before={before:?} after={after:?} a_gate_closed={}",
        a.service.groups[&STALLED].mutation_admission.is_closed(),
    );
    authority(a, GroupId::new(0)).await?;
    authority(a, HEALTHY).await?;
    healthy_crud(
        &resources.clients[client],
        key(&resources.directory, HEALTHY, 3),
    )
    .await?;
    if outcome != MutationOutcome::Committed {
        return Err(format!(
            "new public PUT must reach authoritative B after A's safe refusal: {outcome:?}"
        ));
    }
    if read.map_err(message)? != Some(new_value) {
        return Err("same public client did not read B's committed value".into());
    }
    for index in 0..3 {
        let expected = usize::from(index == 1);
        if after[index].entered != before[index].entered + expected
            || after[index].completed != before[index].completed + expected
            || after[index].active != 0
            || after[index].dropped_before_completion != 0
        {
            return Err(format!(
                "failover requires exact native deltas A0/B1/C0: before={before:?} after={after:?}"
            ));
        }
    }
    if !a.service.groups[&STALLED].mutation_admission.is_closed() {
        return Err("A's failed G1 admission reopened during failover".into());
    }
    Ok(())
}

async fn observe(resources: &mut Resources) -> Result<(), String> {
    let original = resources.client().await?;
    let healthy_key = key(&resources.directory, HEALTHY, 1);
    healthy_crud(&resources.clients[original], healthy_key).await?;
    authority(&resources.nodes[0], STALLED).await?;

    let initial_capacity = resources.nodes[0].service.groups[&STALLED]
        .mutation_admission
        .available_permits();
    if initial_capacity == 0 {
        return Err("fatal fixture group has no initial mutation capacity".into());
    }

    let fatal_key = key(&resources.directory, STALLED, 0);
    let fatal_value = Bytes::from_static(b"injected-fatal-append");
    let append_failure = resources.nodes[0].service.groups[&STALLED]
        .reader
        .fail_append_for_test(fatal_key.clone(), fatal_value.clone());
    let outcome = resources.clients[original]
        .put(fatal_key, fatal_value)
        .map_err(message)?
        .await;
    if !matches!(outcome, MutationOutcome::Unknown(ref failure) if failure.reason == FailureReason::Unavailable)
    {
        return Err(format!(
            "injected native Fatal must preserve Unknown(Unavailable), got {outcome:?}"
        ));
    }
    wait_for("targeted append failure receipt", || {
        append_failure
            .receipt()
            .is_some_and(|receipt| receipt.persist_failed && receipt.callback_failed)
    })
    .await?;
    let receipt = append_failure
        .receipt()
        .expect("completed append failure receipt");
    if receipt.group_id != STALLED
        || receipt.log_id.index == 0
        || !receipt.persist_failed
        || !receipt.callback_failed
    {
        return Err(format!("unexpected targeted append receipt: {receipt:?}"));
    }

    let fatal = resources.nodes[0]
        .service
        .test_fatal_admission(STALLED)
        .ok_or_else(|| "Fatal admission snapshot was not recorded".to_owned())?;
    if !fatal.closed
        || fatal.available != initial_capacity - 1
        || fatal.native_active != 1
        || !fatal.error.contains("injected durable write failure")
    {
        return Err(format!(
            "Fatal gate state must precede permit release: initial_capacity={initial_capacity} snapshot={fatal:?}"
        ));
    }
    let finished = resources.nodes[0].service.test_proposals(STALLED);
    if finished.entered != 1
        || finished.completed != 1
        || finished.active != 0
        || finished.dropped_before_completion != 0
    {
        return Err(format!("unexpected fatal proposal ownership: {finished:?}"));
    }

    let before_rejection: Vec<_> = resources
        .nodes
        .iter()
        .map(|node| node.service.test_client_write_calls())
        .collect();
    let fresh = resources.client().await?;
    let rejected_key = key(&resources.directory, STALLED, 1);
    let rejected = resources.clients[fresh]
        .put(
            rejected_key.clone(),
            Bytes::from_static(b"must-not-enter-closed-group"),
        )
        .map_err(message)?
        .await;
    match rejected {
        MutationOutcome::Rejected(failure)
            if failure.reason == FailureReason::Unavailable
                && failure.message == CLOSED_GATE_MESSAGE => {}
        other => {
            return Err(format!(
                "closed group must explicitly reject before native submission, got {other:?}"
            ));
        }
    }
    let after_rejection: Vec<_> = resources
        .nodes
        .iter()
        .map(|node| node.service.test_client_write_calls())
        .collect();
    if after_rejection != before_rejection {
        return Err(format!(
            "closed-gate rejection entered native Raft work: before={before_rejection:?} after={after_rejection:?}"
        ));
    }
    for node in &resources.nodes {
        let local = node.service.groups[&STALLED]
            .reader
            .read_local(rejected_key.clone())
            .await
            .map_err(message)?;
        if local.value.is_some() {
            return Err(format!(
                "closed-gate rejected key appeared in local state on node {}: {:?}",
                node.service.identity.node_id.get(),
                local.value
            ));
        }
    }

    healthy_crud(
        &resources.clients[original],
        key(&resources.directory, HEALTHY, 2),
    )
    .await?;
    authority(&resources.nodes[0], GroupId::new(0)).await?;
    log::info!(
        "raft_fatal_admission_verified group={} log_id={:?} persist_failed={} callback_failed={} closed={} available={} capacity={} active_at_close={} completed={} active_after={} dropped={} rejected_without_submission=true rejected_key_absent_nodes={} sibling_crud=true metadata_authority=true",
        receipt.group_id.get(),
        receipt.log_id,
        receipt.persist_failed,
        receipt.callback_failed,
        fatal.closed,
        fatal.available,
        initial_capacity,
        fatal.native_active,
        finished.completed,
        finished.active,
        finished.dropped_before_completion,
        resources.nodes.len(),
    );
    verify_new_mutation_failover(resources, original).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_fatal_closes_only_its_group_admission() {
    let _ = env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("rockbee_raft_node=info"),
    )
    .is_test(true)
    .try_init();
    #[cfg(unix)]
    let mut terminate =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
    #[cfg(unix)]
    let mut interrupt =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).unwrap();
    let interrupted = async {
        #[cfg(unix)]
        tokio::select! { _ = terminate.recv() => {}, _ = interrupt.recv() => {} }
        #[cfg(not(unix))]
        std::future::pending::<()>().await;
    };
    tokio::pin!(interrupted);

    // Native constructors and their accepted work remain owned outside the
    // disposable observer, so timeout/signal cleanup never aborts them.
    let (resources, listeners, public) = Resources::prepare()
        .await
        .expect("prepare owned fixture listeners");
    let owner = Arc::new(Mutex::new(resources));
    let starting_owner = Arc::clone(&owner);
    let mut startup = tokio::spawn(async move {
        starting_owner
            .lock()
            .await
            .start_nodes(listeners, public)
            .await
    });
    let starting = tokio::select! {
        result = &mut startup => Some(result),
        _ = &mut interrupted => None,
        _ = sleep(OBSERVATION_LIMIT) => None,
    };
    let startup_result = match starting {
        Some(result) => result.map_err(message).and_then(|result| result),
        None => {
            let _ = startup.await;
            Err("startup interrupted; retained constructor joined".into())
        }
    };
    if let Err(error) = startup_result {
        owner.lock().await.close().await;
        panic!("startup failed after owned cleanup: {error}");
    }

    let observing = Arc::clone(&owner);
    let mut observer = tokio::spawn(async move { observe(&mut *observing.lock().await).await });
    let result = tokio::select! {
        result = &mut observer => result.map_err(message).and_then(|result| result),
        _ = &mut interrupted => Err("observation interrupted".into()),
        _ = sleep(OBSERVATION_LIMIT) => Err("observation deadline expired".into()),
    };
    if !observer.is_finished() {
        observer.abort();
        let _ = observer.await;
    }
    let mut resources = Arc::try_unwrap(owner)
        .unwrap_or_else(|_| panic!("observer released resources"))
        .into_inner();
    resources.close().await;
    result.expect("fatal group observation after owned cleanup");
}
