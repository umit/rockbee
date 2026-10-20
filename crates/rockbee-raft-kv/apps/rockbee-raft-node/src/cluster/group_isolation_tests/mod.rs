//! Default-budget public-path isolation during one group's receiver RPC cut.
//!
//! The cut rejects decoded peer calls immediately before native dispatch. It
//! does not classify a shared TCP connection by its first frame's group ID.

#![allow(clippy::unwrap_used, clippy::panic)]

mod fatal;
mod resources;

use super::{NodeRuntime, service::authority_barrier};
use crate::rpc::test_support::PeerRelays;
use bytes::Bytes;
use resources::Resources;
use rockbee_raft_client::{ClientConfig, RockBeeRaftClient};
use rockbee_raft_gateway::GatewayRuntime;
use rockbee_raft_protocol::{
    FailureReason, GroupId, KvRequest, KvResponse, MAX_FRAME_PAYLOAD, MutationOutcome, WireRequest,
    backend::{BackendOperation, BackendReply, BackendRequest},
    routing::ClusterDirectory,
    transport::server::{RequestHandler, ServerAdmissionSnapshot, ServerConfig},
};
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::Mutex,
    time::{Instant, sleep},
};

const STALLED: GroupId = GroupId::new(1);
const HEALTHY: GroupId = GroupId::new(2);
const OBSERVATION_LIMIT: Duration = Duration::from_secs(45);
const VALUE: &[u8] = b"healthy group remains available";

fn message(error: impl std::fmt::Display) -> String {
    error.to_string()
}

async fn authority(node: &NodeRuntime, group: GroupId) -> Result<(), String> {
    authority_barrier(
        group,
        &node.service.groups[&group],
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("authority for group {}: {error:?}", group.get()))
}

fn key(directory: &ClusterDirectory, group: GroupId, ordinal: usize) -> Bytes {
    (0..10_000)
        .map(|number| Bytes::from(format!("group-isolation/{number}")))
        .filter(|key| directory.shard_config.group_for_key(key).unwrap() == group)
        .nth(ordinal)
        .expect("bounded deterministic key search")
}

fn used_operations(snapshot: ServerAdmissionSnapshot) -> usize {
    snapshot.operations_configured - snapshot.operations_available
}

fn used_responses(snapshot: ServerAdmissionSnapshot) -> usize {
    snapshot.response_bytes_configured - snapshot.response_bytes_available
}

async fn wait_for(label: &str, predicate: impl Fn() -> bool) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !predicate() {
        if Instant::now() >= deadline {
            return Err(format!("timed out waiting for {label}"));
        }
        sleep(Duration::from_millis(5)).await;
    }
    Ok(())
}

async fn healthy_crud(client: &RockBeeRaftClient, key: Bytes) -> Result<(), String> {
    let result = client
        .put(key.clone(), Bytes::from_static(VALUE))
        .map_err(message)?
        .await;
    if result != MutationOutcome::Committed {
        return Err(format!("healthy PUT: {result:?}"));
    }
    let value = client.get(key).await.map_err(message)?;
    if value != Some(Bytes::from_static(VALUE)) {
        return Err(format!("healthy GET: {value:?}"));
    }
    Ok(())
}

struct Observation {
    healthy_under_pressure: Result<(), String>,
    retained: usize,
}

async fn observe(resources: &mut Resources) -> Result<Observation, String> {
    let original = resources.client().await?;
    let healthy_key = key(&resources.directory, HEALTHY, 0);
    healthy_crud(&resources.clients[original], healthy_key.clone()).await?;
    let installed = resources.relays.install_group_cut(STALLED);
    let drained = resources.relays.drain_group_cut(&installed).await;
    let tracker = resources.relays.dispatch_tracker();
    let before = tracker.group_counts(STALLED);
    if drained.earlier_dispatches != 0 {
        return Err("group cut did not drain".into());
    }
    let reservation = 2 * (MAX_FRAME_PAYLOAD + 4)
        + <BackendRequest as WireRequest<rockbee_raft_protocol::backend::BackendResponse>>::RESPONSE_DECODE_HEADROOM;
    let submitted = ServerConfig::default().response_bytes / reservation;
    let mut handles = Vec::new();
    handles.push(
        resources.clients[original]
            .put(
                key(&resources.directory, STALLED, 0),
                Bytes::from_static(b"pending quorum"),
            )
            .map_err(message)?,
    );
    wait_for("first actual native proposal", || {
        resources.nodes[0].service.test_proposals(STALLED).active == 1
    })
    .await?;
    healthy_crud(&resources.clients[original], healthy_key.clone()).await?;
    authority(&resources.nodes[0], GroupId::new(0)).await?;
    wait_for("low-load completed responses released", || {
        used_responses(resources.nodes[0].data_server.admission_snapshot()) == reservation
            && used_operations(resources.gateway.as_ref().unwrap().admission_snapshot()) == 1
    })
    .await?;
    log::info!(
        "raft_group_isolation_low_load cut_generation={} cut_group={} pending=1 sibling_public=committed_and_read metadata_authority=ok",
        drained.generation,
        drained.group_id.get()
    );
    for ordinal in 1..submitted {
        handles.push(
            resources.clients[original]
                .put(
                    key(&resources.directory, STALLED, ordinal),
                    Bytes::from_static(b"pending quorum"),
                )
                .map_err(message)?,
        );
    }
    let mut retained = 0;
    let mut rejected = 0;
    let mut rejected_keys = Vec::new();
    for (ordinal, handle) in handles.into_iter().enumerate() {
        let outcome = handle.await;
        match outcome {
            MutationOutcome::Unknown(_) => retained += 1,
            MutationOutcome::Rejected(ref failure)
                if failure.reason == FailureReason::Overloaded =>
            {
                rejected += 1;
                rejected_keys.push(key(&resources.directory, STALLED, ordinal));
            }
            _ => {
                return Err(format!(
                    "unavailable-group PUT must be Unknown or rejected before submission: {outcome:?}"
                ));
            }
        }
    }
    resources.clients[original].close().await;
    let gateway = resources.gateway.as_ref().unwrap();
    wait_for(
        "gateway handlers and response reservations released",
        || {
            let snapshot = gateway.admission_snapshot();
            used_operations(snapshot) == 0 && used_responses(snapshot) == 0
        },
    )
    .await?;
    let held = resources.nodes[0].service.test_proposals(STALLED);
    let node = resources.nodes[0].data_server.admission_snapshot();
    if retained == 0
        || held.entered != retained
        || held.active != retained
        || held.completed != 0
        || held.dropped_before_completion != 0
        || used_operations(node) != retained
        || used_responses(node) != retained * reservation
    {
        return Err(format!(
            "native work or node reservations were not retained: {held:?} {node:?}"
        ));
    }
    // Bypass only the data-admission layer to diagnose actual sibling quorum.
    // This is explicitly not counted as public success.
    let direct = resources.nodes[0]
        .service
        .handle(
            BackendRequest {
                cluster_id: resources.directory.cluster_id,
                target_node: resources.directory.nodes[0].node_id,
                operation: BackendOperation::Kv {
                    group_id: HEALTHY,
                    request: KvRequest::Get {
                        key: healthy_key.clone(),
                    },
                },
            },
            Instant::now() + Duration::from_secs(5),
        )
        .await;
    if !matches!(direct.reply, BackendReply::Kv { response: KvResponse::Value(Some(ref value)), .. } if value.as_ref() == VALUE)
    {
        return Err(format!(
            "sibling actual strong-read diagnostic failed: {direct:?}"
        ));
    }
    authority(&resources.nodes[0], GroupId::new(0)).await?;
    let fresh = resources.client().await?;
    let healthy_under_pressure = healthy_crud(&resources.clients[fresh], healthy_key.clone()).await;
    let after = tracker.group_counts(STALLED);
    let native_after = resources.nodes[0].service.test_proposals(STALLED);
    if after.admitted != before.admitted
        || after.rejected <= before.rejected
        || native_after != held
    {
        return Err(format!(
            "cut or retained native ownership changed during probe: before={before:?} after={after:?} native={native_after:?}"
        ));
    }
    log::info!(
        "raft_group_isolation_pressure submitted={submitted} retained={retained} rejected={rejected} node_response_bytes={} gateway_operations=0 sibling_quorum=ok metadata_quorum=ok public_result={healthy_under_pressure:?}",
        used_responses(node)
    );
    resources.heal();
    wait_for("actual native completion after heal", || {
        resources.nodes[0].service.test_proposals(STALLED).active == 0
    })
    .await?;
    let finished = resources.nodes[0].service.test_proposals(STALLED);
    if finished.completed != retained || finished.dropped_before_completion != 0 {
        return Err(format!("native completion lost after heal: {finished:?}"));
    }
    wait_for("node response reservations returned after heal", || {
        used_responses(resources.nodes[0].data_server.admission_snapshot()) == 0
    })
    .await?;
    let recovered = resources.client().await?;
    for rejected_key in rejected_keys {
        if resources.clients[recovered]
            .get(rejected_key)
            .await
            .map_err(message)?
            .is_some()
        {
            return Err("an explicitly rejected write appeared after healing".into());
        }
    }
    healthy_crud(&resources.clients[recovered], healthy_key).await?;
    healthy_crud(
        &resources.clients[recovered],
        key(&resources.directory, STALLED, submitted),
    )
    .await?;
    log::info!(
        "raft_group_isolation_healed native_completed={} dropped=0 healthy_public=ok target_public=ok",
        finished.completed
    );
    Ok(Observation {
        healthy_under_pressure,
        retained,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unavailable_group_does_not_starve_healthy_group_after_callers_leave() {
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
    // Constructors are never aborted. On interruption their owned result is
    // still joined and closed before the test reports failure.
    let (resources, listeners, public) =
        Resources::prepare().await.expect("prepare owned listeners");
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
    let observation = result.expect("group isolation observation after joined cleanup");
    assert!(
        observation.healthy_under_pressure.is_ok(),
        "unavailable group retained {} native writes and starved healthy public work after callers left: {:?}",
        observation.retained,
        observation.healthy_under_pressure
    );
}
