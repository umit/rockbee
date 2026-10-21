//! Eight same-group public mutations must reach native Raft before a cut heals.

use super::{OBSERVATION_LIMIT, Resources, STALLED, key, message, used_operations, used_responses};
use bytes::Bytes;
use rockbee_raft_protocol::MutationOutcome;
use tokio::time::{Duration, Instant, sleep};

const PROPOSALS: usize = 8;
const OBSERVE_TIMEOUT: Duration = Duration::from_secs(2);

async fn wait_for(label: &str, predicate: impl Fn() -> bool) -> Result<(), String> {
    let deadline = Instant::now() + OBSERVE_TIMEOUT;
    while !predicate() {
        if Instant::now() >= deadline {
            return Err(format!("timed out waiting for {label}"));
        }
        sleep(Duration::from_millis(5)).await;
    }
    Ok(())
}

async fn observe(resources: &mut Resources) -> Result<(), String> {
    let client = resources.client().await?;
    let mut writes = Vec::with_capacity(PROPOSALS);
    for ordinal in 0..PROPOSALS {
        let key = key(&resources.directory, STALLED, ordinal);
        let value = Bytes::from(format!("eight-native-proposals/{ordinal}"));
        writes.push((key, value));
    }

    let installed = resources.relays.install_group_cut(STALLED);
    let drained = resources.relays.drain_group_cut(&installed).await;
    if drained.earlier_dispatches != 0 {
        return Err("group cut did not drain".into());
    }

    let mut handles = Vec::with_capacity(PROPOSALS);
    for (key, value) in &writes {
        handles.push(
            resources.clients[client]
                .put(key.clone(), value.clone())
                .map_err(message)?,
        );
    }
    wait_for("eight native proposals held by the group cut", || {
        let proposals = resources.nodes[0].service.test_proposals(STALLED);
        proposals.entered == PROPOSALS && proposals.active == PROPOSALS
    })
    .await?;

    // Restore delivery without re-enabling this fixture's suspended G1
    // elections. An immediate follower election would test leader failover
    // instead of completing eight writes on the original leader.
    resources.relays.heal();
    for handle in handles {
        let outcome = handle.await;
        if outcome != MutationOutcome::Committed {
            return Err(format!("healed same-group PUT must commit: {outcome:?}"));
        }
    }
    for (key, value) in writes {
        let observed = resources.clients[client].get(key).await.map_err(message)?;
        if observed != Some(value) {
            return Err(format!("healed same-group GET returned {observed:?}"));
        }
    }

    let proposals = resources.nodes[0].service.test_proposals(STALLED);
    if proposals.entered != PROPOSALS
        || proposals.completed != PROPOSALS
        || proposals.active != 0
        || proposals.dropped_before_completion != 0
    {
        return Err(format!(
            "unexpected native proposal lifecycle: {proposals:?}"
        ));
    }
    wait_for("gateway and node admission released", || {
        let gateway = resources
            .gateway
            .as_ref()
            .expect("started gateway")
            .admission_snapshot();
        used_operations(gateway) == 0
            && used_responses(gateway) == 0
            && gateway.request_bytes_available == gateway.request_bytes_configured
            && resources.nodes.iter().all(|node| {
                let snapshot = node.data_server.admission_snapshot();
                used_operations(snapshot) == 0
                    && used_responses(snapshot) == 0
                    && snapshot.request_bytes_available == snapshot.request_bytes_configured
            })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eight_same_group_puts_enter_native_raft_before_heal() {
    let (mut resources, listeners, public) =
        Resources::prepare().await.expect("prepare owned listeners");
    let result = match resources.start_nodes(listeners, public).await {
        Ok(()) => tokio::time::timeout(OBSERVATION_LIMIT, observe(&mut resources))
            .await
            .map_err(|_| "observation deadline expired".to_owned())
            .and_then(|result| result),
        Err(error) => Err(error),
    };
    resources.heal();
    resources.close().await;
    result.expect("eight same-group proposals after owned cleanup");
}
