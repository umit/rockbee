//! Two eager public mutations share one reply-loss generation.

use super::{
    Events, TEST_TIMEOUT,
    proxy::ReplyLossProxy,
    resources::{Cleanup, Resources},
    scenario::{ReadObservation, observe_read, start_resources},
};
use bytes::Bytes;
use rockbee_raft_client::{AdmitError, ClientConfig, ReadError, RockBeeRaftClient};
use rockbee_raft_protocol::{FailureReason, MutationOutcome};
use std::sync::Arc;

struct Observation {
    keys: [Bytes; 2],
    values: [Bytes; 2],
    unknown_at: [u64; 2],
    retired_get_error: ReadError,
    fresh_reads: [ReadObservation; 2],
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_eager_public_puts_are_unknown_after_their_committed_replies_are_lost() {
    let _ = env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("rockbee_raft_node=info"),
    )
    .is_test(true)
    .try_init();

    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("install test SIGTERM handler");
    #[cfg(unix)]
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .expect("install test SIGINT handler");
    let interrupted = async {
        #[cfg(unix)]
        tokio::select! {
            _ = terminate.recv() => {},
            _ = interrupt.recv() => {},
        }
        #[cfg(not(unix))]
        std::future::pending::<()>().await;
    };

    let events = Arc::new(Events::default());
    let owner = Arc::new(tokio::sync::Mutex::new(Resources::new()));
    let observer_owner = Arc::clone(&owner);
    let observer_events = Arc::clone(&events);
    let mut observer = tokio::spawn(async move {
        let mut resources = observer_owner.lock().await;
        observe_two_lost_replies(&mut resources, observer_events).await
    });
    let observed = tokio::select! {
        result = &mut observer => Ok(result),
        _ = tokio::time::sleep(TEST_TIMEOUT) => Err("two-flight reply-loss observation deadline expired"),
        _ = interrupted => Err("two-flight reply-loss observation interrupted by signal"),
    };
    let observation = match observed {
        Ok(result) => result
            .map_err(|error| format!("two-flight observation task failed: {error}"))
            .and_then(|result| result),
        Err(reason) => {
            observer.abort();
            let _ = observer.await;
            Err(reason.to_owned())
        }
    };

    let mutex = match Arc::try_unwrap(owner) {
        Ok(mutex) => mutex,
        Err(_) => panic!("observer must release outer resources before cleanup"),
    };
    let mut resources = mutex.into_inner();
    let cleanup = resources.cleanup().await;
    let observation = observation.expect("two-flight reply-loss observation after cleanup");
    assert_cleanup(cleanup, &observation);
}

async fn observe_two_lost_replies(
    resources: &mut Resources,
    events: Arc<Events>,
) -> Result<Observation, String> {
    let gateway_address = start_resources(resources).await?;
    resources.proxy = Some(
        ReplyLossProxy::start_two(gateway_address, Arc::clone(&events))
            .await
            .map_err(|error| error.to_string())?,
    );
    let keys = [
        Bytes::from_static(b"reply-loss/two-in-flight/first"),
        Bytes::from_static(b"reply-loss/two-in-flight/second"),
    ];
    let values = [
        Bytes::from_static(b"first-durable-value-before-public-reply-loss"),
        Bytes::from_static(b"second-durable-value-before-public-reply-loss"),
    ];
    let proxy_address = resources.proxy.as_ref().expect("owned proxy").address;
    resources.original = Some(
        RockBeeRaftClient::connect(proxy_address, ClientConfig::default())
            .await
            .map_err(|error| error.to_string())?,
    );
    let original = resources.original.as_ref().expect("owned original client");
    let first = original
        .put(keys[0].clone(), values[0].clone())
        .map_err(|error| format!("first eager PUT admission: {error}"))?;
    let second = original
        .put(keys[1].clone(), values[1].clone())
        .map_err(|error| format!("second eager PUT admission: {error}"))?;
    let first_observation = async { (first.await, events.next()) };
    let second_observation = async { (second.await, events.next()) };
    let ((first_outcome, first_at), (second_outcome, second_at)) =
        tokio::join!(first_observation, second_observation);
    let unknown_at = [first_at, second_at];
    for outcome in [&first_outcome, &second_outcome] {
        if !matches!(outcome, MutationOutcome::Unknown(failure) if failure.reason == FailureReason::Disconnected)
        {
            return Err(format!(
                "lost public reply must be Unknown(Disconnected), got {outcome:?}"
            ));
        }
    }

    let retired_get = original.get(keys[0].clone()).await;
    if !matches!(&retired_get, Err(ReadError::Admission(AdmitError::Closed))) {
        return Err(format!(
            "retired original client must reject GET before explicit close, got {retired_get:?}"
        ));
    }
    let retired_get_error = retired_get.expect_err("retired original GET is closed");
    original.close().await;

    resources.fresh = Some(
        RockBeeRaftClient::connect(gateway_address, ClientConfig::default())
            .await
            .map_err(|error| error.to_string())?,
    );
    let fresh = resources.fresh.as_ref().expect("owned fresh client");
    let first_read = observe_read(
        fresh,
        keys[0].clone(),
        values[0].clone(),
        Arc::clone(&events),
    )
    .await?;
    let second_read = observe_read(fresh, keys[1].clone(), values[1].clone(), events).await?;
    fresh.close().await;
    Ok(Observation {
        keys,
        values,
        unknown_at,
        retired_get_error,
        fresh_reads: [first_read, second_read],
    })
}

fn assert_cleanup(cleanup: Cleanup, observation: &Observation) {
    assert!(
        cleanup.pending_node_joined && cleanup.pending_gateway_joined,
        "owned startup handles must join during cleanup"
    );
    assert!(
        cleanup.node_startup_failure.is_none() && cleanup.gateway_startup_failure.is_none(),
        "happy path has no consumed startup failure"
    );
    assert!(
        matches!(
            &observation.retired_get_error,
            ReadError::Admission(AdmitError::Closed)
        ),
        "retired client GET must fail local closed admission before explicit close"
    );
    assert_eq!(
        cleanup.client_write_calls, 2,
        "exactly two actual KV client_write calls may occur across ABC"
    );
    let receipt = cleanup
        .proxy
        .expect("join two-flight reply-loss proxy after scenario");
    assert_eq!(
        receipt.accepted_connections, 1,
        "both eager PUTs must use one original public connection"
    );
    assert_eq!(
        receipt.forwarded_put_frames, 2,
        "gateway must receive exactly two target public PUT frames"
    );
    assert_eq!(
        receipt.extra_original_frames, 0,
        "the original generation must send no additional request bytes"
    );
    assert!(
        receipt.original_eof_observed,
        "proxy must drain the original generation to EOF"
    );
    assert!(
        receipt.suppressed_response_bytes > 0,
        "proxy must suppress two complete committed response frames"
    );
    let matched = receipt.matched_puts;
    assert!(
        matched.iter().all(Option::is_some),
        "both response correlation slots must contain a real committed reply"
    );
    let first = matched[0].as_ref().expect("first matched reply");
    let second = matched[1].as_ref().expect("second matched reply");
    assert_ne!(
        first.request_id, second.request_id,
        "the two matched replies must retain distinct nonzero request ids"
    );
    assert!(
        first.request_id.get() != 0 && second.request_id.get() != 0,
        "matched response ids must be nonzero"
    );
    assert_eq!(first.key.as_slice(), observation.keys[0].as_ref());
    assert_eq!(first.value.as_slice(), observation.values[0].as_ref());
    assert_eq!(second.key.as_slice(), observation.keys[1].as_ref());
    assert_eq!(second.value.as_slice(), observation.values[1].as_ref());
    assert!(
        observation
            .unknown_at
            .iter()
            .all(|at| first.committed_at < *at),
        "first actual Committed observation must precede both client Unknown notices"
    );
    assert!(
        observation
            .unknown_at
            .iter()
            .all(|at| second.committed_at < *at),
        "second actual Committed observation must precede both client Unknown notices"
    );
    assert!(observation.fresh_reads[0].get_value_at > observation.fresh_reads[0].get_invoked_at);
    assert!(observation.fresh_reads[1].get_value_at > observation.fresh_reads[1].get_invoked_at);
    log::info!(
        "raft_multiflight_reply_loss_complete accepted_connections={} forwarded_puts={} matched_committed=2 suppressed_response_bytes={} client_write_calls={} original_eof=true retired_get=closed fresh_gets=2 request_ids={}:{} committed_ordinals={}:{} unknown_ordinals={}:{} get_ordinals={}:{}:{}:{}",
        receipt.accepted_connections,
        receipt.forwarded_put_frames,
        receipt.suppressed_response_bytes,
        cleanup.client_write_calls,
        first.request_id.get(),
        second.request_id.get(),
        first.committed_at,
        second.committed_at,
        observation.unknown_at[0],
        observation.unknown_at[1],
        observation.fresh_reads[0].get_invoked_at,
        observation.fresh_reads[0].get_value_at,
        observation.fresh_reads[1].get_invoked_at,
        observation.fresh_reads[1].get_value_at,
    );
}
