//! Real public-hop response loss over the normal three-node runtime.
//!
//! The scenario owns a one-request proxy rather than modifying production
//! transport. It proves that a real `Committed` response existed before the
//! public socket closes without forwarding response bytes.

#![allow(clippy::panic, clippy::unwrap_used)]

mod backend;
mod backend_proxy;
mod capture;
mod multiflight;
mod proxy;
mod resources;
mod scenario;

use super::history_model as history;
use capture::{CapturedHistory, check_persisted_history};
use history::{KeyWitness, Verdict};
use resources::Resources;
use scenario::run_observation;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_CAPTURE_BYTES: usize = 64 * 1024;

/// A monotonic fixture-only order, never a wall-clock timestamp.
#[derive(Default)]
pub(super) struct Events(AtomicU64);

impl Events {
    pub(super) fn next(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Relaxed) + 1
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_public_response_loss_is_unknown_without_replay_and_has_a_legal_history() {
    let _ = env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("rockbee_raft_node=info"),
    )
    .is_test(true)
    .try_init();

    // Install interruption handling before creating any runtime. Only the
    // observer is canceled; cleanup retains and joins native startup work.
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
    // Retain only the small diagnostic capture. Database directories remain
    // temporary and are released after the runtimes have closed.
    let report = tempfile::Builder::new()
        .prefix("rockbee-public-reply-loss-")
        .tempdir()
        .expect("create unique reply-loss report directory")
        .keep();
    let capture_path = report.join("history.bin");
    let events = Arc::new(Events::default());
    // The bounded observer is disposable; all real process and connection
    // owners live in this outer mutex until cleanup has completed.
    let owner = Arc::new(tokio::sync::Mutex::new(Resources::new()));
    let observer_owner = Arc::clone(&owner);
    let observer_events = Arc::clone(&events);
    let mut observer = tokio::spawn(async move {
        let mut resources = observer_owner.lock().await;
        run_observation(&mut resources, observer_events).await
    });
    let observed = tokio::select! {
        result = &mut observer => Ok(result),
        _ = tokio::time::sleep(TEST_TIMEOUT) => Err("reply-loss observation deadline expired"),
        _ = interrupted => Err("reply-loss observation interrupted by signal"),
    };
    let observation = match observed {
        Ok(result) => result
            .map_err(|error| format!("observation task failed: {error}"))
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
    let observation = observation.expect("reply-loss observation after cleanup");

    assert!(
        cleanup.pending_node_joined && cleanup.pending_gateway_joined,
        "owned startup handles must join during cleanup"
    );
    assert!(
        cleanup.node_startup_failure.is_none() && cleanup.gateway_startup_failure.is_none(),
        "happy path has no consumed startup failure"
    );
    let proxy_receipt = cleanup.proxy.expect("join reply-loss proxy after scenario");
    let committed_at = proxy_receipt
        .committed_at
        .expect("proxy must observe real Committed before induced close");
    assert!(
        committed_at < observation.mutation.unknown_at,
        "actual Committed observation must precede the client Unknown notice"
    );
    assert_eq!(
        proxy_receipt.accepted_connections, 1,
        "retired client must not reconnect"
    );
    assert_eq!(
        proxy_receipt.forwarded_put_frames, 1,
        "gateway must receive exactly one public PUT frame"
    );
    assert!(
        proxy_receipt.suppressed_response_bytes > 0,
        "proxy must consume a complete response frame"
    );
    assert_eq!(
        proxy_receipt.extra_original_frames, 0,
        "the original generation must send no second complete request"
    );
    assert!(
        proxy_receipt.original_eof_observed,
        "proxy must drain the original generation to EOF before success"
    );
    assert_eq!(
        cleanup.client_write_calls, 1,
        "exactly one actual KV client_write may occur across ABC"
    );

    let horizon = events.next();
    match check_persisted_history(
        &capture_path,
        &CapturedHistory {
            key: observation.key.clone(),
            value: observation.value.clone(),
            put_invoked_at: observation.mutation.put_invoked_at,
            committed_at,
            unknown_at: observation.mutation.unknown_at,
            retired_get_invoked_at: observation.retired_get_invoked_at,
            retired_get_error_at: observation.retired_get_error_at,
            get_invoked_at: observation.fresh_read.get_invoked_at,
            get_value_at: observation.fresh_read.get_value_at,
            horizon,
        },
    ) {
        Verdict::Legal { witnesses, .. } => assert_eq!(
            witnesses,
            vec![KeyWitness {
                key: observation.key.to_vec(),
                linearized: vec![1, 3],
                omitted: vec![],
            }],
            "committed effect must precede the new-client GET"
        ),
        other => panic!("captured public reply-loss history must be legal: {other:?}"),
    }
    log::info!(
        "raft_reply_loss_complete accepted_connections={} forwarded_puts={} suppressed_response_bytes={} client_write_calls={} history=legal history_path={} event_ordinals=put:{} committed:{} unknown:{} retired_get:{}:{} fresh_get:{}:{} horizon={}",
        proxy_receipt.accepted_connections,
        proxy_receipt.forwarded_put_frames,
        proxy_receipt.suppressed_response_bytes,
        cleanup.client_write_calls,
        capture_path.display(),
        observation.mutation.put_invoked_at,
        committed_at,
        observation.mutation.unknown_at,
        observation.retired_get_invoked_at,
        observation.retired_get_error_at,
        observation.fresh_read.get_invoked_at,
        observation.fresh_read.get_value_at,
        horizon,
    );
}
