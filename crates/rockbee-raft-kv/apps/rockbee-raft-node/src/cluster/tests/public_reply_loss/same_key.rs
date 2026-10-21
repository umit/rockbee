//! Real overlapping writes to one key, lost replies, then a fresh overwrite.

use super::{
    Events, TEST_TIMEOUT, history,
    proxy::ReplyLossProxy,
    resources::{Cleanup, Resources},
    same_key_capture::{Capture, Operation, Outcome},
    scenario::start_resources,
};
use bytes::Bytes;
use rockbee_raft_client::{AdmitError, ClientConfig, MutationHandle, ReadError, RockBeeRaftClient};
use rockbee_raft_protocol::{FailureReason, MutationOutcome};
use std::{
    path::Path,
    sync::{Arc, Mutex},
};

struct PublicCalls {
    key: Bytes,
    capture: Arc<Mutex<Capture>>,
    events: Arc<Events>,
}

impl PublicCalls {
    /// Record before invoking the eager API, even if local admission fails.
    fn put(
        &self,
        client: &RockBeeRaftClient,
        value: Bytes,
    ) -> Result<(usize, MutationHandle), String> {
        let slot = self
            .capture
            .lock()
            .unwrap()
            .begin(Operation::Put(value.clone()), self.events.next())?;
        match client.put(self.key.clone(), value) {
            Ok(handle) => Ok((slot, handle)),
            Err(error) => {
                self.capture.lock().unwrap().notice(
                    slot,
                    Outcome::AdmissionError(error.to_string()),
                    self.events.next(),
                )?;
                Err(format!("eager PUT admission: {error}"))
            }
        }
    }

    async fn mutation(&self, pending: (usize, MutationHandle)) -> Result<MutationOutcome, String> {
        let (slot, handle) = pending;
        let outcome = handle.await;
        self.capture.lock().unwrap().notice(
            slot,
            Outcome::Mutation(outcome.clone()),
            self.events.next(),
        )?;
        Ok(outcome)
    }

    async fn get(
        &self,
        client: &RockBeeRaftClient,
    ) -> Result<Result<Option<Bytes>, ReadError>, String> {
        let slot = self
            .capture
            .lock()
            .unwrap()
            .begin(Operation::Get, self.events.next())?;
        let result = client.get(self.key.clone()).await;
        let outcome = match &result {
            Ok(value) => Outcome::Value(value.clone()),
            Err(error) => Outcome::ReadError(error.to_string()),
        };
        self.capture
            .lock()
            .unwrap()
            .notice(slot, outcome, self.events.next())?;
        Ok(result)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overlapping_same_key_puts_have_a_legal_history_after_lost_replies_and_overwrite() {
    let _ = env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("rockbee_raft_node=info"),
    )
    .is_test(true)
    .try_init();
    // Install signals before any runtime startup. Only the observer can abort.
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("install SIGTERM handler");
    #[cfg(unix)]
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .expect("install SIGINT handler");
    let interrupted = async {
        #[cfg(unix)]
        tokio::select! { _ = terminate.recv() => {}, _ = interrupt.recv() => {} }
        #[cfg(not(unix))]
        std::future::pending::<()>().await;
    };
    let report = tempfile::Builder::new()
        .prefix("rockbee-same-key-history-")
        .tempdir()
        .expect("unique report directory")
        .keep();
    let path = report.join("history.bin");
    let key = Bytes::from_static(b"reply-loss/same-key/overlap");
    let events = Arc::new(Events::default());
    // Keep both native resource owners and every captured prefix outside the
    // disposable observation task. Cleanup joins constructors and accepted work.
    let owner = Arc::new(tokio::sync::Mutex::new(Resources::new()));
    let captured = Arc::new(Mutex::new(Capture::new(key.clone())));
    let calls = PublicCalls {
        key,
        capture: Arc::clone(&captured),
        events: Arc::clone(&events),
    };
    let observer_owner = Arc::clone(&owner);
    let mut observer = tokio::spawn(async move {
        let mut resources = observer_owner.lock().await;
        observe(&mut resources, calls).await
    });
    let observed = tokio::select! {
        result = &mut observer => Ok(result),
        _ = tokio::time::sleep(TEST_TIMEOUT) => Err("same-key observation deadline expired"),
        _ = interrupted => Err("same-key observation interrupted by signal"),
    };
    let (observation, was_interrupted) = match observed {
        Ok(result) => completed_observer(result),
        Err(reason) => {
            observer.abort();
            let _ = observer.await;
            (Err(reason.to_owned()), true)
        }
    };
    let mutex = match Arc::try_unwrap(owner) {
        Ok(mutex) => mutex,
        Err(_) => panic!("observer must release outer resource ownership"),
    };
    let mut resources = mutex.into_inner();
    let cleanup = resources.cleanup().await;
    let reloaded = persist_capture(
        &captured,
        &cleanup,
        &observation,
        was_interrupted,
        events.next(),
        &path,
    );
    let model = reloaded
        .history()
        .expect("captured same-key history must project");
    let verdict = history::check(&model, 100_000).expect("captured same-key history must validate");
    log::info!(
        "raft_same_key_history_capture history_path={} attempts={} complete={} verdict={verdict:?} scenario_failure={:?}",
        path.display(),
        model.attempts.len(),
        model.complete,
        reloaded.scenario_failure
    );
    observation.expect("same-key public observations after cleanup and persistence");
    assert!(cleanup.pending_node_joined && cleanup.pending_gateway_joined);
    assert!(cleanup.node_startup_failure.is_none() && cleanup.gateway_startup_failure.is_none());
    assert_eq!(
        cleanup.client_write_calls, 3,
        "only A, B and the fresh C may enter native KV writes"
    );
    let proxy = cleanup.proxy.expect("owned relay joined");
    assert_eq!(proxy.accepted_connections, 1);
    assert_eq!(proxy.forwarded_put_frames, 2);
    assert_eq!(proxy.extra_original_frames, 0);
    assert!(proxy.original_eof_observed);
    assert!(proxy.suppressed_response_bytes > 0);
    assert_eq!(
        model.attempts.len(),
        6,
        "include the retired client's rejected GET"
    );
    assert!(model.complete);
    let received = proxy
        .both_received_at
        .expect("actual receipt of both complete frames");
    let forwarded = proxy
        .forward_started_at
        .expect("actual first forward attempt");
    assert!(model.attempts[0].invoked_at < model.attempts[1].invoked_at);
    assert!(model.attempts[1].invoked_at < received && received < forwarded);
    for attempt in &model.attempts[..2] {
        let proof = attempt
            .execution
            .expect("each Unknown has its own actual Committed receipt");
        assert_eq!(proof.effect, history::EffectKnowledge::Applied);
        assert!(forwarded < proof.ended_by);
        assert!(
            model.attempts[..2]
                .iter()
                .all(|a| proof.ended_by < a.notice.as_ref().unwrap().at),
            "the relay must suppress both actual replies before retiring the generation"
        );
    }
    let history::Verdict::Legal { witnesses, .. } = verdict else {
        panic!("same-key history must be legal: {verdict:?}")
    };
    assert!(
        witnesses[0].omitted.is_empty(),
        "both proven writes must be included"
    );
    assert_eq!(witnesses[0].linearized.len(), 5);
    log::info!(
        "raft_same_key_reply_loss_complete native_calls=3 original_wire_puts=2 connections=1 matched_committed=2 extra_frames=0 original_eof=true public_attempts=6 history=legal both_received={received} forward_started={forwarded} history_path={}",
        path.display()
    );
}

fn completed_observer(
    result: Result<Result<(), String>, tokio::task::JoinError>,
) -> (Result<(), String>, bool) {
    let interrupted = result.is_err();
    (
        result
            .map_err(|e| format!("same-key observer failed: {e}"))
            .and_then(|result| result),
        interrupted,
    )
}

fn persist_capture(
    captured: &Mutex<Capture>,
    cleanup: &Cleanup,
    observation: &Result<(), String>,
    was_interrupted: bool,
    horizon: u64,
    path: &Path,
) -> Capture {
    // A joined, panicking observer may have poisoned the mutex after recording
    // a valid prefix. Recover that prefix rather than losing the evidence.
    let mut capture = captured
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    capture.proxy = cleanup.proxy.as_ref().ok().cloned();
    capture.native_calls = Some(cleanup.client_write_calls);
    capture.scenario_failure = observation
        .as_ref()
        .err()
        .or_else(|| cleanup.proxy.as_ref().err())
        .map(|error| {
            let mut end = error.len().min(1024);
            while !error.is_char_boundary(end) {
                end -= 1;
            }
            error[..end].to_owned()
        });
    capture.seal(horizon, was_interrupted);
    // Called after all native owners close, even if the observer failed.
    capture.persist_reload(path)
}

async fn observe(resources: &mut Resources, calls: PublicCalls) -> Result<(), String> {
    let gateway = start_resources(resources).await?;
    resources.proxy = Some(
        ReplyLossProxy::start_same_key(gateway, Arc::clone(&calls.events))
            .await
            .map_err(|e| e.to_string())?,
    );
    let address = resources.proxy.as_ref().expect("owned proxy").address;
    resources.original = Some(
        RockBeeRaftClient::connect(address, ClientConfig::default())
            .await
            .map_err(|e| e.to_string())?,
    );
    let original = resources.original.as_ref().expect("owned original client");
    let first_value = Bytes::from_static(b"overlapping-value-A");
    let second_value = Bytes::from_static(b"overlapping-value-B");
    let last_value = Bytes::from_static(b"fresh-overwrite-C");
    let first = calls.put(original, first_value.clone())?;
    let second = calls.put(original, second_value.clone())?;
    let (first_outcome, second_outcome) =
        tokio::join!(calls.mutation(first), calls.mutation(second));
    for outcome in [first_outcome?, second_outcome?] {
        if !matches!(outcome, MutationOutcome::Unknown(ref failure) if failure.reason == FailureReason::Disconnected)
        {
            return Err(format!(
                "lost reply must be Unknown(Disconnected): {outcome:?}"
            ));
        }
    }
    let retired = calls.get(original).await?;
    if !matches!(retired, Err(ReadError::Admission(AdmitError::Closed))) {
        return Err(format!(
            "retired client must reject GET locally: {retired:?}"
        ));
    }
    original.close().await;
    resources.fresh = Some(
        RockBeeRaftClient::connect(gateway, ClientConfig::default())
            .await
            .map_err(|e| e.to_string())?,
    );
    let fresh = resources.fresh.as_ref().expect("owned fresh client");
    let first_read = calls.get(fresh).await?.map_err(|e| e.to_string())?;
    if first_read.as_ref() != Some(&first_value) && first_read.as_ref() != Some(&second_value) {
        return Err(format!(
            "fresh GET must return one of the overlapping writes: {first_read:?}"
        ));
    }
    let overwrite = calls
        .mutation(calls.put(fresh, last_value.clone())?)
        .await?;
    if overwrite != MutationOutcome::Committed {
        return Err(format!("fresh overwrite must commit: {overwrite:?}"));
    }
    let last_read = calls.get(fresh).await?.map_err(|e| e.to_string())?;
    if last_read.as_ref() != Some(&last_value) {
        return Err(format!(
            "GET after committed overwrite must return C: {last_read:?}"
        ));
    }
    fresh.close().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::proxy::ProxyReceipt;
    use super::*;

    fn closed_resources() -> Cleanup {
        Cleanup {
            proxy: Ok(ProxyReceipt::default()),
            backend_proxies: vec![],
            client_write_calls: 0,
            pending_node_joined: true,
            pending_gateway_joined: true,
            node_startup_failure: None,
            gateway_startup_failure: None,
        }
    }

    #[tokio::test]
    async fn observer_panic_is_interrupted_and_poisoned_prefix_is_persisted() {
        let capture = Arc::new(Mutex::new(Capture::new(Bytes::from_static(b"same-key"))));
        let task_capture = Arc::clone(&capture);
        let result = tokio::spawn(async move {
            let mut locked = task_capture.lock().unwrap();
            locked
                .begin(Operation::Put(Bytes::from_static(b"A")), 1)
                .unwrap();
            panic!("observer failed while holding capture");
            #[allow(unreachable_code)]
            Ok(())
        })
        .await;
        let (observation, interrupted) = completed_observer(result);
        let directory = tempfile::tempdir().unwrap();
        let reloaded = persist_capture(
            &capture,
            &closed_resources(),
            &observation,
            interrupted,
            2,
            &directory.path().join("history.bin"),
        );
        assert!(interrupted, "JoinError must mark interruption");
        let model = reloaded.history().unwrap();
        assert_eq!(model.attempts.len(), 1);
        assert!(model.attempts[0].notice.is_none());
        assert!(matches!(
            history::check(&model, 100_000).unwrap(),
            history::Verdict::Inconclusive {
                reason: history::Inconclusive::PartialCapture,
                ..
            }
        ));
    }

    #[test]
    fn oversized_unicode_failure_does_not_discard_the_capture() {
        let capture = Mutex::new(Capture::new(Bytes::from_static(b"same-key")));
        let error = "€".repeat(500);
        let directory = tempfile::tempdir().unwrap();
        let reloaded = persist_capture(
            &capture,
            &closed_resources(),
            &Err(error.clone()),
            true,
            1,
            &directory.path().join("history.bin"),
        );
        let retained = reloaded.scenario_failure.unwrap();
        assert!(retained.len() <= 1024);
        assert!(!retained.is_empty());
        assert!(error.starts_with(&retained));
        assert!(!completed_observer(Ok(Err("fully observed liveness failure".into()))).1);
    }
}
