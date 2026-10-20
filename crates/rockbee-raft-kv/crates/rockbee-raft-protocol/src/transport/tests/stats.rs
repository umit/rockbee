use super::*;

fn put(key: &'static [u8]) -> KvRequest {
    KvRequest::Put {
        key: Bytes::from_static(key),
        value: Bytes::from_static(b"value"),
    }
}

#[tokio::test]
async fn stats_admission_and_terminal_are_independent_of_observation() {
    let stats = ClientStatsHandle::default();
    let clone = stats.clone();
    drop(stats.for_put());
    assert_eq!(stats.snapshot(), ClientStatsSnapshot::default());
    let config = LinkConfig {
        max_outstanding: 1,
        ..LinkConfig::default()
    };
    let (link, mut peer) = fixture(4096, config);
    let pending = link
        .try_submit_with_stats(put(b"one"), deadline(), stats.for_put())
        .unwrap();
    let rejected = link.try_submit_with_stats(put(b"two"), deadline(), stats.for_put());
    let request = receive(&mut peer).await;
    respond(
        &mut peer,
        &request,
        KvResponse::Mutation(MutationOutcome::Committed),
    )
    .await;
    assert_eq!(
        pending.await.unwrap(),
        KvResponse::Mutation(MutationOutcome::Committed)
    );
    link.close().await;
    assert!(matches!(rejected, Err(AdmitError::CountLimit)));
    assert_eq!(
        clone.snapshot().put,
        MutationStats {
            attempted: 2,
            admitted: 1,
            admission_failed: 1,
            committed: 1,
            ..MutationStats::default()
        }
    );
}

#[tokio::test]
async fn stats_close_counts_dropped_handed_observer_once() {
    let stats = ClientStatsHandle::default();
    let (link, mut peer) = fixture(4096, LinkConfig::default());
    drop(
        link.try_submit_with_stats(put(b"one"), deadline(), stats.for_put())
            .unwrap(),
    );
    receive(&mut peer).await;
    link.close().await;
    link.close().await;
    assert_eq!(
        stats.snapshot().put,
        MutationStats {
            attempted: 1,
            admitted: 1,
            unknown: 1,
            ..MutationStats::default()
        }
    );
}

#[tokio::test(start_paused = true)]
async fn stats_queued_expiry_is_not_sent_and_retirement_is_unknown() {
    let stats = ClientStatsHandle::default();
    let config = LinkConfig {
        max_outstanding: 2,
        ..LinkConfig::default()
    };
    let (link, mut peer) = fixture(1, config);
    let handed = link
        .try_submit_with_stats(put(b"handed"), deadline(), stats.for_put())
        .unwrap();
    peer.read_u8().await.unwrap();
    let queued = link
        .try_submit_with_stats(
            put(b"queued"),
            Instant::now() + Duration::from_secs(1),
            stats.for_put(),
        )
        .unwrap();
    advance(Duration::from_secs(2)).await;
    assert_failure(
        queued.await,
        Delivery::NotSent,
        TransportFailureKind::Deadline,
    );
    let before_close = stats.snapshot();
    link.close().await;
    assert_failure(
        handed.await,
        Delivery::MayHaveBeenSent,
        TransportFailureKind::Closed,
    );
    assert_eq!(before_close.put.pending, 1);
    assert_eq!(before_close.put.not_sent, 1);
    assert_eq!(
        stats.snapshot().put,
        MutationStats {
            attempted: 2,
            admitted: 2,
            not_sent: 1,
            unknown: 1,
            ..MutationStats::default()
        }
    );
}

#[tokio::test]
async fn stats_id_exhaustion_counts_existing_and_rejected_calls_separately() {
    let stats = ClientStatsHandle::default();
    let (link, mut peer) = fixture(4096, LinkConfig::default());
    link.inner.shared.lock().next_id = Some(u64::MAX);
    let pending = link
        .try_submit_with_stats(put(b"last"), deadline(), stats.for_put())
        .unwrap();
    receive(&mut peer).await;
    let rejected = link.try_submit_with_stats(put(b"overflow"), deadline(), stats.for_put());
    assert_failure(
        pending.await,
        Delivery::MayHaveBeenSent,
        TransportFailureKind::IdExhausted,
    );
    link.close().await;
    assert!(matches!(rejected, Err(AdmitError::IdExhausted)));
    assert_eq!(
        stats.snapshot().put,
        MutationStats {
            attempted: 2,
            admitted: 1,
            admission_failed: 1,
            unknown: 1,
            ..MutationStats::default()
        }
    );
}

#[tokio::test]
async fn stats_read_empty_value_is_hit_and_disconnect_is_transport_error() {
    let stats = ClientStatsHandle::default();
    let (link, mut peer) = fixture(4096, LinkConfig::default());
    let first = link
        .try_submit_with_stats(get(b"empty"), deadline(), stats.for_get())
        .unwrap();
    let request = receive(&mut peer).await;
    respond(&mut peer, &request, KvResponse::Value(Some(Bytes::new()))).await;
    first.await.unwrap();
    let second = link
        .try_submit_with_stats(get(b"lost"), deadline(), stats.for_get())
        .unwrap();
    receive(&mut peer).await;
    drop(peer);
    assert_failure(
        second.await,
        Delivery::MayHaveBeenSent,
        TransportFailureKind::Disconnected,
    );
    link.close().await;
    assert_eq!(
        stats.snapshot().get,
        ReadStats {
            attempted: 2,
            admitted: 2,
            hit: 1,
            transport_error: 1,
            ..ReadStats::default()
        }
    );
}

#[tokio::test]
async fn stats_handoff_expiry_counts_once_without_writing() {
    let stats = ClientStatsHandle::default();
    let (link, mut peer) = fixture(4096, LinkConfig::default());
    let pending = link
        .try_submit_with_stats(put(b"expired"), deadline(), stats.for_put())
        .unwrap();
    // No await since submission: the writer cannot have taken this queued entry.
    let id = RequestId::new(1).unwrap();
    {
        let mut state = link.inner.shared.lock();
        let now = Instant::now();
        state.entries.get_mut(&id).unwrap().deadline = now;
        state.deadlines.clear();
        state.deadlines.insert((now, id));
    }
    assert!(!link.inner.shared.handoff(id));
    assert!(!link.inner.shared.handoff(id));
    assert_failure(
        pending.await,
        Delivery::NotSent,
        TransportFailureKind::Deadline,
    );
    link.close().await;
    let mut bytes = Vec::new();
    peer.read_to_end(&mut bytes).await.unwrap();
    assert!(bytes.is_empty());
    assert_eq!(
        stats.snapshot().put,
        MutationStats {
            attempted: 1,
            admitted: 1,
            not_sent: 1,
            ..MutationStats::default()
        }
    );
}

#[tokio::test(start_paused = true)]
async fn stats_response_winner_ignores_later_expiration_and_close() {
    let stats = ClientStatsHandle::default();
    let (link, mut peer) = fixture(4096, LinkConfig::default());
    let expires = Instant::now() + Duration::from_secs(1);
    let pending = link
        .try_submit_with_stats(put(b"winner"), expires, stats.for_put())
        .unwrap();
    let request = receive(&mut peer).await;
    respond(
        &mut peer,
        &request,
        KvResponse::Mutation(MutationOutcome::Committed),
    )
    .await;
    assert_eq!(
        pending.await.unwrap(),
        KvResponse::Mutation(MutationOutcome::Committed)
    );
    link.inner
        .shared
        .expire(request.request_id, expires + Duration::from_secs(1));
    link.close().await;
    assert_eq!(
        stats.snapshot().put,
        MutationStats {
            attempted: 1,
            admitted: 1,
            committed: 1,
            ..MutationStats::default()
        }
    );
}

#[tokio::test(start_paused = true)]
async fn stats_reader_deadline_wins_over_received_success_once() {
    let stats = ClientStatsHandle::default();
    let (link, mut peer) = fixture(4096, LinkConfig::default());
    // As in the existing reader-deadline test, stop only this owned coordinator
    // to exercise the reader's independent deadline check deterministically.
    let coordinator = link.inner.workers.lock().await.pop().unwrap();
    coordinator.abort();
    let _ = coordinator.await;
    let pending = link
        .try_submit_with_stats(
            put(b"late"),
            Instant::now() + Duration::from_secs(1),
            stats.for_put(),
        )
        .unwrap();
    let request = receive(&mut peer).await;
    advance(Duration::from_secs(2)).await;
    respond(
        &mut peer,
        &request,
        KvResponse::Mutation(MutationOutcome::Committed),
    )
    .await;
    assert_failure(
        pending.await,
        Delivery::MayHaveBeenSent,
        TransportFailureKind::Deadline,
    );
    link.close().await;
    assert_eq!(
        stats.snapshot().put,
        MutationStats {
            attempted: 1,
            admitted: 1,
            unknown: 1,
            ..MutationStats::default()
        }
    );
}

#[tokio::test]
async fn stats_read_miss_and_remote_error_are_distinct() {
    let stats = ClientStatsHandle::default();
    let (link, mut peer) = fixture(4096, LinkConfig::default());
    for response in [
        KvResponse::Value(None),
        KvResponse::ReadError(crate::ReadFailure::new(
            Failure::new(FailureReason::Unavailable, "unavailable").unwrap(),
        )),
    ] {
        let pending = link
            .try_submit_with_stats(get(b"key"), deadline(), stats.for_get())
            .unwrap();
        let request = receive(&mut peer).await;
        respond(&mut peer, &request, response.clone()).await;
        assert_eq!(pending.await.unwrap(), response);
    }
    link.close().await;
    assert_eq!(
        stats.snapshot().get,
        ReadStats {
            attempted: 2,
            admitted: 2,
            miss: 1,
            remote_error: 1,
            ..ReadStats::default()
        }
    );
}
