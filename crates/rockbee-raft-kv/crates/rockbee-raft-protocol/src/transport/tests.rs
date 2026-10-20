#![allow(clippy::unwrap_used)]

use std::{
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf},
    sync::Notify,
    time::{Instant, advance},
};

use super::*;
use crate::{
    Failure, FailureReason, KvRequest, KvResponse, MAX_FRAME_PAYLOAD, MAX_VALUE_BYTES,
    MutationOutcome, RequestEnvelope, ResponseEnvelope, VERSION, decode_frame, encode_frame,
};

fn get(key: &'static [u8]) -> KvRequest {
    KvRequest::Get {
        key: Bytes::from_static(key),
    }
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(30)
}
fn fixture(capacity: usize, config: LinkConfig) -> (Link<KvRequest, KvResponse>, DuplexStream) {
    let (socket, peer) = tokio::io::duplex(capacity);
    (Link::start(socket, config), peer)
}
async fn receive<R: AsyncRead + Unpin>(peer: &mut R) -> RequestEnvelope {
    let length = peer.read_u32().await.unwrap() as usize;
    let mut frame = vec![0; length + 4];
    frame[..4].copy_from_slice(&(length as u32).to_be_bytes());
    peer.read_exact(&mut frame[4..]).await.unwrap();
    decode_frame(&frame).unwrap()
}
async fn respond<W: AsyncWrite + Unpin>(
    peer: &mut W,
    request: &RequestEnvelope,
    response: KvResponse,
) {
    peer.write_all(
        &encode_frame(&ResponseEnvelope {
            version: VERSION,
            request_id: request.request_id,
            response,
        })
        .unwrap(),
    )
    .await
    .unwrap();
}
async fn settle() {
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
}
fn assert_failure(
    result: Result<KvResponse, TransportFailure>,
    delivery: Delivery,
    reason: TransportFailureKind,
) {
    assert_eq!(result.unwrap_err(), TransportFailure { delivery, reason });
}

#[tokio::test]
async fn eager_unpolled_requests_correlate_out_of_order_and_retain_result_permits() {
    let config = LinkConfig {
        max_outstanding: 2,
        ..LinkConfig::default()
    };
    let (link, mut peer) = fixture(4096, config);
    let first = link.try_submit(get(b"first"), deadline()).unwrap();
    let second = link.try_submit(get(b"second"), deadline()).unwrap();
    let one = receive(&mut peer).await;
    let two = receive(&mut peer).await;
    respond(
        &mut peer,
        &two,
        KvResponse::Value(Some(Bytes::from_static(b"two"))),
    )
    .await;
    respond(
        &mut peer,
        &one,
        KvResponse::Value(Some(Bytes::from_static(b"one"))),
    )
    .await;
    settle().await;
    assert!(matches!(
        link.try_submit(get(b"full"), deadline()),
        Err(AdmitError::CountLimit)
    ));
    assert!(link.inner.shared.receive_bytes.available_permits() < config.receive_bytes);
    assert_eq!(
        second.await.unwrap(),
        KvResponse::Value(Some(Bytes::from_static(b"two")))
    );
    assert_eq!(
        first.await.unwrap(),
        KvResponse::Value(Some(Bytes::from_static(b"one")))
    );
    assert_eq!(
        link.inner.shared.receive_bytes.available_permits(),
        config.receive_bytes
    );
    link.close().await;
    assert_eq!(
        link.inner.shared.count.available_permits(),
        config.max_outstanding
    );
}

#[tokio::test]
async fn dropped_handle_does_not_cancel_or_release_inflight_admission() {
    let config = LinkConfig {
        max_outstanding: 1,
        ..LinkConfig::default()
    };
    let (link, mut peer) = fixture(4096, config);
    drop(link.try_submit(get(b"accepted"), deadline()).unwrap());
    assert!(matches!(
        link.try_submit(get(b"full"), deadline()),
        Err(AdmitError::CountLimit)
    ));
    let request = receive(&mut peer).await;
    respond(&mut peer, &request, KvResponse::Value(None)).await;
    settle().await;
    assert_eq!(link.inner.shared.count.available_permits(), 1);
    assert_eq!(
        link.inner.shared.receive_bytes.available_permits(),
        config.receive_bytes
    );
    link.close().await;
}

#[tokio::test(start_paused = true)]
async fn queued_expiry_never_writes_and_its_storage_stays_charged_until_drained() {
    let config = LinkConfig {
        max_outstanding: 2,
        ..LinkConfig::default()
    };
    let (link, mut peer) = fixture(1, config);
    let first = link.try_submit(get(b"writer-blocked"), deadline()).unwrap();
    assert_eq!(peer.read_u8().await.unwrap(), 0); // First header byte, then write stalls.
    let queued = link
        .try_submit(
            get(b"must-not-write"),
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
    let charged = link.inner.shared.request_bytes.available_permits();
    advance(Duration::from_secs(2)).await;
    assert_failure(
        queued.await,
        Delivery::NotSent,
        TransportFailureKind::Deadline,
    );
    assert_eq!(link.inner.shared.count.available_permits(), 0);
    assert_eq!(link.inner.shared.request_bytes.available_permits(), charged);
    assert_eq!(link.inner.shared.lock().deadlines.len(), 1);
    for _ in 0..100 {
        assert!(matches!(
            link.try_submit(get(b"expired-storm"), deadline()),
            Err(AdmitError::CountLimit)
        ));
    }
    assert_eq!(link.inner.shared.request_bytes.available_permits(), charged);
    assert_eq!(link.inner.shared.lock().entries.len(), 1);
    // Finish exactly the first frame. There is no second request to read.
    let mut header_tail = [0; 3];
    peer.read_exact(&mut header_tail).await.unwrap();
    let length = u32::from_be_bytes([0, header_tail[0], header_tail[1], header_tail[2]]) as usize;
    let mut body = vec![0; length];
    peer.read_exact(&mut body).await.unwrap();
    settle().await;
    assert_eq!(link.inner.shared.count.available_permits(), 1);
    link.close().await;
    assert_failure(
        first.await,
        Delivery::MayHaveBeenSent,
        TransportFailureKind::Closed,
    );
    let mut tail = Vec::new();
    peer.read_to_end(&mut tail).await.unwrap();
    assert!(
        tail.is_empty(),
        "expired queued request must write zero bytes"
    );
    assert_eq!(
        link.inner.shared.request_bytes.available_permits(),
        config.request_bytes
    );
}

/// Test-only stream: observes the first socket-write poll while accepting zero
/// bytes. No production transport fault hooks or per-request tasks are needed.
struct HeldWrites {
    stream: DuplexStream,
    polls: Arc<AtomicUsize>,
    observed: Arc<Notify>,
}
impl AsyncRead for HeldWrites {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}
impl AsyncWrite for HeldWrites {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        self.observed.notify_one();
        Poll::Pending
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test(start_paused = true)]
async fn post_handoff_zero_byte_timeout_retires_and_queued_work_is_unsent() {
    let config = LinkConfig::default();
    let (stream, mut peer) = tokio::io::duplex(4096);
    let observed = Arc::new(Notify::new());
    let polls = Arc::new(AtomicUsize::new(0));
    let link: Link<KvRequest, KvResponse> = Link::start(
        HeldWrites {
            stream,
            polls: Arc::clone(&polls),
            observed: Arc::clone(&observed),
        },
        config,
    );
    let handed = link
        .try_submit(get(b"handed"), Instant::now() + Duration::from_secs(1))
        .unwrap();
    observed.notified().await;
    let queued = link.try_submit(get(b"queued"), deadline()).unwrap();
    advance(Duration::from_secs(2)).await;
    assert_failure(
        handed.await,
        Delivery::MayHaveBeenSent,
        TransportFailureKind::Deadline,
    );
    assert_failure(
        queued.await,
        Delivery::NotSent,
        TransportFailureKind::Deadline,
    );
    assert!(polls.load(Ordering::SeqCst) > 0);
    link.close().await;
    let mut bytes = Vec::new();
    peer.read_to_end(&mut bytes).await.unwrap();
    assert!(bytes.is_empty());
    assert_eq!(
        link.inner.shared.request_bytes.available_permits(),
        config.request_bytes
    );
    assert!(link.inner.shared.lock().entries.is_empty());
    assert!(link.inner.shared.lock().deadlines.is_empty());
}

#[tokio::test(start_paused = true)]
async fn partial_frame_timeout_never_appends_the_next_frame() {
    let (link, mut peer) = fixture(1, LinkConfig::default());
    let first = link
        .try_submit(get(b"first"), Instant::now() + Duration::from_secs(1))
        .unwrap();
    assert_eq!(peer.read_u8().await.unwrap(), 0);
    let second = link.try_submit(get(b"second"), deadline()).unwrap();
    advance(Duration::from_secs(2)).await;
    assert_failure(
        first.await,
        Delivery::MayHaveBeenSent,
        TransportFailureKind::Deadline,
    );
    assert_failure(
        second.await,
        Delivery::NotSent,
        TransportFailureKind::Deadline,
    );
    link.close().await;
    let mut tail = Vec::new();
    peer.read_to_end(&mut tail).await.unwrap();
    assert!(
        tail.len() <= 1,
        "only the first partial frame's buffered byte may remain"
    );
}

#[tokio::test(start_paused = true)]
async fn missing_response_retires_all_handed_operations_and_never_replays() {
    let (link, mut peer) = fixture(4096, LinkConfig::default());
    let first = link
        .try_submit(get(b"first"), Instant::now() + Duration::from_secs(1))
        .unwrap();
    let first_wire = receive(&mut peer).await;
    let second = link.try_submit(get(b"second"), deadline()).unwrap();
    let _second_wire = receive(&mut peer).await;
    advance(Duration::from_secs(2)).await;
    assert_failure(
        first.await,
        Delivery::MayHaveBeenSent,
        TransportFailureKind::Deadline,
    );
    assert_failure(
        second.await,
        Delivery::MayHaveBeenSent,
        TransportFailureKind::Deadline,
    );
    assert!(matches!(
        link.try_submit(get(b"new"), deadline()),
        Err(AdmitError::Closed)
    ));
    link.close().await;
    let late = encode_frame(&ResponseEnvelope {
        version: VERSION,
        request_id: first_wire.request_id,
        response: KvResponse::Value(None),
    })
    .unwrap();
    assert!(peer.write_all(&late).await.is_err());
    let (replacement, mut new_peer) = fixture(4096, LinkConfig::default());
    let fresh = replacement.try_submit(get(b"fresh"), deadline()).unwrap();
    let request = receive(&mut new_peer).await;
    assert_eq!(request.request, get(b"fresh"));
    respond(&mut new_peer, &request, KvResponse::Value(None)).await;
    assert_eq!(fresh.await.unwrap(), KvResponse::Value(None));
    replacement.close().await;
}

#[tokio::test(start_paused = true)]
async fn completed_request_stale_deadline_does_not_retire_new_work() {
    let (link, mut peer) = fixture(4096, LinkConfig::default());
    let first = link
        .try_submit(get(b"first"), Instant::now() + Duration::from_secs(1))
        .unwrap();
    let request = receive(&mut peer).await;
    respond(&mut peer, &request, KvResponse::Value(None)).await;
    assert_eq!(first.await.unwrap(), KvResponse::Value(None));
    let second = link.try_submit(get(b"second"), deadline()).unwrap();
    let request = receive(&mut peer).await;
    advance(Duration::from_secs(2)).await;
    assert!(link.inner.shared.lock().retired.is_none());
    respond(&mut peer, &request, KvResponse::Value(None)).await;
    assert_eq!(second.await.unwrap(), KvResponse::Value(None));
    link.close().await;
}

#[tokio::test]
async fn duplicate_response_keeps_completed_result_and_retires_other_handed_work() {
    let (link, mut peer) = fixture(4096, LinkConfig::default());
    let first = link.try_submit(get(b"first"), deadline()).unwrap();
    let second = link.try_submit(get(b"second"), deadline()).unwrap();
    let first_wire = receive(&mut peer).await;
    let _second_wire = receive(&mut peer).await;
    respond(&mut peer, &first_wire, KvResponse::Value(None)).await;
    assert_eq!(first.await.unwrap(), KvResponse::Value(None));
    respond(&mut peer, &first_wire, KvResponse::Value(None)).await;
    assert_failure(
        second.await,
        Delivery::MayHaveBeenSent,
        TransportFailureKind::Protocol,
    );
    link.close().await;
}

#[tokio::test]
async fn wrong_kind_remote_not_sent_and_unknown_id_retire_without_false_rejection() {
    let responses = [
        KvResponse::Value(None),
        KvResponse::Mutation(MutationOutcome::NotSent(Failure {
            reason: FailureReason::Overloaded,
            message: String::new(),
        })),
        KvResponse::Mutation(MutationOutcome::Committed),
    ];
    for (index, response) in responses.into_iter().enumerate() {
        let (link, mut peer) = fixture(4096, LinkConfig::default());
        let pending = link
            .try_submit(
                KvRequest::Delete {
                    key: Bytes::from_static(b"key"),
                },
                deadline(),
            )
            .unwrap();
        let mut request = receive(&mut peer).await;
        if index == 2 {
            request.request_id = RequestId::new(999).unwrap();
        }
        respond(&mut peer, &request, response).await;
        assert_failure(
            pending.await,
            Delivery::MayHaveBeenSent,
            TransportFailureKind::Protocol,
        );
        link.close().await;
    }
}

#[tokio::test]
async fn id_exhaustion_never_wraps_or_reuses_and_retires_existing_work() {
    let (link, mut peer) = fixture(4096, LinkConfig::default());
    link.inner.shared.lock().next_id = Some(u64::MAX);
    let final_id = link.try_submit(get(b"maximum"), deadline()).unwrap();
    assert_eq!(receive(&mut peer).await.request_id.get(), u64::MAX);
    assert!(matches!(
        link.try_submit(get(b"overflow"), deadline()),
        Err(AdmitError::IdExhausted)
    ));
    assert_failure(
        final_id.await,
        Delivery::MayHaveBeenSent,
        TransportFailureKind::IdExhausted,
    );
    link.close().await;
}

#[tokio::test]
async fn byte_saturation_is_unsent_and_close_drops_every_queued_reservation() {
    let config = LinkConfig {
        request_bytes: 5 * 1024 * 1024,
        ..LinkConfig::default()
    };
    config.validate::<KvRequest, KvResponse>().unwrap();
    let (link, mut peer) = fixture(1, config);
    let large = || KvRequest::Put {
        key: Bytes::from_static(b"key"),
        value: Bytes::from(vec![7; MAX_VALUE_BYTES]),
    };
    let first = link.try_submit(large(), deadline()).unwrap();
    peer.read_u8().await.unwrap();
    let second = link.try_submit(large(), deadline()).unwrap();
    assert!(matches!(
        link.try_submit(large(), deadline()),
        Err(AdmitError::ByteLimit)
    ));
    drop(first);
    drop(second);
    link.close().await;
    assert_eq!(
        link.inner.shared.request_bytes.available_permits(),
        config.request_bytes
    );
    assert_eq!(
        link.inner.shared.count.available_permits(),
        config.max_outstanding
    );
    assert!(link.inner.workers.lock().await.is_empty());
}

#[tokio::test]
async fn invalid_config_rejects_zero_impossible_and_overflowing_limits() {
    for config in [
        LinkConfig {
            max_outstanding: 0,
            ..LinkConfig::default()
        },
        LinkConfig {
            request_bytes: MAX_FRAME_PAYLOAD,
            ..LinkConfig::default()
        },
        LinkConfig {
            receive_bytes: MAX_FRAME_PAYLOAD,
            ..LinkConfig::default()
        },
        LinkConfig {
            receive_bytes: usize::MAX,
            ..LinkConfig::default()
        },
        LinkConfig {
            max_outstanding: usize::MAX,
            ..LinkConfig::default()
        },
    ] {
        assert!(matches!(
            Link::<KvRequest, KvResponse>::connect(
                "127.0.0.1:1".parse().unwrap(),
                config,
                deadline()
            )
            .await,
            Err(ConnectError::Config(_))
        ));
    }
}

#[tokio::test]
async fn outer_and_hostile_inner_lengths_fail_closed_and_release_receive_budget() {
    // Ordinary frame with a Value(Some(Bytes)) declaring an impossible inner
    // length, independently encoded rather than using trusted serializers.
    let hostile_payload = [1, 1, 0, 1, 0xff, 0xff, 0xff, 0xff, 0x0f];
    for hostile in [Vec::from(u32::MAX.to_be_bytes()), {
        let mut frame = Vec::from((hostile_payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(&hostile_payload);
        frame
    }] {
        let config = LinkConfig::default();
        let (link, mut peer) = fixture(4096, config);
        let pending = link.try_submit(get(b"key"), deadline()).unwrap();
        receive(&mut peer).await;
        peer.write_all(&hostile).await.unwrap();
        assert_failure(
            pending.await,
            Delivery::MayHaveBeenSent,
            TransportFailureKind::Protocol,
        );
        link.close().await;
        assert_eq!(
            link.inner.shared.receive_bytes.available_permits(),
            config.receive_bytes
        );
    }
}

#[tokio::test]
async fn receive_budget_stays_with_unpolled_results_and_blocks_before_decode() {
    let config = LinkConfig {
        receive_bytes: 5 * 1024 * 1024,
        ..LinkConfig::default()
    };
    let (link, peer) = fixture(4 * 1024 * 1024, config);
    let (mut read_peer, mut write_peer) = tokio::io::split(peer);
    let mut pending = Vec::new();
    let mut requests = Vec::new();
    for _ in 0..3 {
        pending.push(link.try_submit(get(b"tiny"), deadline()).unwrap());
        requests.push(receive(&mut read_peer).await);
    }
    let maximum = KvResponse::Value(Some(Bytes::from(vec![9; MAX_VALUE_BYTES])));
    for request in &requests {
        respond(&mut write_peer, request, maximum.clone()).await;
    }
    settle().await;
    // The third result cannot be decoded until one of the first two results
    // releases its full encoded+decoded receive reservation.
    assert_eq!(link.inner.shared.lock().entries.len(), 1);
    assert!(link.inner.shared.receive_bytes.available_permits() < MAX_VALUE_BYTES);
    drop(pending.remove(0));
    settle().await;
    assert!(link.inner.shared.lock().entries.is_empty());
    for result in pending {
        assert_eq!(result.await.unwrap(), maximum);
    }
    assert_eq!(
        link.inner.shared.receive_bytes.available_permits(),
        config.receive_bytes
    );
    link.close().await;
}

#[tokio::test(start_paused = true)]
async fn reader_rejects_late_success_even_when_deadline_actor_is_delayed() {
    let (link, mut peer) = fixture(4096, LinkConfig::default());
    // Remove only this fixture's owned coordinator to force reader-side
    // enforcement independently of timer scheduling. Production has no switch.
    let coordinator = link.inner.workers.lock().await.pop().unwrap();
    coordinator.abort();
    let _ = coordinator.await;
    let pending = link
        .try_submit(get(b"late"), Instant::now() + Duration::from_secs(1))
        .unwrap();
    let request = receive(&mut peer).await;
    advance(Duration::from_secs(2)).await;
    respond(&mut peer, &request, KvResponse::Value(None)).await;
    assert_failure(
        pending.await,
        Delivery::MayHaveBeenSent,
        TransportFailureKind::Deadline,
    );
    link.close().await;
}

#[tokio::test(start_paused = true)]
async fn queued_time_is_spent_before_stamping_wire_budget() {
    let (link, mut peer) = fixture(1, LinkConfig::default());
    let first = link.try_submit(get(b"first"), deadline()).unwrap();
    let first_byte = peer.read_u8().await.unwrap();
    let second = link.try_submit(get(b"second"), deadline()).unwrap();
    advance(Duration::from_secs(2)).await;
    let mut tail = [0; 3];
    peer.read_exact(&mut tail).await.unwrap();
    let length = u32::from_be_bytes([first_byte, tail[0], tail[1], tail[2]]) as usize;
    let mut frame = vec![0; length + 4];
    frame[..4].copy_from_slice(&(length as u32).to_be_bytes());
    peer.read_exact(&mut frame[4..]).await.unwrap();
    let one: RequestEnvelope = decode_frame(&frame).unwrap();
    let two = receive(&mut peer).await;
    assert!(two.budget_ms <= 28_000 && two.budget_ms > 0);
    respond(&mut peer, &one, KvResponse::Value(None)).await;
    respond(&mut peer, &two, KvResponse::Value(None)).await;
    first.await.unwrap();
    second.await.unwrap();
    link.close().await;
}

#[derive(serde::Serialize)]
struct Ping(u64);
#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Pong(u64);
impl WireRequest<Pong> for Ping {
    type Expected = u64;
    fn validate(&self) -> Result<(), crate::ProtocolError> {
        Ok(())
    }
    fn retained_bytes(&self) -> Result<usize, crate::ProtocolError> {
        Ok(size_of::<Self>())
    }
    fn expected_response(&self) -> Self::Expected {
        self.0
    }
    fn validate_response(
        expected: Self::Expected,
        response: &Pong,
    ) -> Result<(), crate::ProtocolError> {
        if expected == response.0 {
            Ok(())
        } else {
            Err(crate::ProtocolError::ResponseKindMismatch)
        }
    }
}

#[tokio::test]
async fn separate_message_family_uses_the_same_generic_link_and_envelopes() {
    let (socket, mut peer) = tokio::io::duplex(4096);
    let link: Link<Ping, Pong> = Link::start(socket, LinkConfig::default());
    let pending = link.try_submit(Ping(17), deadline()).unwrap();
    let length = peer.read_u32().await.unwrap() as usize;
    let mut frame = vec![0; length + 4];
    frame[..4].copy_from_slice(&(length as u32).to_be_bytes());
    peer.read_exact(&mut frame[4..]).await.unwrap();
    // Decode independently into the generic primitive body, not public KV.
    let request: RequestEnvelope<u64> = decode_frame(&frame).unwrap();
    assert_eq!(request.request, 17);
    let response = ResponseEnvelope {
        version: VERSION,
        request_id: request.request_id,
        response: Pong(17),
    };
    peer.write_all(&encode_frame(&response).unwrap())
        .await
        .unwrap();
    assert_eq!(pending.await.unwrap(), Pong(17));
    link.close().await;
}

#[tokio::test]
async fn disconnect_after_partial_write_is_uncertain_and_drops_queued_work() {
    let config = LinkConfig::default();
    let (link, mut peer) = fixture(1, config);
    let handed = link.try_submit(get(b"partial"), deadline()).unwrap();
    peer.read_u8().await.unwrap();
    let queued = link.try_submit(get(b"queued"), deadline()).unwrap();
    drop(peer);
    let failure = handed.await.unwrap_err();
    assert_eq!(failure.delivery, Delivery::MayHaveBeenSent);
    assert!(matches!(
        failure.reason,
        TransportFailureKind::Io | TransportFailureKind::Disconnected
    ));
    let failure = queued.await.unwrap_err();
    assert_eq!(failure.delivery, Delivery::NotSent);
    link.close().await;
    assert_eq!(
        link.inner.shared.request_bytes.available_permits(),
        config.request_bytes
    );
    assert_eq!(
        link.inner.shared.count.available_permits(),
        config.max_outstanding
    );
}

#[tokio::test]
async fn expired_connect_and_submit_deadlines_fail_without_submission() {
    let expired = Instant::now() - Duration::from_secs(1);
    assert!(matches!(
        Link::<KvRequest, KvResponse>::connect(
            "127.0.0.1:1".parse().unwrap(),
            LinkConfig::default(),
            expired
        )
        .await,
        Err(ConnectError::Deadline)
    ));
    let (link, mut peer) = fixture(4096, LinkConfig::default());
    assert!(matches!(
        link.try_submit(get(b"expired"), expired),
        Err(AdmitError::Deadline)
    ));
    link.close().await;
    let mut bytes = Vec::new();
    peer.read_to_end(&mut bytes).await.unwrap();
    assert!(bytes.is_empty());
}

#[tokio::test]
async fn directory_decode_headroom_is_reserved_until_result_is_consumed() {
    use crate::{
        ClusterId, NodeId, ShardConfig,
        backend::{BackendOperation, BackendReply, BackendRequest, BackendResponse},
        routing::{ClusterDirectory, NodeEndpoint},
    };
    let cluster_id = ClusterId::new([1; 16]).unwrap();
    let node_id = NodeId::new(1).unwrap();
    let nodes = (1..=3)
        .map(|id| NodeEndpoint {
            node_id: NodeId::new(id).unwrap(),
            peer_address: format!("127.0.0.1:{}", 7100 + id).parse().unwrap(),
            data_address: format!("127.0.0.1:{}", 8100 + id).parse().unwrap(),
        })
        .collect();
    let directory =
        ClusterDirectory::initial(cluster_id, ShardConfig::new(256, 0).unwrap(), nodes).unwrap();
    let config = LinkConfig::default();
    let (stream, mut peer) = tokio::io::duplex(65_536);
    let link = Link::<BackendRequest, BackendResponse>::start(stream, config);
    let pending = link
        .try_submit(
            BackendRequest {
                cluster_id,
                target_node: node_id,
                operation: BackendOperation::Directory,
            },
            deadline(),
        )
        .unwrap();
    let length = peer.read_u32().await.unwrap();
    let mut frame = vec![0; length as usize + 4];
    frame[..4].copy_from_slice(&length.to_be_bytes());
    peer.read_exact(&mut frame[4..]).await.unwrap();
    let request: RequestEnvelope<BackendRequest> = decode_frame(&frame).unwrap();
    let response = ResponseEnvelope {
        version: VERSION,
        request_id: request.request_id,
        response: BackendResponse {
            cluster_id,
            node_id,
            reply: BackendReply::Directory(Ok(directory)),
        },
    };
    peer.write_all(&encode_frame(&response).unwrap())
        .await
        .unwrap();
    settle().await;
    assert!(
        config.receive_bytes - link.inner.shared.receive_bytes.available_permits() >= 64 * 1024
    );
    pending.await.unwrap();
    assert_eq!(
        link.inner.shared.receive_bytes.available_permits(),
        config.receive_bytes
    );
    link.close().await;
}

mod stats;
