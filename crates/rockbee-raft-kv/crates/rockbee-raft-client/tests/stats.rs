//! Client observations over real TCP; peers are scripted, not consensus servers.
use std::time::Duration;

use bytes::Bytes;
use rockbee_raft_client::{
    AdmitError, ClientConfig, ClientStatsHandle, ClientStatsSnapshot, LinkConfig, MutationOutcome,
    ReadError, RockBeeRaftClient,
};
use rockbee_raft_protocol::{
    Failure, FailureReason, KvRequest, KvResponse, ReadFailure, RequestEnvelope, ResponseEnvelope,
    VERSION, decode_frame, encode_frame,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    time::timeout,
};

const WAIT: Duration = Duration::from_secs(3);

async fn listener() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").await.expect("owned port")
}

async fn connect(listener: &TcpListener, config: ClientConfig) -> RockBeeRaftClient {
    RockBeeRaftClient::connect(listener.local_addr().expect("owned address"), config)
        .await
        .expect("connect")
}

async fn receive(stream: &mut TcpStream) -> RequestEnvelope {
    timeout(WAIT, async {
        let size = stream.read_u32().await.expect("request header");
        assert!(size <= 4096, "bounded small fixture request");
        let mut frame = size.to_be_bytes().to_vec();
        frame.resize(usize::try_from(size).expect("bounded size") + 4, 0);
        stream.read_exact(&mut frame[4..]).await.expect("request");
        decode_frame(&frame).expect("valid request")
    })
    .await
    .expect("bounded request wait")
}

async fn respond(stream: &mut TcpStream, request: RequestEnvelope, response: KvResponse) {
    let frame = encode_frame(&ResponseEnvelope {
        version: VERSION,
        request_id: request.request_id,
        response,
    })
    .expect("valid response");
    timeout(WAIT, stream.write_all(&frame))
        .await
        .expect("bounded response wait")
        .expect("response write");
}

async fn expect_eof(stream: &mut TcpStream) {
    let mut byte = [0];
    assert_eq!(
        timeout(WAIT, stream.read(&mut byte))
            .await
            .expect("client releases its socket")
            .expect("peer read"),
        0,
        "no automatic replay or additional request"
    );
}

async fn wait_stats(
    stats: &ClientStatsHandle,
    ready: impl Fn(ClientStatsSnapshot) -> bool,
) -> ClientStatsSnapshot {
    timeout(WAIT, async {
        loop {
            let snapshot = stats.snapshot();
            if ready(snapshot) {
                return snapshot;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("terminal counters become visible without polling the result")
}

fn failure(reason: FailureReason) -> Failure {
    Failure {
        reason,
        message: "scripted peer outcome".to_owned(),
    }
}

#[tokio::test]
async fn mutation_stats_preserve_received_certainty_and_dropped_observers() {
    let listener = listener().await;
    let client = connect(&listener, ClientConfig::default()).await;
    let clone = client.clone();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        for outcome in [
            MutationOutcome::Committed,
            MutationOutcome::Rejected(failure(FailureReason::Overloaded)),
            MutationOutcome::Unknown(failure(FailureReason::Unavailable)),
        ] {
            let request = receive(&mut stream).await;
            respond(&mut stream, request, KvResponse::Mutation(outcome)).await;
        }
        expect_eof(&mut stream).await;
    });
    drop(
        client
            .put(Bytes::from_static(b"dropped"), Bytes::new())
            .expect("put"),
    );
    let rejected = clone
        .delete(Bytes::from_static(b"rejected"))
        .expect("delete")
        .await;
    let unknown = client
        .put(Bytes::from_static(b"unknown"), Bytes::new())
        .expect("put")
        .await;
    client.close().await;
    peer.await.expect("owned peer joined");
    let stats = client.stats();
    assert_eq!(stats, clone.stats(), "clones share counters");
    assert_eq!(
        (stats.put.attempted, stats.put.admitted, stats.put.pending),
        (2, 2, 0)
    );
    assert_eq!(
        (
            stats.put.committed,
            stats.put.unknown,
            stats.put.rejected,
            stats.put.not_sent
        ),
        (1, 1, 0, 0)
    );
    assert_eq!(
        (
            stats.delete.attempted,
            stats.delete.admitted,
            stats.delete.pending,
            stats.delete.rejected
        ),
        (1, 1, 0, 1)
    );
    assert!(matches!(rejected, MutationOutcome::Rejected(_)));
    assert!(matches!(unknown, MutationOutcome::Unknown(_)));
    assert!(!stats.overflowed);
}

#[tokio::test]
async fn read_stats_are_lazy_and_distinguish_empty_missing_remote_and_transport_errors() {
    let listener = listener().await;
    let client = connect(&listener, ClientConfig::default()).await;
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let first = receive(&mut stream).await;
        assert!(
            matches!(first.request, KvRequest::Put { .. }),
            "unpolled GET sends nothing"
        );
        respond(
            &mut stream,
            first,
            KvResponse::Mutation(MutationOutcome::Committed),
        )
        .await;
        for response in [
            KvResponse::Value(None),
            KvResponse::Value(Some(Bytes::new())),
            KvResponse::Value(Some(Bytes::from_static(b"value"))),
            KvResponse::ReadError(ReadFailure(failure(FailureReason::Unavailable))),
        ] {
            let request = receive(&mut stream).await;
            assert!(matches!(request.request, KvRequest::Get { .. }));
            respond(&mut stream, request, response).await;
        }
        let request = receive(&mut stream).await;
        assert!(matches!(request.request, KvRequest::Get { .. }));
        // Actual EOF after a handed GET, without a reply.
    });
    let unpolled = client.get(Bytes::from_static(b"unpolled"));
    let put = client
        .put(Bytes::from_static(b"barrier"), Bytes::new())
        .expect("put")
        .await;
    let lazy = client.stats();
    drop(unpolled);
    let missing = client.get(Bytes::from_static(b"missing")).await;
    let empty = client.get(Bytes::from_static(b"empty")).await;
    let value = client.get(Bytes::from_static(b"value")).await;
    let remote = client.get(Bytes::from_static(b"remote")).await;
    let transport = client.get(Bytes::from_static(b"transport")).await;
    let invalid_get = client.get(Bytes::new()).await;
    let invalid_put = client.put(Bytes::new(), Bytes::new());
    let invalid_delete = client.delete(Bytes::new());
    client.close().await;
    peer.await.expect("owned peer joined");
    let stats = client.stats();
    assert_eq!(put, MutationOutcome::Committed);
    assert_eq!(lazy.get.attempted, 0);
    assert_eq!(missing.expect("missing read"), None);
    assert_eq!(empty.expect("empty read"), Some(Bytes::new()));
    assert_eq!(
        value.expect("value read"),
        Some(Bytes::from_static(b"value"))
    );
    assert!(matches!(remote, Err(ReadError::Remote(_))));
    assert!(matches!(transport, Err(ReadError::Transport(_))));
    assert!(invalid_get.is_err() && invalid_put.is_err() && invalid_delete.is_err());
    assert_eq!(
        (
            stats.get.attempted,
            stats.get.admitted,
            stats.get.admission_failed,
            stats.get.pending
        ),
        (6, 5, 1, 0)
    );
    assert_eq!(
        (
            stats.get.hit,
            stats.get.miss,
            stats.get.remote_error,
            stats.get.transport_error
        ),
        (2, 1, 1, 1)
    );
    assert_eq!(
        (
            stats.put.attempted,
            stats.put.admitted,
            stats.put.admission_failed,
            stats.put.committed
        ),
        (2, 1, 1, 1)
    );
    assert_eq!(
        (
            stats.delete.attempted,
            stats.delete.admitted,
            stats.delete.admission_failed,
            stats.delete.not_sent
        ),
        (1, 0, 1, 0)
    );
}

#[tokio::test]
async fn retained_stats_handle_is_shared_but_does_not_keep_the_connection_alive() {
    let listener = listener().await;
    let client = connect(&listener, ClientConfig::default()).await;
    let (mut first_stream, _) = listener.accept().await.expect("first accept");
    let independent = connect(&listener, ClientConfig::default()).await;
    let (mut second_stream, _) = listener.accept().await.expect("second accept");
    let (seen, received) = oneshot::channel();
    let first_peer = tokio::spawn(async move {
        let request = receive(&mut first_stream).await;
        assert!(matches!(request.request, KvRequest::Put { .. }));
        seen.send(()).expect("observer");
        expect_eof(&mut first_stream).await;
    });
    let second_peer = tokio::spawn(async move {
        let request = receive(&mut second_stream).await;
        respond(&mut second_stream, request, KvResponse::Value(None)).await;
        expect_eof(&mut second_stream).await;
    });
    let clone = client.clone();
    let stats = client.stats_handle();
    drop(
        clone
            .put(Bytes::from_static(b"handed"), Bytes::new())
            .expect("put"),
    );
    timeout(WAIT, received)
        .await
        .expect("handed request")
        .expect("peer observed");
    let shared = client.stats() == clone.stats();
    let independent_initial = independent.stats();
    drop(client);
    drop(clone);
    first_peer
        .await
        .expect("peer sees EOF with only stats retained");
    let separate_read = independent.get(Bytes::from_static(b"separate")).await;
    independent.close().await;
    second_peer.await.expect("second owned peer joined");
    let final_stats = stats.snapshot();
    assert!(shared);
    assert_eq!(independent_initial, ClientStatsSnapshot::default());
    assert_eq!(
        (
            final_stats.put.attempted,
            final_stats.put.admitted,
            final_stats.put.unknown,
            final_stats.put.pending
        ),
        (1, 1, 1, 0)
    );
    assert_eq!(final_stats.get.attempted, 0);
    assert_eq!(separate_read.expect("independent read"), None);
    assert_eq!(independent.stats().get.miss, 1);
    assert_eq!(independent.stats().put.attempted, 0);
}

#[tokio::test]
async fn completed_unpolled_result_has_zero_pending_but_retains_admission() {
    let listener = listener().await;
    let config = ClientConfig {
        link: LinkConfig {
            max_outstanding: 1,
            ..LinkConfig::default()
        },
        ..ClientConfig::default()
    };
    let client = connect(&listener, config).await;
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        for _ in 0..2 {
            let request = receive(&mut stream).await;
            respond(
                &mut stream,
                request,
                KvResponse::Mutation(MutationOutcome::Committed),
            )
            .await;
        }
        expect_eof(&mut stream).await;
    });
    let first = client
        .put(Bytes::from_static(b"first"), Bytes::new())
        .expect("put");
    let completed = wait_stats(&client.stats_handle(), |stats| stats.put.committed == 1).await;
    let blocked = client.put(Bytes::from_static(b"blocked"), Bytes::new());
    drop(first);
    let last = client
        .put(Bytes::from_static(b"last"), Bytes::new())
        .expect("permit released")
        .await;
    client.close().await;
    peer.await.expect("owned peer joined");
    let stats = client.stats();
    assert_eq!(completed.put.pending, 0);
    assert!(matches!(blocked, Err(AdmitError::CountLimit)));
    assert_eq!(last, MutationOutcome::Committed);
    assert_eq!(
        (
            stats.put.attempted,
            stats.put.admitted,
            stats.put.admission_failed,
            stats.put.committed,
            stats.put.pending
        ),
        (3, 2, 1, 2, 0)
    );
}

#[tokio::test]
async fn dropped_submitted_get_still_records_its_terminal_response() {
    let listener = listener().await;
    let client = connect(&listener, ClientConfig::default()).await;
    let (seen, observed) = oneshot::channel();
    let (release, resumed) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let request = receive(&mut stream).await;
        seen.send(()).expect("observer");
        resumed.await.expect("release reply");
        respond(&mut stream, request, KvResponse::Value(Some(Bytes::new()))).await;
        expect_eof(&mut stream).await;
    });
    let mut read = Box::pin(client.get(Bytes::from_static(b"abandoned")));
    let handed = timeout(WAIT, async {
        tokio::select! {
            _ = observed => true,
            _ = &mut read => false,
        }
    })
    .await
    .expect("GET submitted");
    drop(read);
    release.send(()).expect("release peer");
    let stats = wait_stats(&client.stats_handle(), |stats| stats.get.hit == 1).await;
    client.close().await;
    peer.await.expect("owned peer joined");
    assert!(handed);
    assert_eq!(
        (
            stats.get.attempted,
            stats.get.admitted,
            stats.get.pending,
            stats.get.hit
        ),
        (1, 1, 0, 1)
    );
}
