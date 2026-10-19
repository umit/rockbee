use std::time::Duration;

use bytes::Bytes;
use rockbee_raft_client::{ClientConfig, RockBeeRaftClient};
use rockbee_raft_protocol::{
    KvRequest, KvResponse, MutationOutcome, RequestEnvelope, ResponseEnvelope, VERSION,
    decode_frame, encode_frame,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};

async fn receive(stream: &mut TcpStream) -> RequestEnvelope {
    let size = stream.read_u32().await.expect("frame header") as usize;
    let mut frame = Vec::from((size as u32).to_be_bytes());
    frame.resize(size + 4, 0);
    stream
        .read_exact(&mut frame[4..])
        .await
        .expect("frame payload");
    decode_frame(&frame).expect("valid request")
}

async fn respond(stream: &mut TcpStream, request: &RequestEnvelope, response: KvResponse) {
    let frame = encode_frame(&ResponseEnvelope {
        version: VERSION,
        request_id: request.request_id,
        response,
    })
    .expect("valid response");
    stream.write_all(&frame).await.expect("response write");
}

#[tokio::test]
async fn eager_unpolled_and_dropped_mutations_reach_the_peer() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("owned port");
    let address = listener.local_addr().expect("address");
    let (observed, observations) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let first = receive(&mut stream).await;
        let second = receive(&mut stream).await;
        assert!(matches!(first.request, KvRequest::Put { .. }));
        assert!(matches!(second.request, KvRequest::Delete { .. }));
        observed.send(()).expect("observation receiver");
        respond(
            &mut stream,
            &second,
            KvResponse::Mutation(MutationOutcome::Committed),
        )
        .await;
        respond(
            &mut stream,
            &first,
            KvResponse::Mutation(MutationOutcome::Committed),
        )
        .await;
    });
    let client = RockBeeRaftClient::connect(address, ClientConfig::default())
        .await
        .expect("connect");
    let first = client
        .put(Bytes::from_static(b"key"), Bytes::from_static(b"value"))
        .expect("admit put");
    drop(
        client
            .delete(Bytes::from_static(b"key"))
            .expect("admit delete"),
    );
    tokio::time::timeout(Duration::from_secs(2), observations)
        .await
        .expect("eager delivery")
        .expect("observed");
    assert_eq!(first.await, MutationOutcome::Committed);
    client.close().await;
    peer.await.expect("peer");
}

#[tokio::test]
async fn reads_distinguish_missing_empty_binary_and_remote_error() {
    use rockbee_raft_client::ReadError;
    use rockbee_raft_protocol::{Failure, FailureReason, ReadFailure};
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("owned port");
    let address = listener.local_addr().expect("address");
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        for response in [
            KvResponse::Value(None),
            KvResponse::Value(Some(Bytes::new())),
            KvResponse::Value(Some(Bytes::from_static(b"\0\xffbinary"))),
            KvResponse::ReadError(ReadFailure(Failure {
                reason: FailureReason::Unavailable,
                message: "reader unavailable".to_owned(),
            })),
        ] {
            let request = receive(&mut stream).await;
            assert!(matches!(request.request, KvRequest::Get { .. }));
            respond(&mut stream, &request, response).await;
        }
    });
    let client = RockBeeRaftClient::connect(address, ClientConfig::default())
        .await
        .expect("connect");
    assert_eq!(
        client
            .get(Bytes::from_static(b"missing"))
            .await
            .expect("read"),
        None
    );
    assert_eq!(
        client
            .get(Bytes::from_static(b"empty"))
            .await
            .expect("read"),
        Some(Bytes::new())
    );
    assert_eq!(
        client
            .get(Bytes::from_static(b"binary"))
            .await
            .expect("read"),
        Some(Bytes::from_static(b"\0\xffbinary"))
    );
    assert!(matches!(
        client.get(Bytes::from_static(b"error")).await,
        Err(ReadError::Remote(_))
    ));
    client.close().await;
    peer.await.expect("peer");
}

#[tokio::test]
async fn explicit_rejection_is_preserved_but_missing_reply_is_unknown_and_not_replayed() {
    use rockbee_raft_client::AdmitError;
    use rockbee_raft_protocol::{Failure, FailureReason};
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("owned port");
    let address = listener.local_addr().expect("address");
    let (observed, observations) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let request = receive(&mut stream).await;
        respond(
            &mut stream,
            &request,
            KvResponse::Mutation(MutationOutcome::Rejected(Failure {
                reason: FailureReason::Overloaded,
                message: "not submitted".to_owned(),
            })),
        )
        .await;
        let _uncertain = receive(&mut stream).await;
        observed.send(()).expect("observed");
        let mut rest = Vec::new();
        stream.read_to_end(&mut rest).await.expect("closed stream");
        assert!(rest.is_empty(), "there must be no automatic replay");
    });
    let config = ClientConfig {
        request_timeout: Duration::from_millis(100),
        ..ClientConfig::default()
    };
    let client = RockBeeRaftClient::connect(address, config)
        .await
        .expect("connect");
    assert!(matches!(
        client
            .delete(Bytes::from_static(b"rejected"))
            .expect("admit")
            .await,
        MutationOutcome::Rejected(_)
    ));
    let uncertain = client
        .put(Bytes::from_static(b"key"), Bytes::from_static(b"value"))
        .expect("admit");
    observations.await.expect("peer saw mutation");
    assert!(matches!(uncertain.await, MutationOutcome::Unknown(_)));
    assert!(matches!(
        client.delete(Bytes::from_static(b"later")),
        Err(AdmitError::Closed)
    ));
    client.close().await;
    peer.await.expect("peer");
}

#[tokio::test]
async fn invalid_timeout_and_invalid_body_fail_locally() {
    use rockbee_raft_client::{AdmitError, ClientConnectError};
    let address = "127.0.0.1:1".parse().expect("address");
    assert!(matches!(
        RockBeeRaftClient::connect(
            address,
            ClientConfig {
                request_timeout: Duration::ZERO,
                ..ClientConfig::default()
            }
        )
        .await,
        Err(ClientConnectError::InvalidTimeout)
    ));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("owned port");
    let address = listener.local_addr().expect("address");
    let client = RockBeeRaftClient::connect(address, ClientConfig::default())
        .await
        .expect("connect");
    assert!(matches!(
        client.put(Bytes::new(), Bytes::new()),
        Err(AdmitError::InvalidRequest(_))
    ));
    assert!(matches!(
        client.delete(Bytes::new()),
        Err(AdmitError::InvalidRequest(_))
    ));
    client.close().await;
    let (mut peer, _) = listener.accept().await.expect("accept");
    let mut received = Vec::new();
    peer.read_to_end(&mut received)
        .await
        .expect("closed stream");
    assert!(received.is_empty());
}
