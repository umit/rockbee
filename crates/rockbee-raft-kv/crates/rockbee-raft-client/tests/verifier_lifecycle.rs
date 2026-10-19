use std::time::Duration;

use bytes::Bytes;
use rockbee_raft_client::{AdmitError, ClientConfig, LinkConfig, RockBeeRaftClient};
use rockbee_raft_protocol::{
    KvRequest, KvResponse, MutationOutcome, RequestEnvelope, ResponseEnvelope, VERSION,
    decode_frame, encode_frame,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    time::timeout,
};

async fn receive(stream: &mut TcpStream) -> RequestEnvelope {
    let length = stream.read_u32().await.expect("frame header");
    let mut frame = length.to_be_bytes().to_vec();
    frame.resize(usize::try_from(length).expect("fixture length") + 4, 0);
    stream
        .read_exact(&mut frame[4..])
        .await
        .expect("frame body");
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
async fn clones_share_admission_drop_preserves_owner_and_close_retires_all() {
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("owned port");
        let address = listener.local_addr().expect("address");
        let (release_first, first_released) = oneshot::channel();
        let (second_observed, observed_second) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let first = receive(&mut stream).await;
            first_released.await.expect("release first response");
            respond(
                &mut stream,
                &first,
                KvResponse::Mutation(MutationOutcome::Committed),
            )
            .await;
            let second = receive(&mut stream).await;
            assert!(matches!(second.request, KvRequest::Delete { .. }));
            second_observed.send(()).expect("second observation");
            let mut rest = Vec::new();
            stream
                .read_to_end(&mut rest)
                .await
                .expect("connection closes");
            assert!(rest.is_empty(), "closing must not replay the mutation");
        });
        let original = RockBeeRaftClient::connect(
            address,
            ClientConfig {
                link: LinkConfig {
                    max_outstanding: 1,
                    ..LinkConfig::default()
                },
                ..ClientConfig::default()
            },
        )
        .await
        .expect("connect");
        let survivor = original.clone();
        let first = original
            .put(Bytes::from_static(b"first"), Bytes::new())
            .expect("admit first");
        assert!(matches!(
            survivor.delete(Bytes::from_static(b"full")),
            Err(AdmitError::CountLimit)
        ));
        drop(original);
        release_first.send(()).expect("release peer");
        assert_eq!(first.await, MutationOutcome::Committed);
        let closing_clone = survivor.clone();
        let second = survivor
            .delete(Bytes::from_static(b"second"))
            .expect("remaining clone stays usable");
        observed_second.await.expect("second handed mutation");
        closing_clone.close().await;
        assert!(matches!(second.await, MutationOutcome::Unknown(_)));
        assert!(matches!(
            survivor.delete(Bytes::from_static(b"closed")),
            Err(AdmitError::Closed)
        ));
        peer.await.expect("peer joins");
    })
    .await
    .expect("bounded lifecycle test");
}

#[tokio::test]
async fn unpolled_get_does_not_precede_an_eager_mutation() {
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("owned port");
        let address = listener.local_addr().expect("address");
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let first = receive(&mut stream).await;
            assert!(
                matches!(first.request, KvRequest::Put { .. }),
                "unpolled GET must not have entered the FIFO writer queue"
            );
            respond(
                &mut stream,
                &first,
                KvResponse::Mutation(MutationOutcome::Committed),
            )
            .await;
            let second = receive(&mut stream).await;
            assert!(matches!(second.request, KvRequest::Get { .. }));
            respond(
                &mut stream,
                &second,
                KvResponse::Value(Some(Bytes::from_static(b"observed"))),
            )
            .await;
        });
        let client = RockBeeRaftClient::connect(address, ClientConfig::default())
            .await
            .expect("connect");
        let lazy_get = client.get(Bytes::from_static(b"lazy"));
        let eager_put = client
            .put(Bytes::from_static(b"marker"), Bytes::new())
            .expect("eager admission");
        assert_eq!(eager_put.await, MutationOutcome::Committed);
        assert_eq!(
            lazy_get.await.expect("polling starts GET"),
            Some(Bytes::from_static(b"observed"))
        );
        client.close().await;
        peer.await.expect("peer joins");
    })
    .await
    .expect("bounded lazy GET test");
}
