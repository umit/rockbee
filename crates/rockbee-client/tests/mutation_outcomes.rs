use rockbee_client::{Result, RockBeeClient};
use rockbee_protocol::transport::{FrameReader, read_frame, write_frame};
use rockbee_protocol::{
    MAX_KEY_LEN, MutationError, MutationOutcome, RequestEnvelope, Response, ResponseEnvelope,
};
use tokio::{
    net::TcpListener,
    time::{Duration, timeout},
};

fn outcome(error: Box<dyn std::error::Error + Send + Sync>) -> MutationOutcome {
    error
        .downcast::<MutationError>()
        .expect("mutation error must expose typed certainty")
        .outcome()
}

#[tokio::test]
async fn local_preflight_is_not_sent_and_the_connection_remains_usable() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let client = RockBeeClient::connect(&listener.local_addr()?.to_string()).await?;
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);
        let request =
            RequestEnvelope::decode(&read_frame(&mut input).await.unwrap().unwrap()).unwrap();
        assert!(
            matches!(request.request, rockbee_protocol::Request::Get { .. }),
            "a rejected mutation reached the wire"
        );
        write_frame(
            &mut output,
            &ResponseEnvelope {
                request_id: request.request_id,
                response: Response::Value {
                    value: Some(b"alive".to_vec()),
                },
            },
        )
        .await
        .unwrap();
    });
    let key = vec![0; MAX_KEY_LEN + 1];
    assert_eq!(
        outcome(client.put(&key, b"v", None).await.unwrap_err().into()),
        MutationOutcome::NotSent
    );
    assert_eq!(
        outcome(client.delete(&key).await.unwrap_err().into()),
        MutationOutcome::NotSent
    );
    assert_eq!(
        outcome(
            client
                .put(b"k", b"v", Some(Duration::MAX))
                .await
                .unwrap_err()
                .into()
        ),
        MutationOutcome::NotSent
    );
    assert_eq!(client.get(b"still-usable").await?, Some(b"alive".to_vec()));
    timeout(Duration::from_secs(2), peer).await??;
    Ok(())
}

#[tokio::test]
async fn explicit_rejection_and_legacy_error_are_distinct_without_message_parsing() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let client = RockBeeClient::connect(&listener.local_addr()?.to_string()).await?;
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);
        for status in [
            MutationOutcome::Rejected,
            MutationOutcome::Unknown,
            MutationOutcome::Rejected,
            MutationOutcome::Unknown,
        ] {
            let request =
                RequestEnvelope::decode(&read_frame(&mut input).await.unwrap().unwrap()).unwrap();
            write_frame(
                &mut output,
                &ResponseEnvelope {
                    request_id: request.request_id,
                    response: Response::Error {
                        message: "same diagnostic text".into(),
                        mutation_outcome: status,
                    },
                },
            )
            .await
            .unwrap();
        }
    });
    assert_eq!(
        outcome(client.put(b"k", b"v", None).await.unwrap_err().into()),
        MutationOutcome::Rejected
    );
    assert_eq!(
        outcome(client.put(b"k", b"v", None).await.unwrap_err().into()),
        MutationOutcome::Unknown
    );
    assert_eq!(
        outcome(client.delete(b"k").await.unwrap_err().into()),
        MutationOutcome::Rejected
    );
    assert_eq!(
        outcome(client.delete(b"k").await.unwrap_err().into()),
        MutationOutcome::Unknown
    );
    timeout(Duration::from_secs(2), peer).await??;
    Ok(())
}

#[tokio::test]
async fn delivery_without_a_reply_is_unknown_and_is_not_replayed() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let client = RockBeeClient::connect(&listener.local_addr()?.to_string()).await?;
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut input = FrameReader::new(stream);
        let request =
            RequestEnvelope::decode(&read_frame(&mut input).await.unwrap().unwrap()).unwrap();
        assert!(matches!(
            request.request,
            rockbee_protocol::Request::Put { .. }
        ));
        drop(input); // Request received; no acknowledgement.
        assert!(
            timeout(Duration::from_millis(150), listener.accept())
                .await
                .is_err(),
            "mutation was replayed"
        );
    });
    assert_eq!(
        outcome(
            timeout(Duration::from_secs(2), client.put(b"k", b"v", None))
                .await?
                .unwrap_err()
                .into()
        ),
        MutationOutcome::Unknown
    );
    timeout(Duration::from_secs(2), peer).await??;
    Ok(())
}
