use rockbee_client::{ClientConfig, MutationOutcome, Result, RockBeeClient};
use rockbee_protocol::transport::{FrameReader, read_frame, write_frame};
use rockbee_protocol::{Request, RequestEnvelope, Response, ResponseEnvelope};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::time::{Duration, timeout};

#[test]
fn defaults_preserve_the_existing_client_limits() {
    let config = ClientConfig::default();
    assert_eq!(config.request_timeout(), Duration::from_secs(5));
    assert_eq!(config.max_in_flight(), 128);
    assert_eq!(config.max_pending(), 128);
}

#[test]
fn client_config_rejects_unusable_limits_before_connecting() {
    assert!(ClientConfig::new(Duration::ZERO, 1, 1).is_err());
    assert!(ClientConfig::new(Duration::MAX, 1, 1).is_err());
    assert!(ClientConfig::new(Duration::from_secs(1), 0, 1).is_err());
    assert!(ClientConfig::new(Duration::from_secs(1), 1, 0).is_err());
    assert!(ClientConfig::new(Duration::from_secs(1), 129, 1).is_err());
    assert!(
        ClientConfig::new(
            Duration::from_secs(1),
            1,
            tokio::sync::Semaphore::MAX_PERMITS + 1,
        )
        .is_err()
    );
}

#[tokio::test]
async fn configured_active_and_pending_limits_reject_excess_put_locally() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?.to_string();
    let (first_received, first_received_waiter) = oneshot::channel();
    let (release_first, release_first_waiter) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);

        let first =
            RequestEnvelope::decode(&read_frame(&mut input).await?.ok_or("missing first PUT")?)?;
        first_received
            .send(())
            .map_err(|_| "first PUT waiter dropped")?;
        release_first_waiter
            .await
            .map_err(|_| "first PUT release dropped")?;
        write_frame(
            &mut output,
            &ResponseEnvelope {
                request_id: first.request_id,
                response: Response::Ok,
            },
        )
        .await?;

        let second =
            RequestEnvelope::decode(&read_frame(&mut input).await?.ok_or("missing second PUT")?)?;
        write_frame(
            &mut output,
            &ResponseEnvelope {
                request_id: second.request_id,
                response: Response::Ok,
            },
        )
        .await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let config = ClientConfig::new(Duration::from_secs(1), 1, 1)?;
    let client = RockBeeClient::connect_with_config(&address, config).await?;
    let first = client.put(b"first", b"one", None);
    timeout(Duration::from_secs(1), first_received_waiter).await??;
    let second = client.put(b"second", b"two", None);
    let rejected = timeout(
        Duration::from_millis(100),
        client.put(b"third", b"three", None),
    )
    .await?
    .expect_err("third PUT must exceed the configured admission budget");
    assert_eq!(rejected.outcome(), MutationOutcome::NotSent);
    assert!(rejected.to_string().contains("pending limit"));

    release_first
        .send(())
        .map_err(|_| "first PUT peer stopped")?;
    first.await?;
    second.await?;
    client.shutdown().await;
    peer.await??;
    Ok(())
}

#[tokio::test]
async fn configured_timeout_applies_to_put_without_replaying_the_mutation() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?.to_string();
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let (input, _) = stream.into_split();
        let mut input = FrameReader::new(input);
        let request =
            RequestEnvelope::decode(&read_frame(&mut input).await?.ok_or("missing PUT")?)?;
        assert!(matches!(request.request, Request::Put { .. }));

        let (next_frame, next_connection) = tokio::join!(
            timeout(Duration::from_millis(300), read_frame(&mut input)),
            timeout(Duration::from_millis(300), listener.accept()),
        );
        assert!(
            !matches!(next_frame, Ok(Ok(Some(_)))),
            "timed-out PUT was replayed on its connection"
        );
        assert!(
            next_connection.is_err(),
            "timed-out PUT opened a replay connection"
        );
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let config = ClientConfig::new(Duration::from_millis(150), 1, 1)?;
    let client = RockBeeClient::connect_with_config(&address, config).await?;
    let error = timeout(Duration::from_secs(2), client.put(b"put", b"value", None))
        .await?
        .expect_err("peer withholds the PUT acknowledgement");
    assert_eq!(error.outcome(), MutationOutcome::Unknown);
    peer.await??;
    Ok(())
}

#[tokio::test]
async fn configured_timeout_applies_to_get() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?.to_string();
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let (input, _) = stream.into_split();
        let mut input = FrameReader::new(input);
        let request =
            RequestEnvelope::decode(&read_frame(&mut input).await?.ok_or("missing GET")?)?;
        assert!(matches!(request.request, Request::Get { .. }));
        tokio::time::sleep(Duration::from_millis(300)).await;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let config = ClientConfig::new(Duration::from_millis(150), 1, 1)?;
    let client = RockBeeClient::connect_with_config(&address, config).await?;
    timeout(Duration::from_secs(2), client.get(b"get"))
        .await?
        .expect_err("peer withholds the GET response");
    peer.await??;
    Ok(())
}

#[tokio::test]
async fn configured_timeout_applies_to_delete_without_replaying_the_mutation() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?.to_string();
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let (input, _) = stream.into_split();
        let mut input = FrameReader::new(input);
        let request =
            RequestEnvelope::decode(&read_frame(&mut input).await?.ok_or("missing DELETE")?)?;
        assert!(matches!(request.request, Request::Delete { .. }));

        let (next_frame, next_connection) = tokio::join!(
            timeout(Duration::from_millis(300), read_frame(&mut input)),
            timeout(Duration::from_millis(300), listener.accept()),
        );
        assert!(
            !matches!(next_frame, Ok(Ok(Some(_)))),
            "timed-out DELETE was replayed on its connection"
        );
        assert!(
            next_connection.is_err(),
            "timed-out DELETE opened a replay connection"
        );
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let config = ClientConfig::new(Duration::from_millis(150), 1, 1)?;
    let client = RockBeeClient::connect_with_config(&address, config).await?;
    let error = timeout(Duration::from_secs(2), client.delete(b"delete"))
        .await?
        .expect_err("peer withholds the DELETE acknowledgement");
    assert_eq!(error.outcome(), MutationOutcome::Unknown);
    peer.await??;
    Ok(())
}
