use rockbee_client::{Client, MutationOutcome, Result};
use rockbee_protocol::transport::{FrameReader, read_frame, write_frame};
use rockbee_protocol::{Request, RequestEnvelope, Response, ResponseEnvelope};
use tokio::net::TcpListener;
use tokio::time::{Duration, timeout};

const BOUND: Duration = Duration::from_secs(8);

fn poll_once<F: Future + Unpin>(future: &mut F) -> std::task::Poll<F::Output> {
    std::pin::Pin::new(future).poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
}

#[tokio::test]
async fn shutdown_drains_active_and_waiting_puts_without_polling_their_handles() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let client = Client::connect(&listener.local_addr()?.to_string()).await?;
    let clone = client.clone();
    let mut handles = Vec::new();
    // No peer can ACK yet: this fills active and waiting admissions.
    for sequence in 0_u16..256 {
        let handle = client.put(&sequence.to_be_bytes(), b"value\0\xff", None);
        if sequence % 2 == 0 {
            drop(handle);
        } else {
            handles.push(handle);
        }
    }
    let lazy_get = clone.get(b"not-started");
    let lazy_delete = clone.delete(b"not-started");
    let mut shutdown = Box::pin(client.shutdown());
    assert!(
        poll_once(&mut shutdown).is_pending(),
        "shutdown must drain accepted PUTs"
    );
    assert_eq!(
        clone
            .put(b"too-late", b"value", None)
            .await
            .unwrap_err()
            .outcome(),
        MutationOutcome::NotSent
    );
    assert!(lazy_get.await.is_err());
    assert_eq!(
        lazy_delete.await.unwrap_err().outcome(),
        MutationOutcome::NotSent
    );

    let peer = async {
        let (stream, _) = listener.accept().await?;
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);
        let mut seen = [false; 256];
        for _ in 0..256 {
            let frame = read_frame(&mut input).await?.ok_or("closed before drain")?;
            let request = RequestEnvelope::decode(&frame)?;
            let Request::Put { key, value } = request.request else {
                panic!("only accepted PUTs may reach the peer");
            };
            assert_eq!(key.len(), 2, "a post-shutdown request was sent");
            let sequence = usize::from(u16::from_be_bytes([key[0], key[1]]));
            assert!(sequence < seen.len());
            assert!(!seen[sequence], "an accepted PUT was replayed");
            seen[sequence] = true;
            assert_eq!(value, b"value\0\xff");
            write_frame(
                &mut output,
                &ResponseEnvelope {
                    request_id: request.request_id,
                    response: Response::Ok,
                },
            )
            .await?;
        }
        assert!(seen.into_iter().all(|received| received));
        assert!(
            read_frame(&mut input).await?.is_none(),
            "shutdown must close TCP even while Client clones live"
        );
        Result::<()>::Ok(())
    };
    timeout(BOUND, async {
        let ((), result) = tokio::join!(shutdown, peer);
        result?;
        for handle in handles {
            // oneshot::poll may yield solely because this task used its Tokio
            // cooperative budget. Inspect readiness without that scheduler yield.
            let result = poll_once(&mut tokio::task::unconstrained(handle));
            assert!(
                matches!(result, std::task::Poll::Ready(Ok(()))),
                "shutdown returned without a successful PUT result: {result:?}"
            );
        }
        clone.shutdown().await;
        client.shutdown().await;
        assert!(client.get(b"closed").await.is_err());
        assert_eq!(
            clone.delete(b"closed").await.unwrap_err().outcome(),
            MutationOutcome::NotSent
        );
        Result::<()>::Ok(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn cancelled_and_concurrent_shutdowns_preserve_started_get_and_delete() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let client = Client::connect(&listener.local_addr()?.to_string()).await?;
    let clone = client.clone();
    let mut get = Box::pin(client.get(b"read"));
    let mut delete = Box::pin(clone.delete(b"delete"));
    assert!(poll_once(&mut get).is_pending());
    assert!(poll_once(&mut delete).is_pending());
    let mut cancelled = Box::pin(client.shutdown());
    assert!(poll_once(&mut cancelled).is_pending());
    drop(cancelled);
    assert_eq!(
        clone
            .put(b"closed", b"value", None)
            .await
            .unwrap_err()
            .outcome(),
        MutationOutcome::NotSent
    );

    let peer = async {
        let (stream, _) = listener.accept().await?;
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);
        let mut requests = Vec::new();
        for _ in 0..2 {
            let frame = read_frame(&mut input)
                .await?
                .ok_or("shutdown cancelled admitted work")?;
            requests.push(RequestEnvelope::decode(&frame)?);
        }
        assert!(
            requests
                .iter()
                .any(|request| matches!(&request.request, Request::Get { key } if key == b"read"))
        );
        assert!(requests.iter().any(
            |request| matches!(&request.request, Request::Delete { key } if key == b"delete")
        ));
        for request in requests.into_iter().rev() {
            let response = match request.request {
                Request::Get { .. } => Response::Value {
                    value: Some(b"exact\0bytes".to_vec()),
                },
                Request::Delete { .. } => Response::Ok,
                _ => panic!("unexpected request after shutdown"),
            };
            write_frame(
                &mut output,
                &ResponseEnvelope {
                    request_id: request.request_id,
                    response,
                },
            )
            .await?;
        }
        assert!(read_frame(&mut input).await?.is_none());
        Result::<()>::Ok(())
    };
    timeout(BOUND, async {
        let (get_result, delete_result, (), (), peer_result) =
            tokio::join!(get, delete, client.shutdown(), clone.shutdown(), peer);
        assert_eq!(get_result?, Some(b"exact\0bytes".to_vec()));
        delete_result?;
        peer_result?;
        Result::<()>::Ok(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn shutdown_finishes_after_connection_loss_without_replaying_put() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let client = Client::connect(&listener.local_addr()?.to_string()).await?;
    let put = client.put(b"uncertain", b"value", None);
    let peer = async {
        let (stream, _) = listener.accept().await?;
        let mut input = FrameReader::new(stream);
        let frame = read_frame(&mut input)
            .await?
            .ok_or("PUT was not delivered")?;
        assert!(matches!(
            RequestEnvelope::decode(&frame)?.request,
            Request::Put { .. }
        ));
        drop(input); // Delivery happened, but the ACK is lost.
        assert!(
            timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err(),
            "shutdown replayed a mutation"
        );
        Result::<()>::Ok(())
    };
    timeout(BOUND, async {
        let ((), peer_result) = tokio::join!(client.shutdown(), peer);
        peer_result?;
        assert_eq!(put.await.unwrap_err().outcome(), MutationOutcome::Unknown);
        assert_eq!(
            client
                .put(b"closed", b"value", None)
                .await
                .unwrap_err()
                .outcome(),
            MutationOutcome::NotSent
        );
        Result::<()>::Ok(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn shutdown_of_idle_client_closes_socket_and_is_repeatable() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let client = Client::connect(&listener.local_addr()?.to_string()).await?;
    let clone = client.clone();
    timeout(BOUND, async {
        client.shutdown().await;
        clone.shutdown().await;
        let (stream, _) = listener.accept().await?;
        assert!(read_frame(&mut FrameReader::new(stream)).await?.is_none());
        Result::<()>::Ok(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn shutdown_preserves_request_deadline_when_peer_never_acknowledges() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let client = Client::connect(&listener.local_addr()?.to_string()).await?;
    let put = client.put(b"unacknowledged", b"value", None);
    let peer = async {
        let (stream, _) = listener.accept().await?;
        let mut input = FrameReader::new(stream);
        let frame = read_frame(&mut input)
            .await?
            .ok_or("PUT was not delivered")?;
        assert!(matches!(
            RequestEnvelope::decode(&frame)?.request,
            Request::Put { .. }
        ));
        // Keep the connection open without an ACK. The normal request deadline
        // must terminate the operation and let shutdown drain it.
        assert!(read_frame(&mut input).await?.is_none());
        assert!(
            timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err(),
            "shutdown replayed the timed-out mutation"
        );
        Result::<()>::Ok(())
    };
    timeout(BOUND, async {
        let ((), peer_result) = tokio::join!(client.shutdown(), peer);
        peer_result?;
        assert_eq!(put.await.unwrap_err().outcome(), MutationOutcome::Unknown);
        Result::<()>::Ok(())
    })
    .await??;
    Ok(())
}

#[test]
fn put_result_survives_runtime_teardown_after_shutdown() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let (client, mut handle) = runtime.block_on(drain_put_on_current_runtime())?;
    drop(runtime);
    assert!(
        matches!(poll_once(&mut handle), std::task::Poll::Ready(Ok(()))),
        "drained PUT must retain its result after the connect runtime stops"
    );
    drop(client);
    Ok(())
}

async fn drain_put_on_current_runtime() -> Result<(Client, rockbee_client::PutHandle)> {
    timeout(BOUND, async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let client = Client::connect(&listener.local_addr()?.to_string()).await?;
        let handle = client.put(b"before-shutdown", b"value", None);
        let peer = async {
            let (stream, _) = listener.accept().await?;
            let (input, mut output) = stream.into_split();
            let mut input = FrameReader::new(input);
            let frame = read_frame(&mut input).await?.ok_or("PUT not delivered")?;
            let request = RequestEnvelope::decode(&frame)?;
            assert_eq!(
                request.request,
                Request::Put {
                    key: b"before-shutdown".to_vec(),
                    value: b"value".to_vec(),
                }
            );
            write_frame(
                &mut output,
                &ResponseEnvelope {
                    request_id: request.request_id,
                    response: Response::Ok,
                },
            )
            .await?;
            assert!(read_frame(&mut input).await?.is_none());
            Result::<()>::Ok(())
        };
        let ((), result) = tokio::join!(client.shutdown(), peer);
        result?;
        Ok((client, handle))
    })
    .await?
}
