#![cfg_attr(test, allow(clippy::panic))]

use rockbee_client::{Client, MutationOutcome, Result};
use rockbee_protocol::transport::{FrameReader, read_frame, write_frame};
use rockbee_protocol::{Request, RequestEnvelope, Response, ResponseEnvelope};
use std::future::Future;
use std::task::{Context, Poll, Waker};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Duration, timeout};

fn ready_error<T, E>(future: impl Future<Output = std::result::Result<T, E>>) -> E {
    let mut future = std::pin::pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    match future.as_mut().poll(&mut context) {
        Poll::Ready(Err(error)) => error,
        Poll::Ready(Ok(_)) => panic!("empty input unexpectedly succeeded"),
        Poll::Pending => panic!("empty input must be rejected immediately before admission"),
    }
}

async fn connected() -> Result<(Client, TcpStream)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let client = Client::connect(&listener.local_addr()?.to_string()).await?;
    let (stream, _) = listener.accept().await?;
    Ok((client, stream))
}

// Removing any preflight must fail the local-ready assertion or this real socket
// transcript: only valid binary CRUD may arrive, with no extra request before EOF.
async fn assert_binary_crud_after_rejection(client: Client, stream: TcpStream) -> Result<()> {
    let peer = async move {
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);
        for (expected, response) in [
            (
                Request::Put {
                    key: vec![0],
                    value: vec![0, 255],
                },
                Response::Ok,
            ),
            (
                Request::Get { key: vec![0] },
                Response::Value {
                    value: Some(vec![0, 255]),
                },
            ),
            (Request::Delete { key: vec![0] }, Response::Ok),
        ] {
            let frame = read_frame(&mut input)
                .await?
                .ok_or("missing valid CRUD request")?;
            let request = RequestEnvelope::decode(&frame)?;
            assert_eq!(
                request.request, expected,
                "locally rejected input reached the wire"
            );
            write_frame(
                &mut output,
                &ResponseEnvelope {
                    request_id: request.request_id,
                    response,
                },
            )
            .await?;
        }
        assert!(
            read_frame(&mut input).await?.is_none(),
            "extra request reached the wire"
        );
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let operations = async move {
        client.put(&[0], &[0, 255], None).await?;
        assert_eq!(client.get(&[0]).await?, Some(vec![0, 255]));
        client.delete(&[0]).await?;
        drop(client);
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    timeout(Duration::from_secs(2), async {
        tokio::try_join!(peer, operations)
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn empty_put_keys_are_not_sent_with_or_without_ttl() -> Result<()> {
    let (client, stream) = connected().await?;
    for ttl in [None, Some(Duration::from_millis(1)), Some(Duration::ZERO)] {
        let error = ready_error(client.put(b"", b"value", ttl));
        assert_eq!(error.outcome(), MutationOutcome::NotSent);
        assert!(error.to_string().contains("key"));
    }
    assert_binary_crud_after_rejection(client, stream).await
}

#[tokio::test]
async fn empty_put_values_are_not_sent_with_or_without_ttl() -> Result<()> {
    let (client, stream) = connected().await?;
    for ttl in [None, Some(Duration::from_millis(1)), Some(Duration::ZERO)] {
        let error = ready_error(client.put(b"key", b"", ttl));
        assert_eq!(error.outcome(), MutationOutcome::NotSent);
        assert!(error.to_string().contains("value"));
    }
    assert_binary_crud_after_rejection(client, stream).await
}

#[tokio::test]
async fn empty_get_keys_are_rejected_without_sending() -> Result<()> {
    let (client, stream) = connected().await?;
    let error = ready_error(client.get(b""));
    assert!(error.to_string().contains("key"));
    assert_binary_crud_after_rejection(client, stream).await
}

#[tokio::test]
async fn empty_delete_keys_are_not_sent() -> Result<()> {
    let (client, stream) = connected().await?;
    let error = ready_error(client.delete(b""));
    assert_eq!(error.outcome(), MutationOutcome::NotSent);
    assert!(error.to_string().contains("key"));
    assert_binary_crud_after_rejection(client, stream).await
}
