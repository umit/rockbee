//! A backend Error that fits its internal correlation ID must not make a larger
//! public correlation ID overflow the gateway's egress frame or retire the peer.

use rockbee_gateway::Gateway;
use rockbee_test_support::Result;
use rockbee_test_support::protocol::{
    MAX_FRAME, Request, RequestEnvelope, Response, ResponseEnvelope,
};
use rockbee_test_support::transport::{FrameReader, read_frame, write_frame};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;

const BOUND: Duration = Duration::from_secs(5);
const FIRST_ID: u64 = u64::MAX - 1;
const SECOND_ID: u64 = u64::MAX;
const SECOND_VALUE: &[u8] = &[0, 255, 7];
const PUBLIC_LIMIT_ERROR: &str = "RPC response exceeds 1 MiB frame limit";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gateway_bounds_oversized_backend_error_without_retiring_max_id_peer() -> Result<()> {
    let writer_reservation = TcpListener::bind("127.0.0.1:0").await?;
    let reader = TcpListener::bind("127.0.0.1:0").await?;
    let reader_address = reader.local_addr()?.to_string();

    let gateway = Gateway::bind(
        "127.0.0.1:0",
        writer_reservation.local_addr()?.to_string(),
        vec![reader_address],
    )
    .await?;
    let address = gateway.local_addr()?;
    let (stop, stopped) = oneshot::channel();
    let gateway = tokio::spawn(gateway.run_until(async move {
        stopped.await.map_err(|_| "gateway stop sender dropped")?;
        Ok(())
    }));
    let backend = tokio::spawn(scripted_reader(reader));

    let body: Result<()> = match timeout(Duration::from_secs(10), async {
        let stream = timeout(BOUND, TcpStream::connect(address))
            .await
            .map_err(|_| "public gateway connect timed out")??;
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);

        exchange_get(&mut output, FIRST_ID, b"boundary/first").await?;
        let first = read_response(&mut input).await?;
        if first.request_id != FIRST_ID
            || first.response
                != (Response::Error {
                    mutation_outcome: rockbee_protocol::MutationOutcome::Unknown,
                    message: PUBLIC_LIMIT_ERROR.into(),
                })
        {
            return Err("gateway did not return the bounded correlated Error".into());
        }

        exchange_get(&mut output, SECOND_ID, b"boundary/second").await?;
        let second = read_response(&mut input).await?;
        if second.request_id != SECOND_ID
            || second.response
                != (Response::Value {
                    value: Some(SECOND_VALUE.to_vec()),
                })
        {
            return Err(
                "gateway did not keep the public connection for the exact second Value".into(),
            );
        }
        Ok(())
    })
    .await
    {
        Ok(result) => result,
        Err(_) => Err("ten-second aggregate test body timeout".into()),
    };

    let _ = stop.send(());
    let gateway_cleanup = join("gateway", gateway).await;
    let backend_cleanup = join("scripted reader", backend).await;
    let mut failures = Vec::new();
    if let Err(error) = body {
        failures.push(format!("test failed: {error}"));
    }
    if let Err(error) = gateway_cleanup {
        failures.push(format!("gateway cleanup failed: {error}"));
    }
    if let Err(error) = backend_cleanup {
        failures.push(format!("backend cleanup failed: {error}"));
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; ").into())
    }
}

async fn exchange_get(
    output: &mut tokio::net::tcp::OwnedWriteHalf,
    request_id: u64,
    key: &[u8],
) -> Result<()> {
    timeout(
        BOUND,
        write_frame(
            output,
            &RequestEnvelope {
                request_id,
                request: Request::Get { key: key.to_vec() },
            },
        ),
    )
    .await
    .map_err(|_| "public request write timed out")??;
    Ok(())
}

async fn read_response(
    input: &mut FrameReader<tokio::net::tcp::OwnedReadHalf>,
) -> Result<ResponseEnvelope> {
    let frame = timeout(BOUND, read_frame(input))
        .await
        .map_err(|_| "public response read timed out")??
        .ok_or("gateway closed the public connection")?;
    ResponseEnvelope::decode(&frame)
}

async fn scripted_reader(listener: TcpListener) -> Result<()> {
    let (stream, _) = timeout(BOUND, listener.accept())
        .await
        .map_err(|_| "gateway did not connect to scripted reader")??;
    let (input, mut output) = stream.into_split();
    let mut input = FrameReader::new(input);

    let first = read_request(&mut input).await?;
    if first.request_id != 1
        || first.request
            != (Request::Get {
                key: b"boundary/first".to_vec(),
            })
    {
        return Err("gateway did not send the first expected internal GET".into());
    }
    // Frame + version/ID/oneof/nested-message overhead is 17 bytes at ID 1.
    let oversized = ResponseEnvelope {
        request_id: 1,
        response: Response::Error {
            mutation_outcome: rockbee_protocol::MutationOutcome::Unknown,
            message: "E".repeat(MAX_FRAME - 17),
        },
    };
    if rockbee_test_support::protocol::encode_frame(&oversized)?.len() != MAX_FRAME {
        return Err("oversized backend Error fixture did not exactly fill MAX_FRAME".into());
    }
    timeout(BOUND, write_frame(&mut output, &oversized))
        .await
        .map_err(|_| "oversized internal Error write timed out")??;

    let second = read_request(&mut input).await?;
    if second.request_id != 2
        || second.request
            != (Request::Get {
                key: b"boundary/second".to_vec(),
            })
    {
        return Err("gateway did not send the second expected internal GET".into());
    }
    timeout(
        BOUND,
        write_frame(
            &mut output,
            &ResponseEnvelope {
                request_id: 2,
                response: Response::Value {
                    value: Some(SECOND_VALUE.to_vec()),
                },
            },
        ),
    )
    .await
    .map_err(|_| "second internal Value write timed out")??;

    let eof = timeout(BOUND, read_frame(&mut input))
        .await
        .map_err(|_| "scripted reader did not observe gateway EOF after shutdown")??;
    if eof.is_some() {
        return Err("gateway sent an unexpected third backend request".into());
    }
    Ok(())
}

async fn read_request(
    input: &mut FrameReader<tokio::net::tcp::OwnedReadHalf>,
) -> Result<RequestEnvelope> {
    let frame = timeout(BOUND, read_frame(input))
        .await
        .map_err(|_| "scripted reader request read timed out")??
        .ok_or("gateway closed before expected backend request")?;
    RequestEnvelope::decode(&frame)
}

async fn join<T>(name: &str, mut task: JoinHandle<Result<T>>) -> Result<()>
where
    T: Send + 'static,
{
    match timeout(BOUND, &mut task).await {
        Ok(Ok(Ok(_))) => Ok(()),
        Ok(Ok(Err(error))) => Err(error),
        Ok(Err(error)) => Err(format!("{name} join failed: {error}").into()),
        Err(_) => {
            task.abort();
            let _ = task.await;
            Err(format!("{name} did not stop within cleanup bound").into())
        }
    }
}
