use super::*;
use rockbee_client::Client;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::AsyncReadExt;

#[tokio::test]
async fn writing_response_leaves_admission_for_the_next_request() -> Result<()> {
    let connection = Arc::new(Semaphore::new(1));
    let process = Arc::new(Semaphore::new(1));
    let (send, responses) = mpsc::channel(1);
    send.send(AdmittedResponse {
        envelope: ResponseEnvelope {
            request_id: 1,
            response: Response::Value {
                value: Some(vec![0xff; 1024]),
            },
        },
        _permit: Arc::clone(&connection).try_acquire_owned()?,
        _process_permit: Arc::clone(&process).try_acquire_owned()?,
    })
    .await?;
    let (output, mut peer) = tokio::io::duplex(16);
    let writer = tokio::spawn(drain_responses(output, responses, Duration::from_secs(1)));
    // Four bytes are visible to the peer, while the bounded socket write is
    // deliberately still blocked on the rest of the response.
    let mut header = [0_u8; 4];
    timeout(Duration::from_secs(1), peer.read_exact(&mut header)).await??;
    assert!(u32::from_be_bytes(header) > 16);
    assert!(!writer.is_finished());
    let next_process = Arc::clone(&process).try_acquire_owned();
    let next_connection = Arc::clone(&connection).try_acquire_owned();
    let next_admitted = match (next_process, next_connection) {
        (Ok(process_permit), Ok(permit)) => {
            send.send(AdmittedResponse {
                envelope: ResponseEnvelope {
                    request_id: 2,
                    response: Response::Ok,
                },
                _permit: permit,
                _process_permit: process_permit,
            })
            .await?;
            // The one egress response is additional, but a queued response
            // still holds both budgets and prevents a third admission.
            assert!(Arc::clone(&process).try_acquire_owned().is_err());
            assert!(Arc::clone(&connection).try_acquire_owned().is_err());
            true
        }
        _ => false,
    };
    // Always stop the owned driver before reporting an assertion failure.
    drop((send, peer));
    assert!(writer.await?.is_err());
    assert!(
        next_admitted,
        "egress response must leave request admission available"
    );
    assert_eq!(process.available_permits(), 1);
    assert_eq!(connection.available_permits(), 1);
    Ok(())
}

// The peer can observe the last response byte on another runtime thread before
// the writer task resumes and drops its response admission. Replenishing a
// full, healthy Client window must not be mistaken for new overload.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn completed_gets_can_replenish_the_full_default_window() -> Result<()> {
    let rpc = RpcServer::bind("127.0.0.1:0").await?;
    let address = rpc.local_addr()?.to_string();
    let calls = Arc::new(AtomicUsize::new(0));
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(rpc.run_until(
        {
            let calls = Arc::clone(&calls);
            move |request| {
                calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    let Request::Get { key } = request else {
                        panic!("expected GET")
                    };
                    Ok(Response::Value { value: Some(key) })
                }
            }
        },
        async {
            stopped.await?;
            Ok(())
        },
    ));
    let client = Client::connect(&address).await?;
    let mut workers = JoinSet::new();
    for index in 0..INTERNAL_MAX_IN_FLIGHT {
        let client = client.clone();
        workers.spawn(async move {
            let mut key = vec![index as u8; 1024];
            for iteration in 0..256_u64 {
                key[..8].copy_from_slice(&iteration.to_be_bytes());
                let value = client.get(&key).await?;
                assert_eq!(value.as_deref(), Some(key.as_slice()));
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        });
    }
    let result = timeout(Duration::from_secs(10), async {
        while let Some(worker) = workers.join_next().await {
            worker??;
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await?;
    drop((workers, client));
    stop.send(()).unwrap();
    server.await??;
    result?;
    assert_eq!(calls.load(Ordering::SeqCst), INTERNAL_MAX_IN_FLIGHT * 256);
    Ok(())
}
