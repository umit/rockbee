use super::*;
use rockbee_protocol::transport::{FrameReader, read_frame};
use rockbee_protocol::{INTERNAL_MAX_IN_FLIGHT, RequestEnvelope, ResponseEnvelope, encode_frame};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tokio::time::timeout;

fn request(id: u64, key: &[u8]) -> Vec<u8> {
    encode_frame(&RequestEnvelope {
        request_id: id,
        request: Request::Get { key: key.to_vec() },
    })
    .unwrap()
}

async fn response(reader: &mut FrameReader<tokio::net::tcp::OwnedReadHalf>) -> ResponseEnvelope {
    ResponseEnvelope::decode(
        &timeout(Duration::from_secs(1), read_frame(reader))
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
    )
    .unwrap()
}

#[test]
fn global_max_in_flight_defaults_and_validates() -> Result<()> {
    assert_eq!(
        RpcConfig::default().global_max_in_flight(),
        INTERNAL_MAX_IN_FLIGHT
    );
    assert_eq!(
        RpcConfig::default()
            .with_global_max_in_flight(1)?
            .global_max_in_flight(),
        1
    );
    assert!(RpcConfig::default().with_global_max_in_flight(0).is_err());
    assert!(
        RpcConfig::default()
            .with_global_max_in_flight(tokio::sync::Semaphore::MAX_PERMITS + 1)
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn global_admission_is_shared_by_mux_and_legacy_connections() -> Result<()> {
    // Run both directions: a mux request blocks a legacy request, then a
    // legacy request blocks a mux request. The successful third request proves
    // release occurs only after the first request has produced its response.
    for (first_id, blocked_id) in [(1, 0), (0, 1)] {
        let config = RpcConfig::default().with_global_max_in_flight(1)?;
        let rpc = RpcServer::bind_with_config("127.0.0.1:0", config).await?;
        let address = rpc.local_addr()?;
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(rpc.run_until(
            {
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                let calls = Arc::clone(&calls);
                move |request| {
                    let entered = Arc::clone(&entered);
                    let release = Arc::clone(&release);
                    let calls = Arc::clone(&calls);
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        let Request::Get { key } = request else {
                            panic!("expected GET")
                        };
                        if key == b"hold" {
                            entered.notify_one();
                            release.notified().await;
                        }
                        Ok(Response::Value { value: Some(key) })
                    }
                }
            },
            async {
                stopped.await?;
                Ok(())
            },
        ));

        let first = TcpStream::connect(address).await?;
        let (first_input, mut first_output) = first.into_split();
        first_output.write_all(&request(first_id, b"hold")).await?;
        timeout(Duration::from_secs(1), entered.notified()).await?;

        let blocked = TcpStream::connect(address).await?;
        let (blocked_input, mut blocked_output) = blocked.into_split();
        blocked_output
            .write_all(&request(blocked_id, b"blocked"))
            .await?;
        let mut blocked_input = FrameReader::new(blocked_input);
        assert!(
            timeout(Duration::from_millis(300), read_frame(&mut blocked_input))
                .await??
                .is_none(),
            "global overload must retire the second connection before its handler"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        release.notify_one();
        let mut first_input = FrameReader::new(first_input);
        assert_eq!(response(&mut first_input).await.request_id, first_id);

        let third = TcpStream::connect(address).await?;
        let (third_input, mut third_output) = third.into_split();
        third_output
            .write_all(&request(blocked_id, b"after-release"))
            .await?;
        let mut third_input = FrameReader::new(third_input);
        assert_eq!(response(&mut third_input).await.request_id, blocked_id);
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        drop((
            first_input,
            first_output,
            blocked_input,
            blocked_output,
            third_input,
            third_output,
        ));
        stop.send(()).unwrap();
        server.await??;
    }
    Ok(())
}

struct Live(Arc<AtomicUsize>);

impl Drop for Live {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn global_admission_recovers_after_mux_eof() -> Result<()> {
    let config = RpcConfig::default().with_global_max_in_flight(1)?;
    let rpc = RpcServer::bind_with_config("127.0.0.1:0", config).await?;
    let address = rpc.local_addr()?;
    let entered = Arc::new(Notify::new());
    let live = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(rpc.run_until(
        {
            let entered = Arc::clone(&entered);
            let live = Arc::clone(&live);
            let calls = Arc::clone(&calls);
            move |request| {
                let entered = Arc::clone(&entered);
                let live = Arc::clone(&live);
                let calls = Arc::clone(&calls);
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    live.fetch_add(1, Ordering::SeqCst);
                    let _live = Live(live);
                    let Request::Get { key } = request else {
                        panic!("expected GET")
                    };
                    if key == b"hold" {
                        entered.notify_one();
                        std::future::pending::<()>().await;
                    }
                    Ok(Response::Value { value: Some(key) })
                }
            }
        },
        async {
            stopped.await?;
            Ok(())
        },
    ));

    let mux = TcpStream::connect(address).await?;
    let (mux_input, mut mux_output) = mux.into_split();
    mux_output.write_all(&request(1, b"hold")).await?;
    timeout(Duration::from_secs(1), entered.notified()).await?;
    drop((mux_input, mux_output));
    timeout(Duration::from_secs(1), async {
        while live.load(Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;

    let legacy = TcpStream::connect(address).await?;
    let (legacy_input, mut legacy_output) = legacy.into_split();
    legacy_output
        .write_all(&request(0, b"after-mux-eof"))
        .await?;
    let mut legacy_input = FrameReader::new(legacy_input);
    assert_eq!(response(&mut legacy_input).await.request_id, 0);
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    drop((legacy_input, legacy_output));
    stop.send(()).unwrap();
    server.await??;
    Ok(())
}

#[tokio::test]
async fn malformed_peer_releases_capacity_without_canceling_another_client() -> Result<()> {
    use rockbee_client::Client;
    use rockbee_protocol::MAX_FRAME;

    let mut truncated = request(2, b"must-not-run");
    truncated.pop();
    for (case, frame, half_close) in [
        ("invalid protobuf", vec![0, 0, 0, 1, 0xff], false),
        (
            "oversize length",
            (MAX_FRAME as u32).to_be_bytes().to_vec(),
            false,
        ),
        ("truncated body with EOF", truncated, true),
    ] {
        // Two occupied sockets and process permits. The replacement GET must
        // use the failed peer's capacity while the healthy GET is still held.
        let config = RpcConfig::new(2, Duration::from_secs(10), Duration::from_secs(1))?
            .with_global_max_in_flight(2)?;
        let rpc = RpcServer::bind_with_config("127.0.0.1:0", config).await?;
        let address = rpc.local_addr()?.to_string();
        let bad_entered = Arc::new(Notify::new());
        let healthy_entered = Arc::new(Notify::new());
        let release_healthy = Arc::new(Notify::new());
        let bad_live = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let mut server = JoinSet::new();
        server.spawn(rpc.run_until(
            {
                let bad_entered = Arc::clone(&bad_entered);
                let healthy_entered = Arc::clone(&healthy_entered);
                let release_healthy = Arc::clone(&release_healthy);
                let bad_live = Arc::clone(&bad_live);
                let calls = Arc::clone(&calls);
                move |request| {
                    let bad_entered = Arc::clone(&bad_entered);
                    let healthy_entered = Arc::clone(&healthy_entered);
                    let release_healthy = Arc::clone(&release_healthy);
                    let bad_live = Arc::clone(&bad_live);
                    let calls = Arc::clone(&calls);
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        let Request::Get { key } = request else {
                            return Err("expected GET".into());
                        };
                        if key == b"bad-hold" {
                            bad_live.fetch_add(1, Ordering::SeqCst);
                            let _live = Live(bad_live);
                            bad_entered.notify_one();
                            std::future::pending::<()>().await;
                        } else if key == b"healthy-\0\xff" {
                            healthy_entered.notify_one();
                            release_healthy.notified().await;
                        }
                        Ok(Response::Value { value: Some(key) })
                    }
                }
            },
            async {
                stopped.await?;
                Ok(())
            },
        ));
        let mut pending = JoinSet::new();
        let result = timeout(Duration::from_secs(4), async {
            let bad = TcpStream::connect(&address).await?;
            let (input, mut output) = bad.into_split();
            output.write_all(&request(1, b"bad-hold")).await?;
            bad_entered.notified().await;

            let healthy = Client::connect(&address).await?;
            pending.spawn({
                let healthy = healthy.clone();
                async move { healthy.get(b"healthy-\0\xff").await }
            });
            healthy_entered.notified().await;
            assert_eq!(calls.load(Ordering::SeqCst), 2, "{case}");

            output.write_all(&frame).await?;
            if half_close {
                output.shutdown().await?;
            }
            let closed = read_frame(&mut FrameReader::new(input)).await;
            assert!(
                matches!(&closed, Ok(None))
                    || matches!(&closed, Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset),
                "{case}: failed peer did not close: {closed:?}"
            );
            while bad_live.load(Ordering::SeqCst) != 0 {
                tokio::task::yield_now().await;
            }

            // EOF and handler Drop may precede the connection owner's final
            // socket-permit release. Explicit retries here are GET-only and
            // bounded; the production Client does not replay requests.
            let replacement = timeout(Duration::from_secs(1), async {
                let mut last_error = None;
                for _ in 0..32 {
                    let client = Client::connect(&address).await?;
                    match client.get(b"replacement-\xff\0").await {
                        Ok(value) => {
                            assert_eq!(value, Some(b"replacement-\xff\0".to_vec()), "{case}");
                            return Result::Ok(client);
                        }
                        Err(error) => last_error = Some(error),
                    }
                    tokio::task::yield_now().await;
                }
                Err(last_error.ok_or("replacement GET was not attempted")?)
            })
            .await??;
            assert_eq!(calls.load(Ordering::SeqCst), 3, "{case}");
            release_healthy.notify_one();
            assert_eq!(
                pending.join_next().await.ok_or("healthy task missing")???,
                Some(b"healthy-\0\xff".to_vec()),
                "{case}: another connection canceled the healthy GET"
            );
            assert_eq!(healthy.get(b"after").await?, Some(b"after".to_vec()), "{case}");
            assert_eq!(calls.load(Ordering::SeqCst), 4, "{case}: bad frame reached handler");
            drop(replacement);
            Result::Ok(())
        })
        .await;
        pending.abort_all();
        while pending.join_next().await.is_some() {}
        let _ = stop.send(());
        let finished = timeout(Duration::from_secs(1), server.join_next()).await;
        server.abort_all();
        while server.join_next().await.is_some() {}
        result??;
        finished?.ok_or("server task missing")???;
    }
    Ok(())
}

#[tokio::test]
async fn legacy_eof_cancels_held_handler_and_releases_global_admission() -> Result<()> {
    use rockbee_client::Client;
    use tokio::task::JoinSet;

    let config = RpcConfig::new(2, Duration::from_secs(10), Duration::from_secs(1))?
        .with_global_max_in_flight(1)?;
    let rpc = RpcServer::bind_with_config("127.0.0.1:0", config).await?;
    let address = rpc.local_addr()?.to_string();
    let entered = Arc::new(Notify::new());
    let live = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let mut owners = JoinSet::new();
    owners.spawn(rpc.run_until(
        {
            let entered = Arc::clone(&entered);
            let live = Arc::clone(&live);
            let calls = Arc::clone(&calls);
            move |request| {
                let entered = Arc::clone(&entered);
                let live = Arc::clone(&live);
                let calls = Arc::clone(&calls);
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let Request::Get { key } = request else {
                        return Err("expected GET".into());
                    };
                    if key == b"legacy-hold" {
                        live.fetch_add(1, Ordering::SeqCst);
                        let _live = Live(live);
                        entered.notify_one();
                        std::future::pending::<()>().await;
                    }
                    Ok(Response::Value { value: Some(key) })
                }
            }
        },
        async {
            stopped.await?;
            Ok(())
        },
    ));

    let result = timeout(Duration::from_secs(2), async {
        let legacy = TcpStream::connect(&address).await?;
        let (legacy_input, mut legacy_output) = legacy.into_split();
        legacy_output.write_all(&request(0, b"legacy-hold")).await?;
        timeout(Duration::from_secs(1), entered.notified()).await?;
        drop((legacy_input, legacy_output));

        timeout(Duration::from_millis(300), async {
            while live.load(Ordering::SeqCst) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;

        let client = Client::connect(&address).await?;
        let key = b"after-legacy-eof\0\xff";
        assert_eq!(client.get(key).await?, Some(key.to_vec()));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        Result::Ok(())
    })
    .await;

    let _ = stop.send(());
    let finished = timeout(Duration::from_secs(1), owners.join_next()).await;
    owners.abort_all();
    while owners.join_next().await.is_some() {}
    result??;
    finished?.ok_or("RPC owner task missing")???;
    Ok(())
}
