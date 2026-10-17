use super::*;
use rockbee_client::RockBeeClient;
use rockbee_protocol::transport::{FrameReader, read_frame};
use rockbee_protocol::{RequestEnvelope, encode_frame};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio::time::timeout;

const BOUND: Duration = Duration::from_secs(2);

fn legacy_get(key: &[u8]) -> Vec<u8> {
    encode_frame(&RequestEnvelope {
        request_id: 0,
        request: Request::Get { key: key.to_vec() },
    })
    .expect("test request encodes")
}

async fn legacy_response(
    reader: &mut FrameReader<tokio::net::tcp::OwnedReadHalf>,
) -> Result<Response> {
    let frame = timeout(Duration::from_secs(1), read_frame(reader))
        .await??
        .ok_or("legacy peer closed before expected response")?;
    let envelope = ResponseEnvelope::decode(&frame)?;
    assert_eq!(envelope.request_id, 0);
    Ok(envelope.response)
}

struct Live(Arc<AtomicUsize>);

impl Drop for Live {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

async fn wait_dropped(live: &AtomicUsize) -> Result<()> {
    timeout(Duration::from_millis(300), async {
        while live.load(Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

async fn stop_and_drain(
    stop: tokio::sync::oneshot::Sender<()>,
    owners: &mut JoinSet<Result<()>>,
) -> Result<()> {
    let _ = stop.send(());
    let finished = timeout(Duration::from_secs(1), owners.join_next()).await;
    owners.abort_all();
    while owners.join_next().await.is_some() {}
    finished?.ok_or("RPC owner task missing")???;
    Ok(())
}

#[tokio::test]
async fn legacy_pipelined_requests_remain_sequential_and_ordered() -> Result<()> {
    let config = RpcConfig::new(2, Duration::from_secs(10), Duration::from_secs(1))?
        .with_global_max_in_flight(1)?;
    let rpc = RpcServer::bind_with_config("127.0.0.1:0", config).await?;
    let address = rpc.local_addr()?.to_string();
    let first_entered = Arc::new(Notify::new());
    let second_entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let mut owners = JoinSet::new();
    owners.spawn(rpc.run_until(
        {
            let first_entered = Arc::clone(&first_entered);
            let second_entered = Arc::clone(&second_entered);
            let release = Arc::clone(&release);
            move |request| {
                let first_entered = Arc::clone(&first_entered);
                let second_entered = Arc::clone(&second_entered);
                let release = Arc::clone(&release);
                async move {
                    let Request::Get { key } = request else {
                        return Err("expected GET".into());
                    };
                    if key == b"first\0\xff" {
                        first_entered.notify_one();
                        release.notified().await;
                    } else if key == b"second\xff\0" {
                        second_entered.notify_one();
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

    let result = timeout(BOUND, async {
        let stream = TcpStream::connect(&address).await?;
        let (input, mut output) = stream.into_split();
        output.write_all(&legacy_get(b"first\0\xff")).await?;
        output.write_all(&legacy_get(b"second\xff\0")).await?;
        timeout(Duration::from_secs(1), first_entered.notified()).await?;
        assert!(
            timeout(Duration::from_millis(100), second_entered.notified())
                .await
                .is_err(),
            "second legacy handler ran before the first response was released"
        );

        release.notify_one();
        let mut input = FrameReader::new(input);
        assert_eq!(
            legacy_response(&mut input).await?,
            Response::Value {
                value: Some(b"first\0\xff".to_vec())
            }
        );
        timeout(Duration::from_secs(1), second_entered.notified()).await?;
        assert_eq!(
            legacy_response(&mut input).await?,
            Response::Value {
                value: Some(b"second\xff\0".to_vec())
            }
        );
        drop((input, output));
        Result::<()>::Ok(())
    })
    .await;
    let cleanup = stop_and_drain(stop, &mut owners).await;
    result??;
    cleanup
}

#[tokio::test]
async fn legacy_write_half_close_cancels_pending_work_with_or_without_staged_frame() -> Result<()> {
    for staged in [false, true] {
        let config = RpcConfig::new(2, Duration::from_secs(10), Duration::from_secs(1))?
            .with_global_max_in_flight(1)?;
        let rpc = RpcServer::bind_with_config("127.0.0.1:0", config).await?;
        let address = rpc.local_addr()?.to_string();
        let first_entered = Arc::new(Notify::new());
        let live = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let mut owners = JoinSet::new();
        owners.spawn(rpc.run_until(
            {
                let first_entered = Arc::clone(&first_entered);
                let live = Arc::clone(&live);
                let calls = Arc::clone(&calls);
                move |request| {
                    let first_entered = Arc::clone(&first_entered);
                    let live = Arc::clone(&live);
                    let calls = Arc::clone(&calls);
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        let Request::Get { key } = request else {
                            return Err("expected GET".into());
                        };
                        if key == b"first-hold" {
                            live.fetch_add(1, Ordering::SeqCst);
                            let _live = Live(live);
                            first_entered.notify_one();
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

        let result = timeout(BOUND, async {
            let stream = TcpStream::connect(&address).await?;
            let (input, mut output) = stream.into_split();
            output.write_all(&legacy_get(b"first-hold")).await?;
            timeout(Duration::from_secs(1), first_entered.notified()).await?;
            if staged {
                output.write_all(&legacy_get(b"staged-never-runs")).await?;
            }
            // Keep the first handler pending until the server observes EOF.
            // shutdown() completes locally; it does not acknowledge that the
            // peer has received the FIN. Retain the read half to observe the
            // server closing, then prove staged work was never dispatched.
            output.shutdown().await?;
            let mut input = FrameReader::new(input);
            assert!(
                timeout(Duration::from_secs(1), read_frame(&mut input))
                    .await??
                    .is_none(),
                "staged={staged}: server did not close after terminal EOF"
            );
            wait_dropped(&live).await?;
            assert_eq!(calls.load(Ordering::SeqCst), 1, "staged={staged}");
            let client = RockBeeClient::connect(&address).await?;
            let key = b"replacement-after-legacy-eof\0\xff";
            assert_eq!(
                client.get(key).await?,
                Some(key.to_vec()),
                "staged={staged}"
            );
            assert_eq!(calls.load(Ordering::SeqCst), 2, "staged={staged}");
            drop((input, output));
            Result::<()>::Ok(())
        })
        .await;
        let cleanup = stop_and_drain(stop, &mut owners).await;
        result??;
        cleanup?;
    }
    Ok(())
}

#[tokio::test]
async fn legacy_lookahead_rejects_invalid_or_overfull_next_frames_without_running_them()
-> Result<()> {
    enum Next {
        MalformedComplete(Vec<u8>),
        MixedPositive(Vec<u8>),
        Overfull(Vec<u8>),
    }

    for (name, next) in [
        (
            "malformed complete",
            Next::MalformedComplete(vec![0, 0, 0, 1, 0xff]),
        ),
        (
            "mixed positive ID",
            Next::MixedPositive(encode_frame(&RequestEnvelope {
                request_id: 1,
                request: Request::Get {
                    key: b"mixed-never-runs".to_vec(),
                },
            })?),
        ),
        (
            "third ID0 while staged frame is full",
            Next::Overfull(
                [
                    legacy_get(b"staged-never-runs"),
                    legacy_get(b"third-never-runs"),
                ]
                .concat(),
            ),
        ),
    ] {
        let config = RpcConfig::new(2, Duration::from_secs(10), Duration::from_secs(1))?
            .with_global_max_in_flight(1)?;
        let rpc = RpcServer::bind_with_config("127.0.0.1:0", config).await?;
        let address = rpc.local_addr()?.to_string();
        let first_entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let live = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let mut owners = JoinSet::new();
        owners.spawn(rpc.run_until(
            {
                let first_entered = Arc::clone(&first_entered);
                let release = Arc::clone(&release);
                let live = Arc::clone(&live);
                let calls = Arc::clone(&calls);
                move |request| {
                    let first_entered = Arc::clone(&first_entered);
                    let release = Arc::clone(&release);
                    let live = Arc::clone(&live);
                    let calls = Arc::clone(&calls);
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        let Request::Get { key } = request else {
                            return Err("expected GET".into());
                        };
                        if key == b"first-hold" {
                            live.fetch_add(1, Ordering::SeqCst);
                            let _live = Live(live);
                            first_entered.notify_one();
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

        let result = timeout(BOUND, async {
            let stream = TcpStream::connect(&address).await?;
            let (input, mut output) = stream.into_split();
            output.write_all(&legacy_get(b"first-hold")).await?;
            timeout(Duration::from_secs(1), first_entered.notified()).await?;
            let malformed_complete = matches!(&next, Next::MalformedComplete(_));
            match next {
                Next::MalformedComplete(frame) | Next::MixedPositive(frame) => {
                    output.write_all(&frame).await?;
                    assert!(
                        timeout(Duration::from_millis(100), async {
                            while live.load(Ordering::SeqCst) != 0 {
                                tokio::task::yield_now().await;
                            }
                        })
                        .await
                        .is_err(),
                        "{name}: complete next frame canceled the held first request before release"
                    );
                    release.notify_one();
                    let mut input = FrameReader::new(input);
                    assert_eq!(
                        legacy_response(&mut input).await?,
                        Response::Value {
                            value: Some(b"first-hold".to_vec())
                        },
                        "{name}"
                    );
                    if malformed_complete {
                        assert!(
                            matches!(legacy_response(&mut input).await?, Response::Error { .. }),
                            "{name}"
                        );
                    }
                    assert!(
                        timeout(Duration::from_secs(1), read_frame(&mut input))
                            .await??
                            .is_none(),
                        "{name} did not close after invalid next frame"
                    );
                    drop((input, output));
                }
                Next::Overfull(frame) => {
                    // Keep the first handler held while the server reads both
                    // queued frames. A completed client write alone does not
                    // establish that terminal input is readable by the peer.
                    output.write_all(&frame).await?;
                    let mut input = FrameReader::new(input);
                    assert!(
                        timeout(Duration::from_secs(1), read_frame(&mut input))
                            .await??
                            .is_none(),
                        "{name}: server did not close on a second staged ID0 frame"
                    );
                    wait_dropped(&live).await?;
                    drop((input, output));
                }
            }
            assert_eq!(
                calls.load(Ordering::SeqCst),
                1,
                "{name}: queued request reached handler"
            );
            let client = RockBeeClient::connect(&address).await?;
            let key = b"replacement-after-invalid-legacy\xff\0";
            assert_eq!(client.get(key).await?, Some(key.to_vec()), "{name}");
            assert_eq!(calls.load(Ordering::SeqCst), 2, "{name}");
            Result::<()>::Ok(())
        })
        .await;
        let cleanup = stop_and_drain(stop, &mut owners).await;
        result??;
        cleanup?;
    }
    Ok(())
}
