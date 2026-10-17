use super::*;
use rockbee_protocol::transport::FrameReader;
use rockbee_protocol::{INTERNAL_MAX_IN_FLIGHT, RequestEnvelope, ResponseEnvelope, encode_frame};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Notify;
use tokio::time::timeout;

fn request(id: u64, request: Request) -> Vec<u8> {
    encode_frame(&RequestEnvelope {
        request_id: id,
        request,
    })
    .unwrap()
}

async fn response(reader: &mut FrameReader<tokio::net::tcp::OwnedReadHalf>) -> ResponseEnvelope {
    ResponseEnvelope::decode(&read_frame(reader).await.unwrap().unwrap()).unwrap()
}

#[tokio::test]
async fn positive_ids_overlap_and_fragmented_packets_keep_reader_state() -> Result<()> {
    let rpc = RpcServer::bind("127.0.0.1:0").await?;
    let address = rpc.local_addr()?;
    let release = Arc::new(Notify::new());
    let started = Arc::new(Notify::new());
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(rpc.run_until(
        {
            let release = release.clone();
            let started = started.clone();
            move |request| {
                let release = release.clone();
                let started = started.clone();
                async move {
                    let Request::Get { key } = request else {
                        panic!("expected GET")
                    };
                    if key == b"slow" {
                        started.notify_one();
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
    let stream = TcpStream::connect(address).await?;
    let (input, mut output) = stream.into_split();
    let mut input = FrameReader::new(input);
    output
        .write_all(&request(
            1,
            Request::Get {
                key: b"slow".to_vec(),
            },
        ))
        .await?;
    output
        .write_all(&request(
            2,
            Request::Get {
                key: b"fast".to_vec(),
            },
        ))
        .await?;
    let fast = timeout(Duration::from_millis(500), response(&mut input)).await?;
    assert_eq!(fast.request_id, 2);
    assert_eq!(
        fast.response,
        Response::Value {
            value: Some(b"fast".to_vec())
        }
    );
    started.notified().await;
    let third = request(
        3,
        Request::Get {
            key: b"fragmented".to_vec(),
        },
    );
    output.write_all(&third[..5]).await?;
    release.notify_one();
    assert_eq!(response(&mut input).await.request_id, 1);
    output.write_all(&third[5..]).await?;
    assert_eq!(response(&mut input).await.request_id, 3);
    stop.send(()).unwrap();
    server.await??;
    Ok(())
}

#[tokio::test]
async fn duplicate_or_decreasing_positive_ids_retire_without_second_handler() -> Result<()> {
    let rpc = RpcServer::bind("127.0.0.1:0").await?;
    let address = rpc.local_addr()?;
    let calls = Arc::new(AtomicUsize::new(0));
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(rpc.run_until(
        {
            let calls = calls.clone();
            move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(Response::Ok) }
            }
        },
        async {
            stopped.await?;
            Ok(())
        },
    ));
    let stream = TcpStream::connect(address).await?;
    let (input, mut output) = stream.into_split();
    let mut input = FrameReader::new(input);
    output
        .write_all(&request(2, Request::Delete { key: b"k".to_vec() }))
        .await?;
    assert_eq!(response(&mut input).await.request_id, 2);
    output
        .write_all(&request(2, Request::Delete { key: b"k".to_vec() }))
        .await?;
    assert!(
        timeout(Duration::from_millis(500), read_frame(&mut input))
            .await??
            .is_none()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    stop.send(()).unwrap();
    server.await??;
    Ok(())
}

#[tokio::test]
async fn malformed_and_wrong_direction_frames_close_without_execution() -> Result<()> {
    for frame in [vec![0, 0, 0, 1, 0xff], encode_frame(&Response::Ok)?] {
        let rpc = RpcServer::bind("127.0.0.1:0").await?;
        let address = rpc.local_addr()?;
        let calls = Arc::new(AtomicUsize::new(0));
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(rpc.run_until(
            {
                let calls = calls.clone();
                move |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    async { Ok(Response::Ok) }
                }
            },
            async {
                stopped.await?;
                Ok(())
            },
        ));
        let mut stream = TcpStream::connect(address).await?;
        stream.write_all(&frame).await?;
        let mut input = FrameReader::new(stream);
        let reply = read_frame(&mut input).await?;
        assert!(reply.is_some(), "a valid recoverable error is permitted");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        stop.send(()).unwrap();
        server.await??;
    }
    Ok(())
}

#[tokio::test]
async fn sequential_id_zero_remains_ordered_and_cannot_mix_with_positive_mode() -> Result<()> {
    let rpc = RpcServer::bind("127.0.0.1:0").await?;
    let address = rpc.local_addr()?;
    let calls = Arc::new(AtomicUsize::new(0));
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(rpc.run_until(
        {
            let calls = calls.clone();
            move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(Response::Ok) }
            }
        },
        async {
            stopped.await?;
            Ok(())
        },
    ));
    let stream = TcpStream::connect(address).await?;
    let (input, mut output) = stream.into_split();
    let mut input = FrameReader::new(input);
    output
        .write_all(&request(
            0,
            Request::Delete {
                key: b"first".to_vec(),
            },
        ))
        .await?;
    assert_eq!(response(&mut input).await.request_id, 0);
    output
        .write_all(&request(
            1,
            Request::Delete {
                key: b"second".to_vec(),
            },
        ))
        .await?;
    assert!(read_frame(&mut input).await?.is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    stop.send(()).unwrap();
    server.await??;
    Ok(())
}

struct Live(Arc<AtomicUsize>);
impl Drop for Live {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn admission_limit_retires_overflowing_connection() -> Result<()> {
    let rpc = RpcServer::bind("127.0.0.1:0").await?;
    let address = rpc.local_addr()?;
    let (entered_tx, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(rpc.run_until(
        {
            let release = release.clone();
            move |_| {
                let release = release.clone();
                let entered_tx = entered_tx.clone();
                async move {
                    entered_tx.send(()).unwrap();
                    release.notified().await;
                    Ok(Response::Ok)
                }
            }
        },
        async {
            stopped.await?;
            Ok(())
        },
    ));
    let stream = TcpStream::connect(address).await?;
    let (input, mut output) = stream.into_split();
    let mut input = FrameReader::new(input);
    for id in 1..=INTERNAL_MAX_IN_FLIGHT as u64 {
        output
            .write_all(&request(id, Request::Delete { key: b"k".to_vec() }))
            .await?;
    }
    for _ in 0..INTERNAL_MAX_IN_FLIGHT {
        timeout(Duration::from_secs(1), entered_rx.recv())
            .await?
            .ok_or("handler channel closed")?;
    }
    output
        .write_all(&request(
            (INTERNAL_MAX_IN_FLIGHT + 1) as u64,
            Request::Delete {
                key: b"overflow".to_vec(),
            },
        ))
        .await?;
    assert!(
        timeout(Duration::from_millis(300), read_frame(&mut input))
            .await??
            .is_none()
    );
    release.notify_waiters();
    stop.send(()).unwrap();
    server.await??;
    Ok(())
}

#[tokio::test]
async fn eof_at_admission_limit_cancels_handlers() -> Result<()> {
    let rpc = RpcServer::bind("127.0.0.1:0").await?;
    let address = rpc.local_addr()?;
    let live = Arc::new(AtomicUsize::new(0));
    let (entered_tx, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(rpc.run_until(
        {
            let live = live.clone();
            move |_| {
                let live = live.clone();
                let entered_tx = entered_tx.clone();
                async move {
                    live.fetch_add(1, Ordering::SeqCst);
                    let _live = Live(live);
                    entered_tx.send(()).unwrap();
                    std::future::pending::<()>().await;
                    Ok(Response::Ok)
                }
            }
        },
        async {
            stopped.await?;
            Ok(())
        },
    ));
    let mut stream = TcpStream::connect(address).await?;
    for id in 1..=INTERNAL_MAX_IN_FLIGHT as u64 {
        stream
            .write_all(&request(id, Request::Delete { key: b"k".to_vec() }))
            .await?;
    }
    for _ in 0..INTERNAL_MAX_IN_FLIGHT {
        timeout(Duration::from_secs(1), entered_rx.recv())
            .await?
            .ok_or("handler channel closed")?;
    }
    drop(stream);
    timeout(Duration::from_secs(1), async {
        while live.load(Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    stop.send(()).unwrap();
    server.await??;
    Ok(())
}

#[tokio::test]
async fn shutdown_drains_handler_destructor() -> Result<()> {
    struct Cleanup(Arc<AtomicUsize>);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            self.0.store(1, Ordering::SeqCst);
        }
    }
    let rpc = RpcServer::bind("127.0.0.1:0").await?;
    let address = rpc.local_addr()?;
    let finished = Arc::new(AtomicUsize::new(0));
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let entered_tx = Arc::new(std::sync::Mutex::new(Some(entered_tx)));
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(rpc.run_until(
        {
            let finished = finished.clone();
            let entered_tx = entered_tx.clone();
            move |_| {
                let finished = finished.clone();
                let entered_tx = entered_tx.clone();
                async move {
                    let _cleanup = Cleanup(finished);
                    if let Some(tx) = entered_tx.lock().unwrap().take() {
                        let _ = tx.send(());
                    }
                    std::future::pending::<()>().await;
                    Ok(Response::Ok)
                }
            }
        },
        async {
            stopped.await?;
            Ok(())
        },
    ));
    let mut stream = TcpStream::connect(address).await?;
    stream
        .write_all(&request(1, Request::Delete { key: b"k".to_vec() }))
        .await?;
    entered_rx.await?;
    stop.send(()).unwrap();
    server.await??;
    assert_eq!(finished.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn slow_response_writer_duplicate_and_overload_retire_bounded_work() -> Result<()> {
    for overload in [false, true] {
        let rpc = RpcServer::bind("127.0.0.1:0").await?;
        let address = rpc.local_addr()?;
        let calls = Arc::new(AtomicUsize::new(0));
        let mutations = Arc::new(AtomicUsize::new(0));
        let live = Arc::new(AtomicUsize::new(0));
        let (entered_tx, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
        let gate = Arc::new(Notify::new());
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(rpc.run_until(
            {
                let calls = calls.clone();
                let mutations = mutations.clone();
                let live = live.clone();
                let gate = gate.clone();
                move |request| {
                    let calls = calls.clone();
                    let mutations = mutations.clone();
                    let live = live.clone();
                    let entered_tx = entered_tx.clone();
                    let gate = gate.clone();
                    async move {
                        live.fetch_add(1, Ordering::SeqCst);
                        let _live = Live(live);
                        calls.fetch_add(1, Ordering::SeqCst);
                        entered_tx.send(()).unwrap();
                        if overload {
                            gate.notified().await;
                        }
                        match request {
                            Request::Get { .. } => Ok(Response::Value {
                                value: Some(vec![b'x'; 900_000]),
                            }),
                            Request::Put { .. } | Request::PutWithTtl { .. } => {
                                mutations.fetch_add(1, Ordering::SeqCst);
                                Ok(Response::Ok)
                            }
                            Request::Delete { .. } => panic!("unexpected delete"),
                            Request::Table(_) => panic!("unexpected table request"),
                            Request::Status => panic!("Status is handled before the handler"),
                        }
                    }
                }
            },
            async {
                stopped.await?;
                Ok(())
            },
        ));
        let stream = TcpStream::connect(address).await?;
        let (mut input, mut output) = stream.into_split();
        let get_count = if overload { INTERNAL_MAX_IN_FLIGHT } else { 64 };
        for id in 1..=get_count as u64 {
            output
                .write_all(&request(id, Request::Get { key: b"k".to_vec() }))
                .await?;
        }
        if !overload {
            output
                .write_all(&request(
                    65,
                    Request::Put {
                        key: b"k".to_vec(),
                        value: b"once".to_vec(),
                    },
                ))
                .await?;
        }
        let expected = if overload { get_count } else { get_count + 1 };
        for _ in 0..expected {
            timeout(Duration::from_secs(2), entered_rx.recv())
                .await?
                .ok_or("handler channel closed")?;
        }
        if overload {
            output
                .write_all(&request(
                    129,
                    Request::Put {
                        key: b"k".to_vec(),
                        value: b"overflow".to_vec(),
                    },
                ))
                .await?;
            gate.notify_waiters();
        } else {
            assert_eq!(mutations.load(Ordering::SeqCst), 1);
            output
                .write_all(&request(
                    65,
                    Request::Put {
                        key: b"k".to_vec(),
                        value: b"duplicate".to_vec(),
                    },
                ))
                .await?;
        }
        let mut bytes = Vec::new();
        let closed = timeout(Duration::from_secs(3), input.read_to_end(&mut bytes)).await?;
        if let Err(error) = closed {
            assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);
        }
        assert_eq!(calls.load(Ordering::SeqCst), expected);
        assert_eq!(mutations.load(Ordering::SeqCst), usize::from(!overload));
        assert!(
            bytes.len() < 64 * 900_000,
            "backpressure must bound response bytes"
        );
        timeout(Duration::from_secs(1), async {
            while live.load(Ordering::SeqCst) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        stop.send(()).unwrap();
        server.await??;
    }
    Ok(())
}
