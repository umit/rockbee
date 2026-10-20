#![allow(clippy::unwrap_used)]

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use rockbee_raft_protocol::{
    KvRequest, KvResponse, RequestEnvelope, RequestId, ResponseEnvelope, VERSION, decode_frame,
    encode_frame,
    transport::server::{RequestHandler, Server, ServerConfig},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Notify, watch},
    time::Instant,
};

struct Echo {
    calls: AtomicUsize,
    release: Arc<Notify>,
    wait: bool,
}

struct SlowOutput {
    calls: Arc<AtomicUsize>,
    completed: Arc<AtomicUsize>,
    output_release: watch::Receiver<bool>,
    held_request_release: watch::Receiver<bool>,
    response: Bytes,
}

impl RequestHandler for SlowOutput {
    type Request = KvRequest;
    type Response = KvResponse;

    fn handle(
        &self,
        request: KvRequest,
        _deadline: Instant,
    ) -> impl Future<Output = KvResponse> + Send {
        let calls = Arc::clone(&self.calls);
        let completed = Arc::clone(&self.completed);
        let mut output_release = self.output_release.clone();
        let mut held_request_release = self.held_request_release.clone();
        let response = self.response.clone();
        async move {
            let hold = match request {
                KvRequest::Get { key } => key,
                KvRequest::Put { .. } | KvRequest::Delete { .. } => {
                    unreachable!("test submits GET only")
                }
            };
            calls.fetch_add(1, Ordering::SeqCst);
            if hold.as_ref() == b"hold" {
                held_request_release
                    .wait_for(|released| *released)
                    .await
                    .expect("test release sender remains alive");
            } else {
                output_release
                    .wait_for(|released| *released)
                    .await
                    .expect("test release sender remains alive");
            }
            completed.fetch_add(1, Ordering::SeqCst);
            KvResponse::Value(Some(response))
        }
    }
}

impl RequestHandler for Echo {
    type Request = KvRequest;
    type Response = KvResponse;

    fn handle(
        &self,
        request: KvRequest,
        _deadline: Instant,
    ) -> impl Future<Output = KvResponse> + Send {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let release = Arc::clone(&self.release);
        let wait = self.wait;
        async move {
            if wait && call == 0 {
                release.notified().await;
            }
            match request {
                KvRequest::Get { key } => KvResponse::Value(Some(key)),
                KvRequest::Put { .. } | KvRequest::Delete { .. } => {
                    unreachable!("test submits GET only")
                }
            }
        }
    }
}

async fn start<H: RequestHandler<Request = KvRequest, Response = KvResponse>>(
    handler: Arc<H>,
    config: ServerConfig,
) -> (Server, std::net::SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    (Server::start(listener, config, handler).unwrap(), address)
}

#[cfg(feature = "diagnostics")]
#[tokio::test]
async fn diagnostics_snapshot_exposes_the_configured_idle_admission_budgets() {
    let config = ServerConfig {
        max_connections: 3,
        max_outstanding: 5,
        request_bytes: 8 * 1024 * 1024,
        response_bytes: 8 * 1024 * 1024,
        ..ServerConfig::default()
    };
    let handler = Arc::new(Echo {
        calls: AtomicUsize::new(0),
        release: Arc::new(Notify::new()),
        wait: false,
    });
    let (mut server, _) = start(handler, config).await;
    assert_eq!(
        server.admission_snapshot(),
        rockbee_raft_protocol::transport::server::ServerAdmissionSnapshot {
            connections_configured: config.max_connections,
            connections_available: config.max_connections,
            operations_configured: config.max_outstanding,
            operations_available: config.max_outstanding,
            request_bytes_configured: config.request_bytes,
            request_bytes_available: config.request_bytes,
            response_bytes_configured: config.response_bytes,
            response_bytes_available: config.response_bytes,
        }
    );
    server.close().await;
}

async fn send(stream: &mut TcpStream, id: u64, key: &'static [u8]) {
    stream.write_all(&request_frame(id, key)).await.unwrap();
}

async fn wait_for_calls(calls: &AtomicUsize, expected: usize) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while calls.load(Ordering::SeqCst) != expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("handler calls reached expected count");
}

async fn call_is_observed_within(calls: &AtomicUsize, expected: usize, wait: Duration) -> bool {
    tokio::time::timeout(wait, async {
        while calls.load(Ordering::SeqCst) != expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok()
}

fn request_frame(id: u64, key: &'static [u8]) -> Vec<u8> {
    encode_frame(
        &RequestEnvelope::new(
            RequestId::new(id).unwrap(),
            5_000,
            KvRequest::Get {
                key: Bytes::from_static(key),
            },
        )
        .unwrap(),
    )
    .unwrap()
}

async fn response(stream: &mut TcpStream) -> ResponseEnvelope {
    let size = stream.read_u32().await.unwrap() as usize;
    let mut frame = vec![0; size + 4];
    frame[..4].copy_from_slice(&(size as u32).to_be_bytes());
    stream.read_exact(&mut frame[4..]).await.unwrap();
    decode_frame(&frame).unwrap()
}

#[tokio::test]
async fn validated_request_preserves_id_and_returns_out_of_order_safe_response() {
    let handler = Arc::new(Echo {
        calls: AtomicUsize::new(0),
        release: Arc::new(Notify::new()),
        wait: false,
    });
    let (mut server, address) = start(handler.clone(), ServerConfig::default()).await;
    let mut client = TcpStream::connect(address).await.unwrap();
    send(&mut client, 7, b"blue").await;
    let got = response(&mut client).await;
    assert_eq!(got.version, VERSION);
    assert_eq!(got.request_id, RequestId::new(7).unwrap());
    assert_eq!(
        got.response,
        KvResponse::Value(Some(Bytes::from_static(b"blue")))
    );
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
    server.close().await;
}

#[tokio::test]
async fn duplicate_active_id_closes_stream_without_executing_duplicate() {
    let release = Arc::new(Notify::new());
    let handler = Arc::new(Echo {
        calls: AtomicUsize::new(0),
        release: Arc::clone(&release),
        wait: true,
    });
    let (mut server, address) = start(handler.clone(), ServerConfig::default()).await;
    let mut client = TcpStream::connect(address).await.unwrap();
    send(&mut client, 1, b"first").await;
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    send(&mut client, 1, b"duplicate").await;
    let mut byte = [0; 1];
    assert!(
        tokio::time::timeout(Duration::from_secs(1), client.read(&mut byte))
            .await
            .unwrap()
            .is_ok()
    );
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
    release.notify_waiters();
    server.close().await;
}

#[tokio::test]
async fn completed_handler_does_not_discard_partial_next_frame() {
    let release = Arc::new(Notify::new());
    let handler = Arc::new(Echo {
        calls: AtomicUsize::new(0),
        release: Arc::clone(&release),
        wait: true,
    });
    let (mut server, address) = start(handler.clone(), ServerConfig::default()).await;
    let mut client = TcpStream::connect(address).await.unwrap();
    send(&mut client, 1, b"first").await;
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    let second = request_frame(2, b"second");
    let split = second.len() - 1;
    client.write_all(&second[..split]).await.unwrap();
    // The reader has an incomplete payload for request two. Completing request
    // one must not drop that receive future and reinterpret the final byte as
    // a fresh frame header.
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    release.notify_waiters();
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    client.write_all(&second[split..]).await.unwrap();
    let first = tokio::time::timeout(Duration::from_secs(1), response(&mut client))
        .await
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(1), response(&mut client))
        .await
        .unwrap();
    assert_eq!(first.request_id, RequestId::new(1).unwrap());
    assert_eq!(
        first.response,
        KvResponse::Value(Some(Bytes::from_static(b"first")))
    );
    assert_eq!(second.request_id, RequestId::new(2).unwrap());
    assert_eq!(
        second.response,
        KvResponse::Value(Some(Bytes::from_static(b"second")))
    );
    assert_eq!(handler.calls.load(Ordering::SeqCst), 2);
    server.close().await;
}

#[tokio::test]
async fn disconnected_accepted_work_keeps_global_slot_until_handler_finishes() {
    let release = Arc::new(Notify::new());
    let handler = Arc::new(Echo {
        calls: AtomicUsize::new(0),
        release: Arc::clone(&release),
        wait: true,
    });
    let config = ServerConfig {
        max_outstanding: 1,
        ..ServerConfig::default()
    };
    let (mut server, address) = start(handler.clone(), config).await;
    let mut first = TcpStream::connect(address).await.unwrap();
    send(&mut first, 1, b"accepted").await;
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    drop(first);
    let mut blocked = TcpStream::connect(address).await.unwrap();
    send(&mut blocked, 2, b"must-not-run-yet").await;
    let mut one = [0; 1];
    let blocked_read = tokio::time::timeout(Duration::from_secs(1), blocked.read(&mut one))
        .await
        .unwrap();
    assert!(
        blocked_read.is_ok(),
        "over-capacity connection is retired conservatively"
    );
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
    release.notify_waiters();
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    let mut second = TcpStream::connect(address).await.unwrap();
    send(&mut second, 3, b"after-release").await;
    let got = response(&mut second).await;
    assert_eq!(
        got.response,
        KvResponse::Value(Some(Bytes::from_static(b"after-release")))
    );
    server.close().await;
}

#[tokio::test]
async fn writer_termination_retires_later_input_but_drains_accepted_work() {
    const ACCEPTED_REQUESTS: usize = 15;
    let (output_ready, output_release) = watch::channel(false);
    let (held_requests_ready, held_request_release) = watch::channel(false);
    let handler = Arc::new(SlowOutput {
        calls: Arc::new(AtomicUsize::new(0)),
        completed: Arc::new(AtomicUsize::new(0)),
        output_release,
        held_request_release,
        response: Bytes::from(vec![9; 1024 * 1024]),
    });
    let config = ServerConfig {
        max_outstanding: 128,
        response_bytes: 600 * 1024 * 1024,
        io_timeout: Duration::from_millis(10),
        ..ServerConfig::default()
    };
    let (mut server, address) = start(Arc::clone(&handler), config).await;
    let mut client = TcpStream::connect(address).await.unwrap();

    for request_id in 1..=ACCEPTED_REQUESTS as u64 {
        send(&mut client, request_id, b"large").await;
    }
    wait_for_calls(&handler.calls, ACCEPTED_REQUESTS).await;
    output_ready.send_replace(true);

    // Do not consume output while the server's writer attempts fifteen MiB.
    // Keep the input half active with handlers that cannot enqueue a response,
    // so the reader's independent timeout cannot hide the writer transition.
    // After the bounded output write expires, the old runtime still leaves its
    // reader accepting a fresh frame.
    let writer_deadline = Instant::now() + Duration::from_millis(100);
    let mut request_id = ACCEPTED_REQUESTS as u64 + 1;
    while Instant::now() < writer_deadline {
        if client
            .write_all(&request_frame(request_id, b"hold"))
            .await
            .is_err()
        {
            break;
        }
        if !call_is_observed_within(
            &handler.calls,
            request_id as usize,
            Duration::from_millis(5),
        )
        .await
        {
            break;
        }
        request_id += 1;
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let (mut read_half, write_half) = client.into_split();
    let (heartbeat_stop, mut heartbeat_stopped) = watch::channel(false);
    let heartbeat_calls = Arc::clone(&handler.calls);
    let heartbeat = tokio::spawn(async move {
        let mut writer = write_half;
        let mut heartbeat_id = request_id;
        loop {
            if *heartbeat_stopped.borrow() {
                break writer;
            }
            let expected_calls = heartbeat_calls.load(Ordering::SeqCst) + 1;
            if writer
                .write_all(&request_frame(heartbeat_id, b"hold"))
                .await
                .is_err()
            {
                break writer;
            }
            let observed = tokio::select! {
                _ = heartbeat_stopped.changed() => false,
                observed = call_is_observed_within(&heartbeat_calls, expected_calls, Duration::from_millis(5)) => observed,
            };
            if !observed {
                break writer;
            }
            heartbeat_id += 1;
            tokio::select! {
                _ = heartbeat_stopped.changed() => break writer,
                _ = tokio::time::sleep(Duration::from_millis(1)) => {}
            }
        }
    });

    // The heartbeat keeps the input half active while this exact connection's
    // buffered output drains. EOF proves its response writer has exited; if
    // draining made the writer survive, this bounded observation fails.
    let mut output = Vec::new();
    let writer_closed = matches!(
        tokio::time::timeout(Duration::from_secs(1), read_half.read_to_end(&mut output)).await,
        Ok(Ok(_))
    );
    heartbeat_stop.send_replace(true);
    let mut write_half = heartbeat.await.expect("heartbeat task does not panic");
    let calls_before_late_frame = handler.calls.load(Ordering::SeqCst);
    let completed_before_late_frame = handler.completed.load(Ordering::SeqCst);
    assert!(
        calls_before_late_frame > completed_before_late_frame,
        "held accepted handlers remain incomplete before their release"
    );
    let late_write = write_half
        .write_all(&request_frame(
            request_id + 10_000,
            b"must-not-reach-handler",
        ))
        .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let calls_after_late_frame = handler.calls.load(Ordering::SeqCst);
    held_requests_ready.send_replace(true);
    server.close().await;
    assert!(
        writer_closed,
        "response writer did not close this connection's output half"
    );
    assert!(
        late_write.is_err() || calls_after_late_frame == calls_before_late_frame,
        "late frame reached handler after the response writer terminated"
    );
    assert_eq!(
        handler.completed.load(Ordering::SeqCst),
        calls_after_late_frame,
        "every accepted handler drains after its hold is released"
    );
}

#[test]
fn server_rejects_response_budget_that_cannot_cover_value_and_frame_overlap() {
    let handler = Arc::new(Echo {
        calls: AtomicUsize::new(0),
        release: Arc::new(Notify::new()),
        wait: false,
    });
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = ServerConfig {
            response_bytes: 2 * 1024 * 1024,
            ..ServerConfig::default()
        };
        assert!(Server::start(listener, config, handler).is_err());
    });
}

#[test]
fn server_rejects_an_unsupported_response_queue_before_spawning_tasks() {
    let handler = Arc::new(Echo {
        calls: AtomicUsize::new(0),
        release: Arc::new(Notify::new()),
        wait: false,
    });
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = ServerConfig {
            connection_response_queue: tokio::sync::Semaphore::MAX_PERMITS + 1,
            ..ServerConfig::default()
        };
        assert!(Server::start(listener, config, handler).is_err());
    });
}
