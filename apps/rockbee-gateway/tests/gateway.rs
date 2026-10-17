#![cfg_attr(test, allow(clippy::unwrap_used))]

use rockbee_gateway::{Gateway, config::Config, router::Router};
use rockbee_protocol::ServiceRole;
use rockbee_protocol::transport::Link;
use rockbee_test_support::Result;
use rockbee_test_support::protocol::{
    MAX_FRAME, MAX_KEY_LEN, Request, RequestEnvelope, Response, ResponseEnvelope, WireMessage,
    validate_request_bytes,
};
use rockbee_test_support::transport::{FrameReader, read_frame, write_frame};
use std::io::ErrorKind;
use std::net::TcpListener as StdTcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Instant, timeout};

#[derive(Clone)]
enum BackendReply {
    Response(Response),
    DelayedResponse(Duration, Response),
    Close,
    Stall,
    Raw(Vec<u8>),
}

async fn backend(
    response: BackendReply,
) -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let task = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (stream, _) = accepted.unwrap();
                    let seen = seen.clone();
                    let response = response.clone();
                    connections.spawn(async move {
                        let (input, mut output) = stream.into_split();
                        let mut input = FrameReader::new(input);
                        while let Ok(Some(frame)) = read_frame(&mut input).await {
                            let request = RequestEnvelope::decode(&frame).unwrap();
                            assert!(request.request_id > 0);
                            seen.fetch_add(1, Ordering::SeqCst);
                            if matches!(response, BackendReply::Stall) {
                                std::future::pending::<()>().await;
                            }
                            if let BackendReply::DelayedResponse(delay, _) = &response {
                                tokio::time::sleep(*delay).await;
                            }
                            match &response {
                                BackendReply::Close => return,
                                BackendReply::Stall => unreachable!(),
                                BackendReply::Response(response)
                                | BackendReply::DelayedResponse(_, response) => {
                                    if write_frame(&mut output, &ResponseEnvelope {
                                        request_id: request.request_id, response: response.clone(),
                                    }).await.is_err() { return; }
                                }
                                BackendReply::Raw(bytes) => {
                                    if output.write_all(bytes).await.is_err() { return; }
                                }
                            }
                        }
                    });
                }
                _ = connections.join_next(), if !connections.is_empty() => {}
            }
        }
    });
    (address, calls, task)
}

#[tokio::test]
async fn gateway_status_is_local_and_never_contacts_any_backend() -> Result<()> {
    let (writer, writer_calls, writer_task) = backend(BackendReply::Stall).await;
    let (reader, reader_calls, reader_task) = backend(BackendReply::Stall).await;
    let gateway = Gateway::bind("127.0.0.1:0", writer, vec![reader]).await?;
    let address = gateway.local_addr()?.to_string();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(gateway.run_until(async move {
        stopped.await?;
        Ok(())
    }));

    let link = Link::new(address, 1, 0);
    let response = timeout(
        Duration::from_secs(1),
        link.exchange(&Request::Status, Instant::now() + Duration::from_secs(1)),
    )
    .await??;
    let Response::Status { snapshot } = response else {
        return Err("gateway Status returned a CRUD response".into());
    };
    assert_eq!(snapshot.role, ServiceRole::Gateway);
    assert_eq!(writer_calls.load(Ordering::SeqCst), 0);
    assert_eq!(reader_calls.load(Ordering::SeqCst), 0);

    drop(link);
    stop.send(()).expect("gateway is running");
    timeout(Duration::from_secs(1), server).await???;
    writer_task.abort();
    reader_task.abort();
    Ok(())
}

async fn concurrent_delayed_backend(
    delay: Duration,
    response: Response,
) -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let task = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (stream, _) = accepted.unwrap();
                    let seen = seen.clone();
                    let response = response.clone();
                    connections.spawn(async move {
                        let (input, output) = stream.into_split();
                        let output = Arc::new(tokio::sync::Mutex::new(output));
                        let mut input = FrameReader::new(input);
                        let mut replies = tokio::task::JoinSet::new();
                        while let Ok(Some(frame)) = read_frame(&mut input).await {
                            let request = RequestEnvelope::decode(&frame).unwrap();
                            assert!(request.request_id > 0);
                            seen.fetch_add(1, Ordering::SeqCst);
                            let output = Arc::clone(&output);
                            let response = response.clone();
                            replies.spawn(async move {
                                tokio::time::sleep(delay).await;
                                let mut output = output.lock().await;
                                let _ = write_frame(&mut *output, &ResponseEnvelope {
                                    request_id: request.request_id, response,
                                }).await;
                            });
                        }
                        while replies.join_next().await.is_some() {}
                    });
                }
                _ = connections.join_next(), if !connections.is_empty() => {}
            }
        }
    });
    (address, calls, task)
}

#[test]
fn gateway_config_requires_explicit_topology() -> Result<()> {
    let config = Config::parse(
        [
            "--writer",
            "127.0.0.1:7000",
            "--reader",
            "127.0.0.1:7001",
            "--reader",
            "127.0.0.1:7002",
        ]
        .map(String::from),
    )?
    .unwrap();
    assert_eq!(config.address, "127.0.0.1:7100");
    assert_eq!(config.readers.len(), 2);
    for args in [
        vec![],
        vec!["--writer", "x:1"],
        vec!["--reader", "x:2"],
        vec!["--writer", "x:1", "--reader", "x:1"],
        vec!["--writer", "x:1", "--reader", "x:2", "--reader", "x:2"],
        vec!["--writer", "--reader", "x:2"],
    ] {
        assert!(Config::parse(args.into_iter().map(String::from)).is_err());
    }
    assert!(Config::parse(["--help".to_string()])?.is_none());
    Ok(())
}

#[test]
fn gateway_rejects_removed_cache_options() {
    let base = ["--writer", "127.0.0.1:7000", "--reader", "127.0.0.1:7001"];
    for args in [
        vec!["--cache-entries", "1"],
        vec!["--cache-bytes", "1"],
        vec!["--cache-ttl-ms", "1"],
        vec![
            "--cache-entries",
            "2",
            "--cache-bytes",
            "32",
            "--cache-ttl-ms",
            "50",
        ],
    ] {
        let mut full = base.to_vec();
        full.extend(args);
        assert!(Config::parse(full.into_iter().map(String::from)).is_err());
    }
}

#[tokio::test]
async fn repeated_gets_reach_reader_before_and_after_a_mutation() -> Result<()> {
    let (writer, writes, writer_task) = backend(BackendReply::Response(Response::Ok)).await;
    let (reader, reads, reader_task) = backend(BackendReply::Response(Response::Value {
        value: Some(b"catalogue".to_vec()),
    }))
    .await;
    let router = Router::new(writer, vec![reader])?;

    for _ in 0..2 {
        assert_eq!(
            router.route(Request::Get { key: "sku".into() }).await?,
            Response::Value {
                value: Some("catalogue".into())
            }
        );
    }
    assert_eq!(reads.load(Ordering::SeqCst), 2);

    assert_eq!(
        router
            .route(Request::Put {
                key: "sku".into(),
                value: "updated".into()
            })
            .await?,
        Response::Ok
    );
    assert_eq!(writes.load(Ordering::SeqCst), 1);
    assert_eq!(
        router.route(Request::Get { key: "sku".into() }).await?,
        Response::Value {
            value: Some("catalogue".into())
        }
    );
    assert_eq!(reads.load(Ordering::SeqCst), 3);

    writer_task.abort();
    reader_task.abort();
    Ok(())
}

#[tokio::test]
async fn reads_continue_after_a_failed_writer_call() -> Result<()> {
    let (writer, writes, writer_task) = backend(BackendReply::Close).await;
    let (reader, reads, reader_task) = backend(BackendReply::Response(Response::Value {
        value: Some(b"catalogue".to_vec()),
    }))
    .await;
    let router = Router::new(writer, vec![reader])?;

    assert!(matches!(
        router.route(Request::Get { key: "sku".into() }).await?,
        Response::Value { .. }
    ));
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    assert!(
        router
            .route(Request::Delete { key: "sku".into() })
            .await
            .is_err()
    );
    assert_eq!(writes.load(Ordering::SeqCst), 1);
    assert!(matches!(
        router.route(Request::Get { key: "sku".into() }).await?,
        Response::Value { .. }
    ));
    assert_eq!(reads.load(Ordering::SeqCst), 2);

    writer_task.abort();
    reader_task.abort();
    Ok(())
}

#[tokio::test]
async fn reads_continue_during_and_after_an_aborted_writer_request() -> Result<()> {
    let (writer, writes, writer_task) = backend(BackendReply::Stall).await;
    let (reader, reads, reader_task) = backend(BackendReply::Response(Response::Value {
        value: Some(b"catalogue".to_vec()),
    }))
    .await;
    let router = Arc::new(Router::new(writer, vec![reader])?);

    assert!(matches!(
        router.route(Request::Get { key: "sku".into() }).await?,
        Response::Value { .. }
    ));
    assert_eq!(reads.load(Ordering::SeqCst), 1);

    let mutation_router = Arc::clone(&router);
    let mutation = tokio::spawn(async move {
        mutation_router
            .route(Request::Put {
                key: "sku".into(),
                value: "updated".into(),
            })
            .await
    });
    for _ in 0..100 {
        if writes.load(Ordering::SeqCst) == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(writes.load(Ordering::SeqCst), 1);

    assert!(matches!(
        router.route(Request::Get { key: "sku".into() }).await?,
        Response::Value { .. }
    ));
    assert_eq!(reads.load(Ordering::SeqCst), 2);
    mutation.abort();
    assert!(mutation.await.unwrap_err().is_cancelled());

    assert!(matches!(
        router.route(Request::Get { key: "sku".into() }).await?,
        Response::Value { .. }
    ));
    assert_eq!(reads.load(Ordering::SeqCst), 3);

    writer_task.abort();
    reader_task.abort();
    Ok(())
}

#[tokio::test]
async fn mutations_only_reach_writer_and_reads_rotate() -> Result<()> {
    let (writer, writes, w) = backend(BackendReply::Response(Response::Ok)).await;
    let (a, ca, ta) = backend(BackendReply::Response(Response::Value {
        value: Some(b"a".to_vec()),
    }))
    .await;
    let (b, cb, tb) = backend(BackendReply::Response(Response::Value {
        value: Some(b"b".to_vec()),
    }))
    .await;
    let router = Arc::new(Router::new(writer, vec![a, b])?);
    assert_eq!(
        router
            .route(Request::Put {
                key: "k".into(),
                value: "v".into()
            })
            .await?,
        Response::Ok
    );
    assert_eq!(
        router.route(Request::Delete { key: "k".into() }).await?,
        Response::Ok
    );
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..20 {
        let router = router.clone();
        tasks.spawn(async move { router.route(Request::Get { key: "k".into() }).await });
    }
    while let Some(result) = tasks.join_next().await {
        assert!(matches!(result??, Response::Value { value: Some(_) }));
    }
    assert_eq!(writes.load(Ordering::SeqCst), 2);
    assert_eq!(ca.load(Ordering::SeqCst), 10);
    assert_eq!(cb.load(Ordering::SeqCst), 10);
    for task in [w, ta, tb] {
        task.abort();
    }
    Ok(())
}

#[tokio::test]
async fn reads_fail_over_but_writes_are_never_retried() -> Result<()> {
    let (writer, writes, w) = backend(BackendReply::Close).await;
    let (bad, bad_calls, b) = backend(BackendReply::Close).await;
    let (good, good_calls, g) =
        backend(BackendReply::Response(Response::Value { value: None })).await;
    let router = Router::new(writer, vec![bad, good])?;
    assert_eq!(
        router
            .route(Request::Get {
                key: "missing".into()
            })
            .await?,
        Response::Value { value: None }
    );
    let error = router
        .route(Request::Put {
            key: "k".into(),
            value: "v".into(),
        })
        .await
        .expect_err("closed writer must leave a sent mutation uncertain");
    assert!(error.to_string().contains("mutation outcome is unknown"));
    assert_eq!(writes.load(Ordering::SeqCst), 1);
    assert_eq!(bad_calls.load(Ordering::SeqCst), 1);
    assert_eq!(good_calls.load(Ordering::SeqCst), 1);
    for task in [w, b, g] {
        task.abort();
    }
    Ok(())
}

#[tokio::test]
async fn gateway_rejects_invalid_requests_before_contacting_any_backend() -> Result<()> {
    let writer = StdTcpListener::bind("127.0.0.1:0")?;
    let reader = StdTcpListener::bind("127.0.0.1:0")?;
    writer.set_nonblocking(true)?;
    reader.set_nonblocking(true)?;
    let router = Router::new(
        writer.local_addr()?.to_string(),
        vec![reader.local_addr()?.to_string()],
    )?;

    let oversized_key = vec![b'k'; MAX_KEY_LEN + 1];
    let combined_key = vec![b'k'];
    let combined_value = vec![b'v'; MAX_FRAME - 27];
    let combined = Request::Put {
        key: combined_key.clone(),
        value: combined_value.clone(),
    };
    assert!(
        combined.validate().is_ok(),
        "fixture must fit a zero-ID request"
    );

    let invalid = vec![
        Request::Put {
            key: b"k".to_vec(),
            value: vec![b'v'; MAX_FRAME - 25],
        },
        Request::Put {
            key: oversized_key.clone(),
            value: b"v".to_vec(),
        },
        Request::Put {
            key: vec![],
            value: b"v".to_vec(),
        },
        Request::Put {
            key: b"k".to_vec(),
            value: vec![],
        },
        Request::PutWithTtl {
            key: vec![],
            value: b"v".to_vec(),
            ttl_millis: 1000,
        },
        Request::PutWithTtl {
            key: b"k".to_vec(),
            value: vec![],
            ttl_millis: 1000,
        },
        Request::Get { key: vec![] },
        Request::Delete { key: vec![] },
        Request::Get {
            key: oversized_key.clone(),
        },
        Request::Delete { key: oversized_key },
        combined,
    ];

    for request in invalid {
        let mutation = !matches!(&request, Request::Get { .. });
        let expected = match &request {
            Request::Put { key, value } => validate_request_bytes(key, Some(value)),
            Request::PutWithTtl {
                key,
                value,
                ttl_millis,
            } => {
                rockbee_test_support::protocol::validate_put_with_ttl_bytes(key, value, *ttl_millis)
            }
            Request::Get { key } | Request::Delete { key } => validate_request_bytes(key, None),
            Request::Status => unreachable!("Status is handled before gateway routing"),
            Request::Table(_) => unreachable!("table validation is tested separately"),
        }
        .err()
        .ok_or("invalid fixture unexpectedly passed protocol validation")?;
        let result = timeout(Duration::from_secs(1), router.route(request))
            .await
            .map_err(|_| "invalid request route timed out")?;
        let error = match result {
            Ok(response) => {
                return Err(format!("invalid request unexpectedly returned {response:?}").into());
            }
            Err(error) => error,
        };
        if mutation {
            let typed = error
                .downcast_ref::<rockbee_protocol::MutationError>()
                .unwrap();
            assert_eq!(typed.outcome(), rockbee_protocol::MutationOutcome::Rejected);
            assert_eq!(
                std::error::Error::source(typed).unwrap().to_string(),
                expected.to_string()
            );
        } else {
            assert_eq!(error.to_string(), expected.to_string());
        }
    }

    for listener in [&writer, &reader] {
        match listener.accept() {
            Err(error) if error.kind() == ErrorKind::WouldBlock => {}
            Ok((_stream, address)) => {
                return Err(format!("invalid request contacted backend at {address}").into());
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

#[tokio::test]
async fn invalid_backend_responses_are_errors_not_missing_values() -> Result<()> {
    for response in [
        BackendReply::Response(Response::Ok),
        BackendReply::Raw(vec![0]),
        BackendReply::Raw(vec![8, 1]),
        BackendReply::Raw(vec![0, 0, 0, 1, 0]),
    ] {
        let (reader, _, task) = backend(response).await;
        let router = Router::new("127.0.0.1:1".into(), vec![reader])?;
        assert!(
            router
                .route(Request::Get { key: "k".into() })
                .await
                .is_err()
        );
        task.abort();
    }
    let (reader, calls, task) = backend(BackendReply::Response(Response::Error {
        mutation_outcome: rockbee_protocol::MutationOutcome::Unknown,
        message: "reader unavailable".into(),
    }))
    .await;
    let router = Router::new("127.0.0.1:1".into(), vec![reader, "127.0.0.1:2".into()])?;
    assert_eq!(
        router.route(Request::Get { key: "k".into() }).await?,
        Response::Error {
            mutation_outcome: rockbee_protocol::MutationOutcome::Unknown,
            message: "reader unavailable".into()
        }
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    task.abort();
    Ok(())
}

#[tokio::test]
async fn reader_application_error_falls_back_to_exact_value_on_each_get() -> Result<()> {
    let (first, first_calls, first_task) = backend(BackendReply::Response(Response::Error {
        mutation_outcome: rockbee_protocol::MutationOutcome::Unknown,
        message: "first reader rejected get".into(),
    }))
    .await;
    let value = vec![0, 0xff, 0x42];
    let (second, second_calls, second_task) = backend(BackendReply::Response(Response::Value {
        value: Some(value.clone()),
    }))
    .await;
    let router = Router::new("127.0.0.1:1".into(), vec![first, second])?;

    for _ in 0..2 {
        assert_eq!(
            router
                .route(Request::Get {
                    key: b"catalogue".to_vec()
                })
                .await?,
            Response::Value {
                value: Some(value.clone())
            }
        );
    }

    assert_eq!(first_calls.load(Ordering::SeqCst), 1);
    assert_eq!(second_calls.load(Ordering::SeqCst), 2);
    first_task.abort();
    second_task.abort();
    Ok(())
}

#[tokio::test]
async fn repeated_missing_gets_reach_reader_without_querying_later_reader() -> Result<()> {
    let (first, first_calls, first_task) = backend(BackendReply::Response(Response::Error {
        mutation_outcome: rockbee_protocol::MutationOutcome::Unknown,
        message: "first reader rejected get".into(),
    }))
    .await;
    let (second, second_calls, second_task) =
        backend(BackendReply::Response(Response::Value { value: None })).await;
    let (third, third_calls, third_task) = backend(BackendReply::Response(Response::Value {
        value: Some(b"must not be queried".to_vec()),
    }))
    .await;
    let router = Router::new("127.0.0.1:1".into(), vec![first, second, third])?;

    for _ in 0..2 {
        assert_eq!(
            router
                .route(Request::Get {
                    key: b"missing".to_vec()
                })
                .await?,
            Response::Value { value: None }
        );
    }
    assert_eq!(first_calls.load(Ordering::SeqCst), 1);
    assert_eq!(second_calls.load(Ordering::SeqCst), 2);
    assert_eq!(third_calls.load(Ordering::SeqCst), 0);
    first_task.abort();
    second_task.abort();
    third_task.abort();
    Ok(())
}

#[tokio::test]
async fn all_reader_application_errors_keep_first_route_order_error_and_are_not_cached()
-> Result<()> {
    let mut readers = Vec::new();
    let mut calls = Vec::new();
    let mut tasks = Vec::new();
    for message in ["reader-a", "reader-b", "reader-c"] {
        let (address, seen, task) = backend(BackendReply::Response(Response::Error {
            mutation_outcome: rockbee_protocol::MutationOutcome::Unknown,
            message: message.into(),
        }))
        .await;
        readers.push(address);
        calls.push(seen);
        tasks.push(task);
    }
    let router = Router::new("127.0.0.1:1".into(), readers)?;

    assert_eq!(
        router.route(Request::Get { key: b"k".to_vec() }).await?,
        Response::Error {
            mutation_outcome: rockbee_protocol::MutationOutcome::Unknown,
            message: "reader-a".into()
        }
    );
    for calls in &calls {
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
    assert_eq!(
        router.route(Request::Get { key: b"k".to_vec() }).await?,
        Response::Error {
            mutation_outcome: rockbee_protocol::MutationOutcome::Unknown,
            message: "reader-b".into()
        }
    );
    for calls in &calls {
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
    for task in tasks {
        task.abort();
    }
    Ok(())
}

#[tokio::test]
async fn reader_application_error_uses_remaining_budget_for_delayed_final_value() -> Result<()> {
    let (first, first_calls, first_task) = backend(BackendReply::Response(Response::Error {
        mutation_outcome: rockbee_protocol::MutationOutcome::Unknown,
        message: "first reader rejected get".into(),
    }))
    .await;
    let value = b"final reader value after application error".to_vec();
    let (final_reader, final_calls, final_task) = backend(BackendReply::DelayedResponse(
        Duration::from_millis(1200),
        Response::Value {
            value: Some(value.clone()),
        },
    ))
    .await;
    let router = Router::new("127.0.0.1:1".into(), vec![first, final_reader])?;

    let response = timeout(
        Duration::from_secs(4),
        router.route(Request::Get {
            key: b"delayed".to_vec(),
        }),
    )
    .await;
    first_task.abort();
    final_task.abort();
    assert_eq!(response??, Response::Value { value: Some(value) });
    assert_eq!(first_calls.load(Ordering::SeqCst), 1);
    assert_eq!(final_calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn mutation_application_errors_only_reach_writer_once() -> Result<()> {
    let (writer, writer_calls, writer_task) = backend(BackendReply::Response(Response::Error {
        mutation_outcome: rockbee_protocol::MutationOutcome::Unknown,
        message: "writer rejected mutation".into(),
    }))
    .await;
    let (reader, reader_calls, reader_task) = backend(BackendReply::Response(Response::Value {
        value: Some(b"reader must not receive mutation".to_vec()),
    }))
    .await;
    let router = Router::new(writer, vec![reader])?;

    for request in [
        Request::Put {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        },
        Request::Delete { key: b"k".to_vec() },
    ] {
        assert_eq!(
            router.route(request).await?,
            Response::Error {
                mutation_outcome: rockbee_protocol::MutationOutcome::Unknown,
                message: "writer rejected mutation".into()
            }
        );
    }
    assert_eq!(writer_calls.load(Ordering::SeqCst), 2);
    assert_eq!(reader_calls.load(Ordering::SeqCst), 0);
    writer_task.abort();
    reader_task.abort();
    Ok(())
}

/// Bypass outbound validation so these frames exercise gateway ingress.
struct UncheckedRequest(RequestEnvelope);

impl WireMessage for UncheckedRequest {
    fn validate(&self) -> Result<()> {
        Ok(())
    }

    fn to_wire(&self) -> rockbee_protocol::wire::Envelope {
        self.0.to_wire()
    }
}

#[tokio::test]
async fn gateway_rejects_raw_empty_inputs_without_forwarding_and_keeps_connection_usable()
-> Result<()> {
    let (writer, writer_calls, writer_task) = backend(BackendReply::Response(Response::Ok)).await;
    let (reader, calls, task) = backend(BackendReply::Response(Response::Value {
        value: Some(b"product".to_vec()),
    }))
    .await;
    let gateway = Gateway::bind("127.0.0.1:0", writer, vec![reader]).await?;
    let address = gateway.local_addr()?;
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let mut server = tokio::spawn(gateway.run_until(async {
        stopped.await?;
        Ok(())
    }));
    let stream = TcpStream::connect(address).await?;
    let (input, mut output) = stream.into_split();
    let mut input = FrameReader::new(input);
    let invalid = [
        Request::Put {
            key: vec![],
            value: b"v".to_vec(),
        },
        Request::Put {
            key: b"k".to_vec(),
            value: vec![],
        },
        Request::PutWithTtl {
            key: vec![],
            value: b"v".to_vec(),
            ttl_millis: 1000,
        },
        Request::PutWithTtl {
            key: b"k".to_vec(),
            value: vec![],
            ttl_millis: 1000,
        },
        Request::Get { key: vec![] },
        Request::Delete { key: vec![] },
    ];
    for (index, request) in invalid.into_iter().enumerate() {
        let request_id = u64::try_from(index)? + 1;
        let mutation = request.is_mutation();
        write_frame(
            &mut output,
            &UncheckedRequest(RequestEnvelope {
                request_id,
                request,
            }),
        )
        .await?;
        let frame = timeout(Duration::from_secs(2), read_frame(&mut input))
            .await??
            .ok_or("gateway closed after an invalid request")?;
        let reply = ResponseEnvelope::decode(&frame)?;
        assert_eq!(reply.request_id, request_id);
        let Response::Error {
            mutation_outcome, ..
        } = reply.response
        else {
            return Err("gateway accepted empty input".into());
        };
        if mutation {
            assert_eq!(
                mutation_outcome,
                rockbee_protocol::MutationOutcome::Rejected
            );
        }
    }
    assert_eq!(writer_calls.load(Ordering::SeqCst), 0);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    for request_id in 7..9 {
        write_frame(
            &mut output,
            &RequestEnvelope {
                request_id,
                request: Request::Get { key: b"k".to_vec() },
            },
        )
        .await?;
        let frame = timeout(Duration::from_secs(2), read_frame(&mut input))
            .await??
            .ok_or("gateway closed before recovery GET")?;
        let reply = ResponseEnvelope::decode(&frame)?;
        assert_eq!(reply.request_id, request_id);
        assert_eq!(
            reply.response,
            Response::Value {
                value: Some(b"product".to_vec())
            }
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    stop.send(()).unwrap();
    match timeout(Duration::from_secs(5), &mut server).await {
        Ok(joined) => joined??,
        Err(error) => {
            server.abort();
            let _ = server.await;
            task.abort();
            writer_task.abort();
            return Err(error.into());
        }
    }
    assert!(TcpStream::connect(address).await.is_err());
    task.abort();
    writer_task.abort();
    Ok(())
}

#[tokio::test]
async fn stalled_reader_does_not_consume_the_entire_get_budget() -> Result<()> {
    let (slow, _, slow_task) = backend(BackendReply::Stall).await;
    let (good, _, good_task) = backend(BackendReply::Response(Response::Value {
        value: Some(b"ok".to_vec()),
    }))
    .await;
    let router = Router::new("127.0.0.1:1".into(), vec![slow, good])?;
    let response = timeout(
        Duration::from_secs(3),
        router.route(Request::Get { key: "k".into() }),
    )
    .await??;
    assert_eq!(
        response,
        Response::Value {
            value: Some("ok".into())
        }
    );
    slow_task.abort();
    good_task.abort();
    Ok(())
}

#[tokio::test]
async fn final_reader_uses_remaining_get_budget_for_a_delayed_exact_reply() -> Result<()> {
    let (stalled, stalled_seen, stalled_task) = backend(BackendReply::Stall).await;
    let value = b"final-reader-exact-value".to_vec();
    let (final_reader, final_seen, final_task) = backend(BackendReply::DelayedResponse(
        Duration::from_millis(1200),
        Response::Value {
            value: Some(value.clone()),
        },
    ))
    .await;
    let router = Router::new("127.0.0.1:1".to_owned(), vec![stalled, final_reader])?;

    let response = timeout(
        Duration::from_secs(4),
        router.route(Request::Get {
            key: b"final-reader-delayed-key".to_vec(),
        }),
    )
    .await;

    stalled_task.abort();
    final_task.abort();
    let response = response??;

    assert_eq!(response, Response::Value { value: Some(value) });
    assert_eq!(stalled_seen.load(Ordering::SeqCst), 1);
    assert_eq!(final_seen.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn two_stalled_readers_share_the_single_total_get_budget() -> Result<()> {
    let (first, first_seen, first_task) = backend(BackendReply::Stall).await;
    let (second, second_seen, second_task) = backend(BackendReply::Stall).await;
    let router = Router::new("127.0.0.1:1".to_owned(), vec![first, second])?;

    let response = timeout(
        Duration::from_millis(5500),
        router.route(Request::Get {
            key: b"two-stalled-readers".to_vec(),
        }),
    )
    .await;

    first_task.abort();
    second_task.abort();
    let response = response?;
    assert!(response.is_err());
    assert_eq!(first_seen.load(Ordering::SeqCst), 1);
    assert_eq!(second_seen.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn concurrent_gets_do_not_lose_final_reader_work_to_an_initial_attempt_timeout() -> Result<()>
{
    let value = b"replicated-delayed-value".to_vec();
    let (reader_a, reader_a_seen, reader_a_task) = concurrent_delayed_backend(
        Duration::from_millis(1200),
        Response::Value {
            value: Some(value.clone()),
        },
    )
    .await;
    let (reader_b, reader_b_seen, reader_b_task) = concurrent_delayed_backend(
        Duration::from_millis(1200),
        Response::Value {
            value: Some(value.clone()),
        },
    )
    .await;
    let router = Arc::new(Router::new(
        "127.0.0.1:1".to_owned(),
        vec![reader_a, reader_b],
    )?);

    let first_router = Arc::clone(&router);
    let mut first = tokio::spawn(async move {
        first_router
            .route(Request::Get {
                key: b"first-overlap".to_vec(),
            })
            .await
    });
    let first_registered = timeout(Duration::from_millis(500), async {
        while reader_a_seen.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    if let Err(error) = first_registered {
        first.abort();
        reader_a_task.abort();
        reader_b_task.abort();
        return Err(error.into());
    }
    tokio::time::sleep(Duration::from_millis(250)).await;

    let second_router = Arc::clone(&router);
    let mut second = tokio::spawn(async move {
        second_router
            .route(Request::Get {
                key: b"second-overlap".to_vec(),
            })
            .await
    });
    let result = timeout(Duration::from_secs(4), async {
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>((
            (&mut first).await??,
            (&mut second).await??,
        ))
    })
    .await;

    if !first.is_finished() {
        first.abort();
    }
    if !second.is_finished() {
        second.abort();
    }
    reader_a_task.abort();
    reader_b_task.abort();
    let (first, second) = result??;

    assert_eq!(
        first,
        Response::Value {
            value: Some(value.clone())
        }
    );
    assert_eq!(second, Response::Value { value: Some(value) });
    assert_eq!(reader_a_seen.load(Ordering::SeqCst), 2);
    assert_eq!(reader_b_seen.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn single_reader_get_uses_total_budget_for_a_delayed_exact_reply() -> Result<()> {
    let value = b"delayed-exact-value".to_vec();
    let (reader, seen, task) = backend(BackendReply::DelayedResponse(
        Duration::from_millis(1200),
        Response::Value {
            value: Some(value.clone()),
        },
    ))
    .await;
    let router = Router::new("127.0.0.1:1".to_owned(), vec![reader])?;

    let response = timeout(
        Duration::from_secs(3),
        router.route(Request::Get {
            key: b"delayed-key".to_vec(),
        }),
    )
    .await??;

    assert_eq!(response, Response::Value { value: Some(value) });
    assert_eq!(seen.load(Ordering::SeqCst), 1);
    task.abort();
    Ok(())
}

#[tokio::test]
async fn many_stalled_readers_share_one_total_timeout() -> Result<()> {
    let mut readers = Vec::new();
    let mut tasks = Vec::new();
    for _ in 0..6 {
        let (reader, _, task) = backend(BackendReply::Stall).await;
        readers.push(reader);
        tasks.push(task);
    }
    let router = Router::new("127.0.0.1:1".into(), readers)?;
    let response = timeout(
        Duration::from_secs(6),
        router.route(Request::Get { key: "k".into() }),
    )
    .await?;
    assert!(response.is_err());
    for task in tasks {
        task.abort();
    }
    Ok(())
}

#[test]
fn upstream_limits_reject_invalid_duplicate_and_overflow_values() -> Result<()> {
    let base = ["--writer", "127.0.0.1:7000", "--reader", "127.0.0.1:7001"];
    for flag in ["--upstream-max-in-flight", "--upstream-max-pending"] {
        let valid: Vec<String> = base
            .into_iter()
            .chain([flag, "2"])
            .map(String::from)
            .collect();
        assert!(Config::parse(valid)?.is_some());
        for value in [
            "0",
            "-1",
            "bad",
            "18446744073709551616",
            "18446744073709551615",
        ] {
            let args = base.into_iter().chain([flag, value]).map(String::from);
            assert!(Config::parse(args).is_err());
        }
        let args = base
            .into_iter()
            .chain([flag, "2", flag, "3"])
            .map(String::from);
        assert!(Config::parse(args).is_err());
    }
    Ok(())
}

// A setting above the fixed server admission ceiling would retire otherwise healthy links.
#[test]
fn unsupported_active_limit_is_rejected_before_gateway_startup() -> Result<()> {
    let base = [
        "--writer",
        "127.0.0.1:7000",
        "--reader",
        "127.0.0.1:7001",
        "--upstream-max-in-flight",
    ];
    assert!(Config::parse(base.into_iter().chain(["129"]).map(String::from)).is_err());
    for limit in ["1", "128"] {
        assert!(Config::parse(base.into_iter().chain([limit]).map(String::from))?.is_some());
    }
    Ok(())
}
