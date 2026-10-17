use rockbee_protocol::ServiceRole;
use rockbee_protocol::transport::Link;
use rockbee_protocol::{MutationOutcome, WireMessage};
use rockbee_test_support::protocol::{
    Request, RequestEnvelope, Response, ResponseEnvelope, encode_frame,
};
use rockbee_test_support::storage::Role;
use rockbee_test_support::transport::{FrameReader, read_frame, write_frame};
use rockbee_test_support::*;
use slatedb::object_store::memory::InMemory;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::oneshot;
use tokio::time::{Instant, timeout};

#[tokio::test]
async fn failed_bind_reports_storage_close_error_too() -> Result<()> {
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let store = Store::open(Role::Writer, "dual-bind-error", Arc::new(InMemory::new())).await?;
    store.close().await?;
    let error = Server::bind(&occupied.local_addr()?.to_string(), store)
        .await
        .err()
        .expect("address is occupied");

    let diagnostic = error.to_string();
    assert!(diagnostic.contains("Closed error"), "{diagnostic}");
    let bind_error = error
        .source()
        .unwrap()
        .downcast_ref::<std::io::Error>()
        .unwrap();
    assert_eq!(bind_error.kind(), std::io::ErrorKind::AddrInUse);
    assert!(diagnostic.contains(&bind_error.to_string()), "{diagnostic}");
    Ok(())
}

#[tokio::test]
async fn failed_bind_keeps_original_error_when_storage_closes() -> Result<()> {
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let store = Store::open(Role::Writer, "bind-error", Arc::new(InMemory::new())).await?;
    let error = Server::bind(&occupied.local_addr()?.to_string(), store)
        .await
        .err()
        .expect("address is occupied");

    assert_eq!(
        error.downcast_ref::<std::io::Error>().unwrap().kind(),
        std::io::ErrorKind::AddrInUse
    );
    Ok(())
}

#[tokio::test]
async fn shutdown_reports_both_rpc_and_storage_close_errors() -> Result<()> {
    let store = Store::open(
        Role::Writer,
        "dual-shutdown-error",
        Arc::new(InMemory::new()),
    )
    .await?;
    let server = Server::bind("127.0.0.1:0", store).await?;
    // A second close is a real SlateDB error, without leaving background work running.
    server.storage().close().await?;
    let error = server
        .run_until(async { Err(std::io::Error::other("supervisor-stop-failure").into()) })
        .await
        .expect_err("both shutdown stages failed");

    let diagnostic = error.to_string();
    assert!(
        diagnostic.contains("supervisor-stop-failure"),
        "{diagnostic}"
    );
    assert!(diagnostic.contains("Closed error"), "{diagnostic}");
    assert_eq!(
        error
            .source()
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap()
            .to_string(),
        "supervisor-stop-failure"
    );
    Ok(())
}

#[tokio::test]
async fn rpc_shutdown_error_still_closes_storage_and_keeps_original_error() -> Result<()> {
    let store = Store::open(
        Role::Writer,
        "rpc-shutdown-error",
        Arc::new(InMemory::new()),
    )
    .await?;
    let server = Server::bind("127.0.0.1:0", store).await?;
    let store = Arc::clone(server.storage());
    let error = server
        .run_until(async {
            Err(
                std::io::Error::new(std::io::ErrorKind::Interrupted, "supervisor-stop-failure")
                    .into(),
            )
        })
        .await
        .expect_err("shutdown future failed");

    assert_eq!(
        error.downcast_ref::<std::io::Error>().unwrap().kind(),
        std::io::ErrorKind::Interrupted
    );
    let closed = store.get(b"key").await.unwrap_err();
    assert!(matches!(
        closed.downcast_ref::<slatedb::Error>().unwrap().kind(),
        slatedb::ErrorKind::Closed(_)
    ));
    Ok(())
}

#[tokio::test]
async fn storage_close_error_after_clean_rpc_shutdown_keeps_original_error() -> Result<()> {
    let store = Store::open(
        Role::Writer,
        "storage-shutdown-error",
        Arc::new(InMemory::new()),
    )
    .await?;
    let server = Server::bind("127.0.0.1:0", store).await?;
    server.storage().close().await?;
    let error = server
        .run_until(async { Ok(()) })
        .await
        .expect_err("storage was already closed");

    assert!(error.is::<slatedb::Error>());
    assert!(error.to_string().contains("Closed error"));
    Ok(())
}

async fn start_server(
    store: Store,
) -> Result<(
    String,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<()>>,
)> {
    let server = Server::bind("127.0.0.1:0", store).await?;
    let address = server.local_addr()?.to_string();
    let (stop, stopped) = oneshot::channel();
    let task = tokio::spawn(server.run_until(async move {
        let _ = stopped.await;
        Ok(())
    }));
    Ok((address, stop, task))
}

async fn request(address: &str, request: Request) -> Result<Response> {
    let stream = tokio::net::TcpStream::connect(address).await?;
    let (input, mut output) = stream.into_split();
    write_frame(
        &mut output,
        &RequestEnvelope {
            request_id: 0,
            request,
        },
    )
    .await?;
    let mut input = FrameReader::new(input);
    let frame = read_frame(&mut input)
        .await?
        .ok_or("server closed before response")?;
    Ok(ResponseEnvelope::decode(&frame)?.response)
}

async fn malformed(address: &str, body: &[u8]) -> Result<Response> {
    let stream = tokio::net::TcpStream::connect(address).await?;
    let (input, mut output) = stream.into_split();
    output.write_u32(body.len().try_into()?).await?;
    output.write_all(body).await?;
    output.flush().await?;
    let mut input = FrameReader::new(input);
    let frame = read_frame(&mut input)
        .await?
        .ok_or("server closed before response")?;
    Ok(ResponseEnvelope::decode(&frame)?.response)
}

#[tokio::test]
async fn status_reports_the_fixed_writer_and_reader_roles_without_storage_requests() -> Result<()> {
    for (storage_role, status_role, prefix) in [
        (Role::Writer, ServiceRole::Writer, "status-writer-role"),
        (Role::Reader, ServiceRole::Reader, "status-reader-role"),
    ] {
        let objects: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        if storage_role == Role::Reader {
            let writer = Store::open(Role::Writer, prefix, Arc::clone(&objects)).await?;
            writer.close().await?;
        }
        let store = Store::open(storage_role, prefix, objects).await?;
        let server = Server::bind_with_rpc("127.0.0.1:0", store, RpcConfig::default()).await?;
        let address = server.local_addr()?.to_string();
        let (stop, stopped) = oneshot::channel();
        let owner = tokio::spawn(server.run_until(async move {
            stopped.await?;
            Ok(())
        }));

        let link = Link::new(address, 1, 0);
        let response = timeout(
            std::time::Duration::from_secs(1),
            link.exchange(
                &Request::Status,
                Instant::now() + std::time::Duration::from_secs(1),
            ),
        )
        .await??;
        let Response::Status { snapshot } = response else {
            return Err("storage Status returned a CRUD response".into());
        };
        assert_eq!(snapshot.role, status_role);

        drop(link);
        stop.send(()).expect("server is running");
        timeout(std::time::Duration::from_secs(1), owner).await???;
    }
    Ok(())
}

#[tokio::test]
async fn binary_requests_reach_the_writer_over_tcp() -> Result<()> {
    let store = Store::open(Role::Writer, "routing", Arc::new(InMemory::new())).await?;
    let (address, stop, task) = start_server(store).await?;
    assert_eq!(
        request(
            &address,
            Request::Put {
                key: vec![0, 0xff],
                value: vec![0x80, 0xff]
            }
        )
        .await?,
        Response::Ok
    );
    assert_eq!(
        request(&address, Request::Get { key: vec![0, 0xff] }).await?,
        Response::Value {
            value: Some(vec![0x80, 0xff])
        }
    );
    assert_eq!(
        request(&address, Request::Delete { key: vec![0, 0xff] }).await?,
        Response::Ok
    );
    assert_eq!(
        request(&address, Request::Get { key: vec![0, 0xff] }).await?,
        Response::Value { value: None }
    );
    stop.send(()).expect("server is running");
    task.await??;
    Ok(())
}

#[tokio::test]
async fn malformed_or_wrong_direction_frames_do_not_write_over_tcp() -> Result<()> {
    let store = Store::open(Role::Writer, "invalid", Arc::new(InMemory::new())).await?;
    let (address, stop, task) = start_server(store).await?;
    let wrong_direction = encode_frame(&Response::Ok)?[4..].to_vec();
    for body in [vec![0xff], wrong_direction] {
        assert!(matches!(
            malformed(&address, &body).await?,
            Response::Error { .. }
        ));
    }
    assert_eq!(
        request(
            &address,
            Request::Get {
                key: b"name".to_vec()
            }
        )
        .await?,
        Response::Value { value: None }
    );
    stop.send(()).expect("server is running");
    task.await??;
    Ok(())
}

#[tokio::test]
async fn reader_endpoint_rejects_put_and_delete_over_tcp() -> Result<()> {
    let objects = Arc::new(InMemory::new());
    let writer = Store::open(Role::Writer, "read-only-routing", objects.clone()).await?;
    writer.put(b"name", b"original").await?;
    let reader = Store::open(Role::Reader, "read-only-routing", objects).await?;
    let (address, stop, task) = start_server(reader).await?;
    for request_value in [
        Request::Put {
            key: b"name".to_vec(),
            value: b"changed".to_vec(),
        },
        Request::Delete {
            key: b"name".to_vec(),
        },
    ] {
        assert!(matches!(
            request(&address, request_value).await?,
            Response::Error { .. }
        ));
    }
    assert_eq!(
        writer.get(b"name").await?.as_deref(),
        Some(&b"original"[..])
    );
    stop.send(()).expect("server is running");
    task.await??;
    writer.close().await
}

/// Deliberately bypass outbound validation to exercise the server boundary.
struct UncheckedRequest(RequestEnvelope);

impl WireMessage for UncheckedRequest {
    fn validate(&self) -> Result<()> {
        Ok(())
    }

    fn to_wire(&self) -> protocol::wire::Envelope {
        self.0.to_wire()
    }
}

#[tokio::test]
async fn raw_empty_inputs_are_rejected_by_both_nodes_without_changing_stored_data() -> Result<()> {
    use std::time::Duration;

    for role in [Role::Writer, Role::Reader] {
        let objects = Arc::new(InMemory::new());
        let writer = Store::open(Role::Writer, "raw-empty-input", objects.clone()).await?;
        writer.put(b"\0", b"original").await?;
        let store = match role {
            Role::Writer => writer,
            Role::Reader => {
                writer.close().await?;
                Store::open(Role::Reader, "raw-empty-input", objects).await?
            }
        };
        let (address, stop, mut task) = start_server(store).await?;
        let body: Result<()> = async {
            // Separate connections exercise legacy (ID 0) and multiplexed RPC.
            for first_id in [0, 1] {
                let stream = tokio::net::TcpStream::connect(&address).await?;
                let (input, mut output) = stream.into_split();
                let mut input = FrameReader::new(input);
                let invalid = [
                    Request::Put {
                        key: vec![],
                        value: b"invalid".to_vec(),
                    },
                    Request::Put {
                        key: vec![0],
                        value: vec![],
                    },
                    Request::PutWithTtl {
                        key: vec![],
                        value: b"invalid".to_vec(),
                        ttl_millis: 1000,
                    },
                    Request::PutWithTtl {
                        key: vec![0],
                        value: vec![],
                        ttl_millis: 1000,
                    },
                    Request::Get { key: vec![] },
                    Request::Delete { key: vec![] },
                ];
                for (index, request) in invalid.into_iter().enumerate() {
                    let request_id = if first_id == 0 {
                        0
                    } else {
                        u64::try_from(index)? + 1
                    };
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
                        .ok_or("node closed after an invalid request")?;
                    let reply = ResponseEnvelope::decode(&frame)?;
                    assert_eq!(reply.request_id, request_id);
                    let Response::Error {
                        mutation_outcome, ..
                    } = reply.response
                    else {
                        return Err(format!("{role:?} accepted empty input").into());
                    };
                    if mutation {
                        assert_eq!(mutation_outcome, MutationOutcome::Rejected);
                    }
                }
                let request_id = if first_id == 0 { 0 } else { 7 };
                write_frame(
                    &mut output,
                    &RequestEnvelope {
                        request_id,
                        request: Request::Get { key: vec![0] },
                    },
                )
                .await?;
                let frame = timeout(Duration::from_secs(2), read_frame(&mut input))
                    .await??
                    .ok_or("node closed before valid GET")?;
                let reply = ResponseEnvelope::decode(&frame)?;
                assert_eq!(reply.request_id, request_id);
                assert_eq!(
                    reply.response,
                    Response::Value {
                        value: Some(b"original".to_vec())
                    }
                );
            }
            Ok(())
        }
        .await;
        let _ = stop.send(());
        let cleanup = match timeout(Duration::from_secs(5), &mut task).await {
            Ok(joined) => joined?,
            Err(error) => {
                task.abort();
                let _ = task.await;
                return Err(error.into());
            }
        };
        body?;
        cleanup?;
    }
    Ok(())
}
