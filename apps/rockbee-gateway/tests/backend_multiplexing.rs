//! TCP peers inject transport faults; assertions exercise Router, never a mock router.
#![cfg_attr(test, allow(clippy::unwrap_used))]

use rockbee_gateway::router::Router;
use rockbee_node::Result;
use rockbee_node::protocol::{
    MAX_FRAME, Request, RequestEnvelope, Response, ResponseEnvelope, encode_frame,
};
use rockbee_node::transport::{FrameReader, read_frame, write_frame};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::time::timeout;

fn get(key: &str) -> Request {
    Request::Get { key: key.into() }
}
async fn receive(reader: &mut FrameReader<tokio::net::tcp::OwnedReadHalf>) -> RequestEnvelope {
    RequestEnvelope::decode(&read_frame(reader).await.unwrap().unwrap()).unwrap()
}
async fn reply(
    output: &mut tokio::net::tcp::OwnedWriteHalf,
    request: &RequestEnvelope,
    value: &[u8],
) {
    write_frame(
        output,
        &ResponseEnvelope {
            request_id: request.request_id,
            response: Response::Value {
                value: Some(value.to_vec()),
            },
        },
    )
    .await
    .unwrap();
}

// A per-call socket or a serialized exchange cannot deliver two frames before a reply.
#[tokio::test]
async fn overlapping_calls_correlate_reordered_replies_and_reuse_one_socket() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let router = Arc::new(Router::new(
        "127.0.0.1:1".into(),
        vec![listener.local_addr()?.to_string()],
    )?);
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);
        let a = receive(&mut input).await;
        let b = timeout(Duration::from_millis(500), receive(&mut input))
            .await
            .expect("two requests must arrive on the same socket before any reply");
        assert!(a.request_id > 0);
        assert_ne!(a.request_id, b.request_id);
        let Request::Get { key: b_key } = &b.request else {
            panic!("expected GET")
        };
        let Request::Get { key: a_key } = &a.request else {
            panic!("expected GET")
        };
        reply(&mut output, &b, b_key).await;
        reply(&mut output, &a, a_key).await;
        let c = receive(&mut input).await;
        assert_ne!(a.request_id, c.request_id);
        assert_ne!(b.request_id, c.request_id);
        reply(&mut output, &c, b"third").await;
        assert!(
            timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err(),
            "a reused link must not open another socket"
        );
        assert!(
            read_frame(&mut input).await.unwrap().is_none(),
            "Router drop must close the link"
        );
    });
    let (a, b) = tokio::join!(router.route(get("first")), router.route(get("second")));
    assert_eq!(
        a?,
        Response::Value {
            value: Some("first".into())
        }
    );
    assert_eq!(
        b?,
        Response::Value {
            value: Some("second".into())
        }
    );
    assert_eq!(
        router.route(get("third")).await?,
        Response::Value {
            value: Some("third".into())
        }
    );
    drop(router);
    timeout(Duration::from_secs(2), peer).await??;
    Ok(())
}

// Retaining canceled entries or retrying their bytes breaks cleanup/no replay.
#[tokio::test]
async fn canceling_registered_mutation_closes_link_and_next_call_reconnects() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let router = Arc::new(Router::new(
        listener.local_addr()?.to_string(),
        vec!["127.0.0.1:1".into()],
    )?);
    let (seen, ready) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);
        let a = receive(&mut input).await;
        assert_eq!(
            a.request,
            Request::Put {
                key: b"k".to_vec(),
                value: b"uncertain".to_vec()
            }
        );
        // A partial reply must be discarded with this socket when canceled.
        output.write_all(&[0, 0, 0, 1, 0]).await.unwrap();
        seen.send(()).unwrap();
        match timeout(Duration::from_secs(1), read_frame(&mut input))
            .await
            .unwrap()
        {
            Ok(None) => {}
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
            other => panic!("canceled partial response must close/reset socket: {other:?}"),
        }
        let (stream, _) = listener.accept().await.unwrap();
        let (input, mut output) = stream.into_split();
        let next = receive(&mut FrameReader::new(input)).await;
        assert_eq!(
            next.request,
            Request::Put {
                key: b"k".to_vec(),
                value: b"new".to_vec()
            }
        );
        write_frame(
            &mut output,
            &ResponseEnvelope {
                request_id: next.request_id,
                response: Response::Ok,
            },
        )
        .await
        .unwrap();
    });
    let r = router.clone();
    let call = tokio::spawn(async move {
        r.route(Request::Put {
            key: "k".into(),
            value: "uncertain".into(),
        })
        .await
    });
    ready.await?;
    call.abort();
    assert!(call.await.unwrap_err().is_cancelled());
    assert_eq!(
        router
            .route(Request::Put {
                key: "k".into(),
                value: "new".into()
            })
            .await?,
        Response::Ok
    );
    peer.await?;
    Ok(())
}

// Local frame rejection must not write any prefix or retire a healthy link.
#[tokio::test]
async fn oversized_local_envelope_does_not_poison_reused_link() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let router = Router::new(
        listener.local_addr()?.to_string(),
        vec!["127.0.0.1:1".into()],
    )?;
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);
        for value in ["before", "after"] {
            let request = receive(&mut input).await;
            assert_eq!(
                request.request,
                Request::Put {
                    key: b"k".to_vec(),
                    value: value.as_bytes().to_vec()
                }
            );
            write_frame(
                &mut output,
                &ResponseEnvelope {
                    request_id: request.request_id,
                    response: Response::Ok,
                },
            )
            .await
            .unwrap();
        }
    });
    for value in [
        "before".to_owned(),
        "x".repeat(MAX_FRAME),
        "after".to_owned(),
    ] {
        let result = router
            .route(Request::Put {
                key: "k".into(),
                value: value.as_bytes().to_vec(),
            })
            .await;
        if value.len() >= MAX_FRAME {
            assert!(result.is_err());
        } else {
            assert_eq!(result?, Response::Ok);
        }
    }
    peer.await?;
    Ok(())
}

// Ignoring IDs, accepting malformed responses, or retaining a poisoned generation breaks this.
#[tokio::test]
async fn invalid_replies_retire_generation_and_later_calls_reconnect() -> Result<()> {
    let invalids = vec![
        encode_frame(&RequestEnvelope {
            request_id: 1,
            request: get("bad"),
        })?,
        encode_frame(&ResponseEnvelope {
            request_id: 0,
            response: Response::Value {
                value: Some(b"bad".to_vec()),
            },
        })?,
        encode_frame(&ResponseEnvelope {
            request_id: 999,
            response: Response::Value {
                value: Some(b"bad".to_vec()),
            },
        })?,
        encode_frame(&ResponseEnvelope {
            request_id: 1,
            response: Response::Ok,
        })?,
        vec![0, 0, 0, 1, 0],
        (MAX_FRAME as u32).to_be_bytes().to_vec(),
        vec![0, 0, 0, 2, 8],
    ];
    for invalid in invalids {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let router = Router::new(
            "127.0.0.1:1".into(),
            vec![listener.local_addr()?.to_string()],
        )?;
        let peer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (input, mut output) = stream.into_split();
            let _ = receive(&mut FrameReader::new(input)).await;
            let _ = output.write_all(&invalid).await;
            drop(output);
            let (stream, _) = listener.accept().await.unwrap();
            let (input, mut output) = stream.into_split();
            let request = receive(&mut FrameReader::new(input)).await;
            assert_eq!(request.request, get("new"));
            reply(&mut output, &request, b"clean").await;
        });
        assert!(router.route(get("bad")).await.is_err());
        assert_eq!(
            router.route(get("new")).await?,
            Response::Value {
                value: Some("clean".into())
            }
        );
        peer.await?;
    }
    Ok(())
}

// A fully received mutation whose ACK is lost must never be replayed on a fresh link.
#[tokio::test]
async fn lost_ack_fails_all_pending_mutations_without_replay_on_reconnect() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let router = Arc::new(Router::new(
        listener.local_addr()?.to_string(),
        vec!["127.0.0.1:1".into()],
    )?);
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (input, output) = stream.into_split();
        let mut input = FrameReader::new(input);
        for _ in 0..2 {
            let request = receive(&mut input).await;
            assert!(matches!(request.request, Request::Put { ref value, .. } if value == b"lost"));
        }
        drop(input);
        drop(output);
        let (stream, _) = listener.accept().await.unwrap();
        let (input, mut output) = stream.into_split();
        let request = receive(&mut FrameReader::new(input)).await;
        assert!(matches!(request.request, Request::Delete { .. }));
        write_frame(
            &mut output,
            &ResponseEnvelope {
                request_id: request.request_id,
                response: Response::Ok,
            },
        )
        .await
        .unwrap();
    });
    let (a, b) = tokio::join!(
        router.route(Request::Put {
            key: "a".into(),
            value: "lost".into()
        }),
        router.route(Request::Put {
            key: "b".into(),
            value: "lost".into()
        })
    );
    for result in [a, b] {
        assert!(result.unwrap_err().to_string().contains("unknown"));
    }
    assert_eq!(
        router
            .route(Request::Delete {
                key: "unrelated".into()
            })
            .await?,
        Response::Ok
    );
    peer.await?;
    Ok(())
}

// Bounds must reject overflow without bytes, while canceled queued calls release admission.
#[tokio::test]
async fn active_and_pending_bounds_cancel_queued_calls_without_sending_bytes() -> Result<()> {
    use rockbee_gateway::config::UpstreamConfig;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let router = Arc::new(Router::new_with_upstream(
        listener.local_addr()?.to_string(),
        vec!["127.0.0.1:1".into()],
        UpstreamConfig::new(1, 1)?,
    )?);
    let (first_seen, seen) = tokio::sync::oneshot::channel();
    let (release, ready) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);
        let first = receive(&mut input).await;
        assert_eq!(
            first.request,
            Request::Delete {
                key: b"active".to_vec()
            }
        );
        first_seen.send(()).unwrap();
        ready.await.unwrap();
        write_frame(
            &mut output,
            &ResponseEnvelope {
                request_id: first.request_id,
                response: Response::Ok,
            },
        )
        .await
        .unwrap();
        let next = receive(&mut input).await;
        assert_eq!(
            next.request,
            Request::Delete {
                key: b"progress".to_vec()
            }
        );
        write_frame(
            &mut output,
            &ResponseEnvelope {
                request_id: next.request_id,
                response: Response::Ok,
            },
        )
        .await
        .unwrap();
    });
    let r = router.clone();
    let first = tokio::spawn(async move {
        r.route(Request::Delete {
            key: "active".into(),
        })
        .await
    });
    seen.await?;
    let r = router.clone();
    let queued = tokio::spawn(async move {
        r.route(Request::Delete {
            key: "canceled".into(),
        })
        .await
    });
    // A task that first polls a semaphore wait is pending by the next scheduler turn.
    tokio::task::yield_now().await;
    let overflow = timeout(
        Duration::from_millis(200),
        router.route(Request::Delete {
            key: "overflow".into(),
        }),
    )
    .await?;
    assert!(overflow.unwrap_err().to_string().contains("pending limit"));
    queued.abort();
    assert!(queued.await.unwrap_err().is_cancelled());
    let r = router.clone();
    let next = tokio::spawn(async move {
        r.route(Request::Delete {
            key: "progress".into(),
        })
        .await
    });
    tokio::task::yield_now().await;
    release.send(()).unwrap();
    assert_eq!(first.await??, Response::Ok);
    assert_eq!(next.await??, Response::Ok);
    peer.await?;
    Ok(())
}

// This exercises durable Store writes and read-only readers behind actual TCP envelopes.
#[tokio::test(flavor = "multi_thread")]
async fn unchanged_client_gateway_file_storage_crud_and_durable_reopen() -> Result<()> {
    use rockbee_client::Client;
    use rockbee_gateway::Gateway;
    use rockbee_node::storage::Role;
    use rockbee_node::{Server, Store};
    use slatedb::object_store::{ObjectStore, local::LocalFileSystem};
    let directory = tempfile::tempdir()?;
    let objects: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(directory.path())?);
    let writer = Store::open(Role::Writer, "gateway-durable", objects.clone()).await?;
    writer.put(b"seed", b"ready").await?;
    let reader = Store::open(Role::Reader, "gateway-durable", objects.clone()).await?;
    let writer = Server::bind("127.0.0.1:0", writer).await?;
    let reader = Server::bind("127.0.0.1:0", reader).await?;
    let reader_address = reader.local_addr()?.to_string();
    let gateway = Gateway::bind(
        "127.0.0.1:0",
        writer.local_addr()?.to_string(),
        vec![reader_address.clone()],
    )
    .await?;
    let address = gateway.local_addr()?.to_string();
    let (stop_w, stopped_w) = tokio::sync::oneshot::channel();
    let (stop_r, stopped_r) = tokio::sync::oneshot::channel();
    let (stop_g, stopped_g) = tokio::sync::oneshot::channel();
    let w = tokio::spawn(writer.run_until(async {
        stopped_w.await?;
        Ok(())
    }));
    let r = tokio::spawn(reader.run_until(async {
        stopped_r.await?;
        Ok(())
    }));
    let g = tokio::spawn(gateway.run_until(async {
        stopped_g.await?;
        Ok(())
    }));
    let client = Client::connect(&address).await?;
    assert_eq!(
        client.get(b"seed").await?.as_deref(),
        Some(b"ready".as_slice())
    );
    client
        .put(b"durable", "Ümit\nvalue".as_bytes(), None)
        .await?;
    client.put(b"deleted", b"gone", None).await?;
    client.delete(b"deleted").await?;
    let until = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        if client.get(b"durable").await?.as_deref() == Some("Ümit\nvalue".as_bytes()) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < until,
            "reader failed to converge"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let read_only = Client::connect(&reader_address).await?;
    assert!(read_only.put(b"durable", b"corrupt", None).await.is_err());
    let read_only = Client::connect(&reader_address).await?;
    assert!(read_only.delete(b"durable").await.is_err());
    stop_g.send(()).unwrap();
    g.await??;
    stop_r.send(()).unwrap();
    r.await??;
    stop_w.send(()).unwrap();
    w.await??;
    let reopened = Store::open(Role::Writer, "gateway-durable", objects).await?;
    assert_eq!(
        reopened.get(b"durable").await?.as_deref(),
        Some("Ümit\nvalue".as_bytes())
    );
    assert_eq!(reopened.get(b"deleted").await?, None);
    reopened.close().await
}

// Duplicate replies must retire a live link instead of satisfying a later call.
#[tokio::test]
async fn duplicate_reply_retires_other_pending_calls() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let router = Router::new(
        "127.0.0.1:1".into(),
        vec![listener.local_addr()?.to_string()],
    )?;
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);
        let first = receive(&mut input).await;
        let _second = receive(&mut input).await;
        // One write contains both replies, preventing scheduling from hiding the duplicate.
        let response = encode_frame(&ResponseEnvelope {
            request_id: first.request_id,
            response: Response::Value {
                value: Some(b"first".to_vec()),
            },
        })
        .unwrap();
        let mut duplicated = response.clone();
        duplicated.extend_from_slice(&response);
        output.write_all(&duplicated).await.unwrap();
        assert!(
            timeout(Duration::from_secs(1), read_frame(&mut input))
                .await
                .unwrap()
                .unwrap()
                .is_none()
        );
    });
    let (a, b) = tokio::join!(router.route(get("a")), router.route(get("b")));
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    peer.await?;
    Ok(())
}
