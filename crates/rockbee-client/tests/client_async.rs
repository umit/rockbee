use rockbee_client::{MutationOutcome, Result, RockBeeClient};
use rockbee_protocol::transport::{FrameReader, read_frame, write_frame};
use rockbee_protocol::{MAX_FRAME, Request, RequestEnvelope, Response, ResponseEnvelope};
use std::net::TcpListener as StdTcpListener;
use tokio::net::TcpListener;
use tokio::time::{Duration, timeout};

#[test]
fn client_connect_without_a_tokio_runtime_returns_an_error() {
    let waker = std::task::Waker::noop();
    let mut context = std::task::Context::from_waker(waker);
    let mut connect = Box::pin(RockBeeClient::connect("127.0.0.1:1"));

    match Future::poll(connect.as_mut(), &mut context) {
        std::task::Poll::Ready(Err(error)) => {
            assert!(error.to_string().contains("Tokio runtime"));
        }
        std::task::Poll::Ready(Ok(_)) => panic!("connect unexpectedly succeeded without a runtime"),
        std::task::Poll::Pending => panic!("connect unexpectedly waited without a runtime"),
    }
}

#[tokio::test]
async fn client_routes_out_of_order_binary_get_replies_to_the_right_call() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?.to_string();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);
        let first =
            RequestEnvelope::decode(&read_frame(&mut input).await.unwrap().unwrap()).unwrap();
        let second =
            RequestEnvelope::decode(&read_frame(&mut input).await.unwrap().unwrap()).unwrap();
        for request in [second, first] {
            let Request::Get { key } = request.request else {
                panic!("expected get");
            };
            write_frame(
                &mut output,
                &ResponseEnvelope {
                    request_id: request.request_id,
                    response: Response::Value { value: Some(key) },
                },
            )
            .await
            .unwrap();
        }
    });

    let client = RockBeeClient::connect(&address).await?;
    let (left, right) = tokio::join!(client.get(&[0, 255]), client.get(&[128, 0]));
    assert_eq!(left?, Some(vec![0, 255]));
    assert_eq!(right?, Some(vec![128, 0]));
    server.await.unwrap();
    Ok(())
}

#[tokio::test]
async fn client_sends_binary_crud_requests() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?.to_string();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);
        for expected in [
            Request::Put {
                key: vec![0],
                value: vec![255],
            },
            Request::Get { key: vec![0] },
            Request::Delete { key: vec![0] },
        ] {
            let request =
                RequestEnvelope::decode(&read_frame(&mut input).await.unwrap().unwrap()).unwrap();
            assert_eq!(request.request, expected);
            let response = match request.request {
                Request::Get { .. } => Response::Value {
                    value: Some(vec![255]),
                },
                Request::Put { .. } | Request::PutWithTtl { .. } | Request::Delete { .. } => {
                    Response::Ok
                }
                Request::Status => panic!("public CRUD RockBeeClient must not send Status"),
                Request::Table(_) => panic!("byte CRUD fixture must not receive table requests"),
            };
            write_frame(
                &mut output,
                &ResponseEnvelope {
                    request_id: request.request_id,
                    response,
                },
            )
            .await
            .unwrap();
        }
    });

    let client = RockBeeClient::connect(&address).await?;
    client.put(&[0], &[255], None).await?;
    assert_eq!(client.get(&[0]).await?, Some(vec![255]));
    client.delete(&[0]).await?;
    server.await.unwrap();
    Ok(())
}

#[tokio::test]
async fn put_rounds_positive_sub_milliseconds_up_and_zero_stays_an_ordinary_put() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?.to_string();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);
        for expected in [
            Request::PutWithTtl {
                key: b"rounded".to_vec(),
                value: b"value".to_vec(),
                ttl_millis: 1,
            },
            Request::Put {
                key: b"forever".to_vec(),
                value: b"value".to_vec(),
            },
        ] {
            let request =
                RequestEnvelope::decode(&read_frame(&mut input).await.unwrap().unwrap()).unwrap();
            assert_eq!(request.request, expected);
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

    let client = RockBeeClient::connect(&address).await?;
    client
        .put(b"rounded", b"value", Some(Duration::from_nanos(1)))
        .await?;
    client
        .put(b"forever", b"value", Some(Duration::ZERO))
        .await?;
    server.await.unwrap();
    Ok(())
}

#[tokio::test]
async fn put_rejects_ttl_outside_signed_millisecond_range_without_sending() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?.to_string();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (input, _) = stream.into_split();
        let mut input = FrameReader::new(input);
        match timeout(Duration::from_millis(100), read_frame(&mut input)).await {
            Err(_) | Ok(Ok(None)) => {}
            Ok(Ok(Some(_))) => panic!("locally rejected TTL reached the wire"),
            Ok(Err(error)) => panic!("unexpected wire error: {error}"),
        }
    });
    let client = RockBeeClient::connect(&address).await?;
    let error = client
        .put(
            b"key",
            b"value",
            Some(Duration::from_millis(i64::MAX as u64 + 1)),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("signed millisecond"));
    server.await.unwrap();
    Ok(())
}

#[tokio::test]
async fn put_submits_before_its_handle_is_polled_and_routes_out_of_order_acks() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?.to_string();
    let (received, received_notice) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);
        let first = timeout(Duration::from_millis(250), read_frame(&mut input))
            .await
            .expect("first PUT must arrive before its handle is polled")
            .unwrap()
            .unwrap();
        let second = timeout(Duration::from_millis(250), read_frame(&mut input))
            .await
            .expect("second PUT must arrive before its handle is polled")
            .unwrap()
            .unwrap();
        let first = RequestEnvelope::decode(&first).unwrap();
        let second = RequestEnvelope::decode(&second).unwrap();
        assert_eq!(
            first.request,
            Request::PutWithTtl {
                key: b"first".to_vec(),
                value: b"value".to_vec(),
                ttl_millis: 2,
            }
        );
        assert!(matches!(second.request, Request::Put { .. }));
        received.send(()).unwrap();
        for request in [second, first] {
            let response = match request.request {
                Request::PutWithTtl { key, .. } if key == b"first" => Response::Error {
                    mutation_outcome: MutationOutcome::Unknown,
                    message: "first rejected".into(),
                },
                Request::Put { key, .. } if key == b"second" => Response::Ok,
                _ => panic!("unexpected request"),
            };
            write_frame(
                &mut output,
                &ResponseEnvelope {
                    request_id: request.request_id,
                    response,
                },
            )
            .await
            .unwrap();
        }
    });

    let client = RockBeeClient::connect(&address).await?;
    let first = client.put(b"first", b"value", Some(Duration::from_millis(2)));
    let second = client.put(b"second", b"value", None);
    timeout(Duration::from_millis(250), received_notice)
        .await
        .expect("both PUTs must arrive before either handle is polled")
        .unwrap();
    assert!(first.await.is_err());
    second.await?;
    server.await.unwrap();
    Ok(())
}

#[tokio::test]
async fn dropping_a_put_handle_does_not_cancel_other_in_flight_puts() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?.to_string();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);
        let first =
            RequestEnvelope::decode(&read_frame(&mut input).await.unwrap().unwrap()).unwrap();
        let second =
            RequestEnvelope::decode(&read_frame(&mut input).await.unwrap().unwrap()).unwrap();
        let retained = [first, second]
            .into_iter()
            .find(|request| matches!(&request.request, Request::Put { key, .. } if key == b"retained"))
            .unwrap();
        write_frame(
            &mut output,
            &ResponseEnvelope {
                request_id: retained.request_id,
                response: Response::Ok,
            },
        )
        .await
        .unwrap();
    });

    let client = RockBeeClient::connect(&address).await?;
    let dropped = client.put(b"dropped", b"value", None);
    let retained = client.put(b"retained", b"value", None);
    drop(dropped);
    retained.await?;
    server.await.unwrap();
    Ok(())
}

#[tokio::test]
async fn dropping_the_last_client_does_not_cancel_an_accepted_put() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?.to_string();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);
        let request =
            RequestEnvelope::decode(&read_frame(&mut input).await.unwrap().unwrap()).unwrap();
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

    let client = RockBeeClient::connect(&address).await?;
    let accepted = client.put(b"accepted", b"value", None);
    drop(client);
    accepted.await?;
    server.await.unwrap();
    Ok(())
}

#[tokio::test]
async fn dropped_put_handle_times_out_without_replaying_the_mutation() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?.to_string();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (input, _output) = stream.into_split();
        let mut input = FrameReader::new(input);
        let request =
            RequestEnvelope::decode(&read_frame(&mut input).await.unwrap().unwrap()).unwrap();
        assert!(matches!(request.request, Request::Put { .. }));
        tokio::time::sleep(Duration::from_millis(5_200)).await;
        match timeout(Duration::from_millis(100), read_frame(&mut input)).await {
            Ok(Ok(Some(_))) => panic!("timed-out mutation must not be replayed"),
            Ok(Ok(None)) | Err(_) => {}
            Ok(Err(error)) => panic!("unexpected original connection error: {error}"),
        }

        let (stream, _) = listener.accept().await.unwrap();
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);
        let get = RequestEnvelope::decode(&read_frame(&mut input).await.unwrap().unwrap()).unwrap();
        assert_eq!(
            get.request,
            Request::Get {
                key: b"after-timeout".to_vec()
            }
        );
        write_frame(
            &mut output,
            &ResponseEnvelope {
                request_id: get.request_id,
                response: Response::Value { value: None },
            },
        )
        .await
        .unwrap();
    });

    let client = RockBeeClient::connect(&address).await?;
    drop(client.put(b"lost", b"value", None));
    tokio::time::sleep(Duration::from_millis(5_100)).await;
    assert_eq!(client.get(b"after-timeout").await?, None);
    server.await.unwrap();
    Ok(())
}

#[test]
fn runtime_shutdown_reports_unknown_after_put_reaches_peer() -> Result<()> {
    let listener = StdTcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?.to_string();
    let runtime = tokio::runtime::Runtime::new()?;
    let client = runtime.block_on(RockBeeClient::connect(&address))?;
    let pending = client.put(b"pending", b"value", None);
    let (mut peer, _) = listener.accept()?;
    peer.set_read_timeout(Some(Duration::from_secs(2)))?;
    let frame = rockbee_protocol::read_frame(&mut peer)?.ok_or("PUT did not reach peer")?;
    assert!(matches!(
        RequestEnvelope::decode(&frame)?.request,
        Request::Put { .. }
    ));
    // A merely created handle can fail before enqueue and correctly be NotSent.
    // Only stop the runtime after proving this request was actually delivered.
    drop(client);
    drop(runtime);

    let collector = tokio::runtime::Runtime::new()?;
    let error = collector.block_on(pending).unwrap_err();
    assert_eq!(error.outcome(), MutationOutcome::Unknown);
    drop(listener);
    Ok(())
}

#[tokio::test]
async fn eager_puts_share_the_link_budget_and_recover_without_sending_rejections() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?.to_string();
    let client = RockBeeClient::connect(&address).await?;
    let clone = client.clone();
    let mut handles = Vec::new();
    // No peer is polled yet, so no ACK can release an admission while filling
    // the documented shared 128 active + 128 waiting budget.
    for sequence in 0_u16..256 {
        let key = sequence.to_be_bytes();
        handles.push(client.put(&key, &[b'v', key[0], key[1]], None));
    }
    assert!(clone.get(b"rejected-get").await.is_err());
    for error in [
        clone.delete(b"rejected-delete").await.unwrap_err(),
        clone
            .put(b"rejected-put", b"value", None)
            .await
            .unwrap_err(),
    ] {
        assert_eq!(error.outcome(), MutationOutcome::NotSent);
    }

    let peer = async move {
        // Only this socket is accepted. Reconnecting after a local rejection
        // cannot satisfy this transcript or the final EOF check.
        let (stream, _) = listener.accept().await?;
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);
        let mut seen = [false; 256];
        for _ in 0..256 {
            let frame = read_frame(&mut input)
                .await?
                .ok_or("connection closed before accepted PUTs completed")?;
            let envelope = RequestEnvelope::decode(&frame)?;
            let Request::Put { key, value } = envelope.request else {
                panic!("only accepted PUTs may reach the peer before recovery");
            };
            assert_eq!(key.len(), 2, "a locally rejected key reached the wire");
            let sequence = u16::from_be_bytes([key[0], key[1]]) as usize;
            assert!(sequence < seen.len(), "unexpected accepted key");
            assert!(!seen[sequence], "accepted PUT was duplicated");
            seen[sequence] = true;
            assert_eq!(value, [b'v', key[0], key[1]]);
            write_frame(
                &mut output,
                &ResponseEnvelope {
                    request_id: envelope.request_id,
                    response: Response::Ok,
                },
            )
            .await?;
        }
        assert!(seen.into_iter().all(|received| received));
        for (expected, response) in [
            (
                Request::Put {
                    key: b"recovered".to_vec(),
                    value: vec![0, 255, 128],
                },
                Response::Ok,
            ),
            (
                Request::Get {
                    key: b"recovered".to_vec(),
                },
                Response::Value {
                    value: Some(vec![0, 255, 128]),
                },
            ),
        ] {
            let frame = read_frame(&mut input)
                .await?
                .ok_or("original connection closed before recovery")?;
            let envelope = RequestEnvelope::decode(&frame)?;
            assert_eq!(envelope.request, expected, "unexpected request after drain");
            write_frame(
                &mut output,
                &ResponseEnvelope {
                    request_id: envelope.request_id,
                    response,
                },
            )
            .await?;
        }
        assert!(
            read_frame(&mut input).await?.is_none(),
            "a rejected or duplicate request arrived after recovery"
        );
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let collect_and_recover = async move {
        for handle in handles {
            handle.await?;
        }
        clone.put(b"recovered", &[0, 255, 128], None).await?;
        assert_eq!(client.get(b"recovered").await?, Some(vec![0, 255, 128]));
        drop(clone);
        drop(client);
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    timeout(Duration::from_secs(10), async {
        tokio::try_join!(peer, collect_and_recover)
    })
    .await
    .map_err(|_| "admission transcript or recovery exceeded its bound")??;
    Ok(())
}

#[tokio::test]
async fn client_does_not_replay_a_failed_mutation() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?.to_string();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);
        let request =
            RequestEnvelope::decode(&read_frame(&mut input).await.unwrap().unwrap()).unwrap();
        assert!(matches!(request.request, Request::Put { .. }));
        write_frame(
            &mut output,
            &ResponseEnvelope {
                request_id: request.request_id,
                response: Response::Error {
                    mutation_outcome: MutationOutcome::Unknown,
                    message: "writer rejected mutation".into(),
                },
            },
        )
        .await
        .unwrap();
        assert!(
            timeout(Duration::from_millis(100), read_frame(&mut input))
                .await
                .is_err()
        );
    });

    let client = RockBeeClient::connect(&address).await?;
    assert!(client.put(b"key", b"value", None).await.is_err());
    server.await.unwrap();
    Ok(())
}

#[tokio::test]
async fn oversized_request_does_not_send_and_the_client_remains_usable() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?.to_string();
    let (checked, continue_server) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);
        assert!(
            timeout(Duration::from_millis(100), read_frame(&mut input))
                .await
                .is_err()
        );
        checked.send(()).unwrap();
        let request =
            RequestEnvelope::decode(&read_frame(&mut input).await.unwrap().unwrap()).unwrap();
        assert_eq!(request.request, Request::Get { key: b"k".to_vec() });
        write_frame(
            &mut output,
            &ResponseEnvelope {
                request_id: request.request_id,
                response: Response::Value { value: None },
            },
        )
        .await
        .unwrap();
    });

    let client = RockBeeClient::connect(&address).await?;
    assert!(
        client
            .put(b"k", &vec![0; MAX_FRAME - 4], None)
            .await
            .is_err()
    );
    continue_server.await.unwrap();
    assert_eq!(client.get(b"k").await?, None);
    server.await.unwrap();
    Ok(())
}

#[tokio::test]
async fn malformed_response_keeps_get_error_distinct_from_uncertain_put_and_reconnects()
-> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?.to_string();
    let mut server = tokio::spawn(async move {
        let (stream, _) = timeout(Duration::from_secs(1), listener.accept())
            .await
            .map_err(|_| "first peer accept timed out")??;
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);
        let mut saw_get = false;
        let mut saw_put = false;
        for _ in 0..2 {
            let frame = timeout(Duration::from_secs(1), read_frame(&mut input))
                .await
                .map_err(|_| "concurrent request read timed out")??
                .ok_or("first connection closed before both requests")?;
            match RequestEnvelope::decode(&frame)?.request {
                Request::Get { key } if key == b"pending-get" => saw_get = true,
                Request::Put { key, value } if key == b"pending-put" && value == b"value" => {
                    saw_put = true
                }
                request => {
                    return Err(format!("unexpected first-generation request: {request:?}").into());
                }
            }
        }
        if !saw_get || !saw_put {
            return Err("first generation did not receive both GET and PUT".into());
        }
        timeout(
            Duration::from_secs(1),
            write_frame(
                &mut output,
                &ResponseEnvelope {
                    request_id: 999,
                    response: Response::Ok,
                },
            ),
        )
        .await
        .map_err(|_| "malformed response write timed out")??;
        match timeout(Duration::from_secs(1), read_frame(&mut input)).await {
            Ok(Ok(None)) => {}
            Ok(Err(error)) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
            Ok(Ok(Some(frame))) => {
                return Err(
                    format!("retired generation received replayed frame: {frame:?}").into(),
                );
            }
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => return Err("retired generation did not close its old socket".into()),
        }

        let (stream, _) = timeout(Duration::from_secs(1), listener.accept())
            .await
            .map_err(|_| "replacement peer accept timed out")??;
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);
        let frame = timeout(Duration::from_secs(1), read_frame(&mut input))
            .await
            .map_err(|_| "replacement request read timed out")??
            .ok_or("replacement connection closed before GET")?;
        let request = RequestEnvelope::decode(&frame)?;
        if request.request
            != (Request::Get {
                key: b"recovered".to_vec(),
            })
        {
            return Err(format!("unexpected replacement request: {:?}", request.request).into());
        }
        timeout(
            Duration::from_secs(1),
            write_frame(
                &mut output,
                &ResponseEnvelope {
                    request_id: request.request_id,
                    response: Response::Value {
                        value: Some(b"exact-recovered-bytes".to_vec()),
                    },
                },
            ),
        )
        .await
        .map_err(|_| "replacement response write timed out")??;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let client = RockBeeClient::connect(&address).await?;
    let put = client.put(b"pending-put", b"value", None);
    let (get, put) = tokio::join!(client.get(b"pending-get"), put);
    let body: Result<()> = async {
        let get_error = match get {
            Err(error) => error.to_string(),
            Ok(value) => {
                return Err(
                    format!("malformed response unexpectedly returned GET {value:?}").into(),
                );
            }
        };
        if !get_error.contains("unknown or duplicate upstream ID")
            || get_error.contains("a sent mutation's outcome is unknown")
        {
            return Err(
                format!("GET error had the wrong mutation classification: {get_error:?}").into(),
            );
        }
        let put_error = match put {
            Err(error) => error,
            Ok(()) => return Err("malformed response unexpectedly acknowledged PUT".into()),
        };
        if put_error.outcome() != MutationOutcome::Unknown {
            return Err(
                format!("PUT error omitted uncertain mutation state: {put_error:?}").into(),
            );
        }
        let recovered = client.get(b"recovered").await?;
        if recovered != Some(b"exact-recovered-bytes".to_vec()) {
            return Err(format!("replacement GET returned {recovered:?}").into());
        }
        Ok(())
    }
    .await;

    if let Err(error) = body {
        server.abort();
        let _ = server.await;
        return Err(error);
    }
    match timeout(Duration::from_secs(2), &mut server).await {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(error))) => Err(error),
        Ok(Err(error)) => Err(error.into()),
        Err(_) => {
            server.abort();
            let _ = server.await;
            Err("malformed-response peer did not finish within two seconds".into())
        }
    }
}
