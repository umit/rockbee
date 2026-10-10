use super::*;
use crate::transport::{FrameReader, read_frame, write_frame};
use crate::{RequestEnvelope, ResponseEnvelope};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;

async fn request(input: tokio::net::tcp::OwnedReadHalf) -> RequestEnvelope {
    let mut input = FrameReader::new(input);
    RequestEnvelope::decode(&read_frame(&mut input).await.unwrap().unwrap()).unwrap()
}

#[tokio::test]
async fn expired_connector_waiters_release_slots_without_connecting() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let link = Link::new(listener.local_addr()?.to_string(), 2, 1);
    let locked = link.connection.lock().await;
    assert!(
        link.exchange(
            &Request::Delete {
                key: b"expired".to_vec()
            },
            Instant::now() + Duration::from_millis(20),
        )
        .await
        .is_err()
    );
    assert_eq!(link.active.available_permits(), 2);
    assert_eq!(link.waiting.available_permits(), 1);
    drop(locked);
    assert!(
        timeout_at(
            Instant::now() + Duration::from_millis(20),
            listener.accept()
        )
        .await
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn canceling_a_waiting_get_preserves_registered_sibling_and_connection() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?.to_string();
    let link =
        Arc::new(Link::connect(address, 1, 1, Instant::now() + Duration::from_secs(1)).await?);
    let (first_registered, first_registered_wait) = oneshot::channel();
    let (release_first, release_first_wait) = oneshot::channel();
    let mut peer = tokio::spawn(async move {
        let (stream, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
            .await
            .map_err(|_| "peer accept timed out")??;
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);

        let first = RequestEnvelope::decode(
            &tokio::time::timeout(Duration::from_secs(1), read_frame(&mut input))
                .await
                .map_err(|_| "first request read timed out")??
                .ok_or("connection closed before the first request")?,
        )?;
        if first.request_id != 1
            || first.request
                != (Request::Get {
                    key: b"first".to_vec(),
                })
        {
            return Err(format!("unexpected first request: {first:?}").into());
        }
        first_registered
            .send(())
            .map_err(|_| "first request observer was dropped")?;
        tokio::time::timeout(Duration::from_secs(1), release_first_wait)
            .await
            .map_err(|_| "first reply was not released")?
            .map_err(|_| "first reply release sender was dropped")?;
        write_frame(
            &mut output,
            &ResponseEnvelope {
                request_id: first.request_id,
                response: Response::Value {
                    value: Some(b"first-value".to_vec()),
                },
            },
        )
        .await?;

        let third = RequestEnvelope::decode(
            &tokio::time::timeout(Duration::from_secs(1), read_frame(&mut input))
                .await
                .map_err(|_| "third request read timed out")??
                .ok_or("connection closed before the third request")?,
        )?;
        if third.request_id != 2
            || third.request
                != (Request::Get {
                    key: b"third".to_vec(),
                })
        {
            return Err(
                format!("canceled waiting request leaked onto the connection: {third:?}").into(),
            );
        }
        write_frame(
            &mut output,
            &ResponseEnvelope {
                request_id: third.request_id,
                response: Response::Value {
                    value: Some(b"third-value".to_vec()),
                },
            },
        )
        .await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let first_link = Arc::clone(&link);
    let mut first_task = Some(tokio::spawn(async move {
        first_link
            .exchange(
                &Request::Get {
                    key: b"first".to_vec(),
                },
                Instant::now() + Duration::from_secs(1),
            )
            .await
    }));
    let mut waiting_task = None;
    let mut third_task = None;

    let body: Result<()> = async {
        tokio::time::timeout(Duration::from_secs(1), first_registered_wait)
            .await
            .map_err(|_| "first request was not registered")?
            .map_err(|_| "peer stopped before observing the first request")?;

        let waiting_link = Arc::clone(&link);
        waiting_task = Some(tokio::spawn(async move {
            waiting_link
                .exchange(
                    &Request::Get {
                        key: b"canceled-waiting".to_vec(),
                    },
                    Instant::now() + Duration::from_secs(1),
                )
                .await
        }));
        tokio::time::timeout(Duration::from_secs(1), async {
            while link.waiting.available_permits() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| "second GET did not reserve its waiting admission")?;
        let waiting_result = {
            let waiting = waiting_task
                .as_mut()
                .ok_or("waiting GET task was not started")?;
            waiting.abort();
            tokio::time::timeout(Duration::from_secs(1), waiting).await
        };
        if waiting_result.is_ok() {
            drop(waiting_task.take());
        }
        match waiting_result {
            Ok(Err(error)) if error.is_cancelled() => {}
            Ok(Ok(result)) => {
                return Err(
                    format!("waiting GET completed instead of canceling: {result:?}").into(),
                );
            }
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => return Err("waiting GET abort did not finish".into()),
        }
        if link.waiting.available_permits() != 1 || link.active.available_permits() != 0 {
            return Err("canceling the waiting GET did not preserve its sibling admission".into());
        }

        let third_link = Arc::clone(&link);
        third_task = Some(tokio::spawn(async move {
            third_link
                .exchange(
                    &Request::Get {
                        key: b"third".to_vec(),
                    },
                    Instant::now() + Duration::from_secs(1),
                )
                .await
        }));
        tokio::time::timeout(Duration::from_secs(1), async {
            while link.waiting.available_permits() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| "third GET did not reuse the canceled waiting admission")?;

        release_first
            .send(())
            .map_err(|_| "peer stopped before the first reply could be released")?;
        let first_result = {
            let first = first_task
                .as_mut()
                .ok_or("first GET task was not started")?;
            tokio::time::timeout(Duration::from_secs(1), first).await
        };
        if first_result.is_ok() {
            drop(first_task.take());
        }
        let first_result = first_result
            .map_err(|_| "first GET did not finish")?
            .map_err(|error| format!("first GET task failed: {error}"))?;
        if first_result?
            != (Response::Value {
                value: Some(b"first-value".to_vec()),
            })
        {
            return Err("first GET did not receive its exact response".into());
        }

        let third_result = {
            let third = third_task
                .as_mut()
                .ok_or("third GET task was not started")?;
            tokio::time::timeout(Duration::from_secs(1), third).await
        };
        if third_result.is_ok() {
            drop(third_task.take());
        }
        let third_result = third_result
            .map_err(|_| "third GET did not finish")?
            .map_err(|error| format!("third GET task failed: {error}"))?;
        if third_result?
            != (Response::Value {
                value: Some(b"third-value".to_vec()),
            })
        {
            return Err("third GET did not receive its exact response".into());
        }
        if link.active.available_permits() != 1 || link.waiting.available_permits() != 1 {
            return Err("GET completion did not restore Link admissions".into());
        }
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;

    if let Err(error) = body {
        if let Some(first_task) = &first_task {
            first_task.abort();
        }
        if let Some(waiting_task) = &waiting_task {
            waiting_task.abort();
        }
        if let Some(third_task) = &third_task {
            third_task.abort();
        }
        peer.abort();
        if let Some(first_task) = first_task {
            let _ = first_task.await;
        }
        if let Some(waiting_task) = waiting_task {
            let _ = waiting_task.await;
        }
        if let Some(third_task) = third_task {
            let _ = third_task.await;
        }
        let _ = peer.await;
        return Err(error);
    }
    match tokio::time::timeout(Duration::from_secs(1), &mut peer).await {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(error))) => Err(error),
        Ok(Err(error)) => Err(error.into()),
        Err(_) => {
            peer.abort();
            let _ = peer.await;
            Err("peer did not finish within one second".into())
        }
    }
}

#[test]
fn synchronous_admission_reserves_the_shared_active_and_waiting_budget() -> Result<()> {
    let link = Link::new("127.0.0.1:1".into(), 1, 1);
    let active = link.try_admit()?;
    let waiting = link.try_admit()?;
    assert!(link.try_admit().is_err());
    drop(active);
    let replacement_active = link.try_admit()?;
    assert!(
        link.try_admit().is_err(),
        "active plus waiting stays bounded"
    );
    drop(waiting);
    assert!(
        link.try_admit().is_ok(),
        "released waiting slot is reusable"
    );
    drop(replacement_active);
    Ok(())
}

#[tokio::test]
async fn failed_enqueue_retires_the_generation_and_never_registers_this_frame() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;

    for request in [
        Request::Get {
            key: b"read".to_vec(),
        },
        Request::Put {
            key: b"write".to_vec(),
            value: b"value".to_vec(),
        },
        Request::Delete {
            key: b"delete".to_vec(),
        },
    ] {
        let stream = TcpStream::connect(listener.local_addr()?).await?;
        let (peer, _) = listener.accept().await?;
        let mut connection = Arc::new(Connection::new(stream, 1));
        let (closed_sender, closed_receiver) = mpsc::channel(1);
        drop(closed_receiver);
        Arc::get_mut(&mut connection)
            .ok_or("test connection unexpectedly has another owner")?
            .send = closed_sender;

        let permits = Arc::new(Semaphore::new(1));
        let error = match connection.register(&request, permits.clone().try_acquire_owned()?) {
            Ok(_) => return Err("closed enqueue unexpectedly registered a request".into()),
            Err(error) => error,
        };
        let message = error.to_string();
        assert!(message.contains("upstream frame queue closed or full"));
        let state = connection.shared.state.lock().unwrap();
        assert!(state.closed);
        assert!(state.pending.is_empty());
        drop(state);
        assert_eq!(permits.available_permits(), 1);

        drop(connection);
        drop(peer);
    }
    Ok(())
}

#[tokio::test]
async fn admission_cannot_be_used_on_a_different_link() -> Result<()> {
    let first = Link::new("127.0.0.1:1".into(), 1, 1);
    let second = Link::new("127.0.0.1:1".into(), 1, 1);
    let admission = first.try_admit()?;
    assert!(
        second
            .exchange_admitted(
                &Request::Delete { key: vec![0] },
                Instant::now() + Duration::from_secs(1),
                admission,
            )
            .await
            .is_err()
    );
    assert_eq!(first.active.available_permits(), 1);
    Ok(())
}

#[tokio::test]
async fn timed_out_mutation_retires_link_and_is_not_replayed() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let link = tokio::time::timeout(
        Duration::from_secs(1),
        Link::connect(
            listener.local_addr()?.to_string(),
            1,
            1,
            Instant::now() + Duration::from_secs(1),
        ),
    )
    .await
    .map_err(|_| "initial link connect timed out")??;
    let mut peers = tokio::task::JoinSet::new();
    peers.spawn(async move {
        let (stream, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
            .await
            .map_err(|_| "first peer accept timed out")??;
        let (input, output) = stream.into_split();
        let mut input = FrameReader::new(input);
        let first = RequestEnvelope::decode(
            &tokio::time::timeout(Duration::from_secs(1), read_frame(&mut input))
                .await
                .map_err(|_| "first request read timed out")??
                .ok_or("first connection closed before its request")?,
        )?;
        assert_eq!(
            first.request,
            Request::Delete {
                key: b"lost".to_vec()
            }
        );
        assert_eq!(first.request_id, 1);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), read_frame(&mut input))
                .await
                .map_err(|_| "client did not close the timed-out connection")??
                .is_none(),
            "timed-out request must close its old connection without a replay"
        );
        drop(output);

        let (stream, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
            .await
            .map_err(|_| "replacement peer accept timed out")??;
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);
        let second = RequestEnvelope::decode(
            &tokio::time::timeout(Duration::from_secs(1), read_frame(&mut input))
                .await
                .map_err(|_| "replacement request read timed out")??
                .ok_or("replacement connection closed before its request")?,
        )?;
        assert_eq!(
            second.request,
            Request::Delete {
                key: b"new".to_vec()
            }
        );
        assert_eq!(second.request_id, 1);
        tokio::time::timeout(
            Duration::from_secs(1),
            write_frame(
                &mut output,
                &ResponseEnvelope {
                    request_id: second.request_id,
                    response: Response::Ok,
                },
            ),
        )
        .await
        .map_err(|_| "replacement response write timed out")??;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });

    let body: Result<()> = async {
        let started = Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            link.exchange(
                &Request::Delete {
                    key: b"lost".to_vec(),
                },
                Instant::now() + Duration::from_millis(100),
            ),
        )
        .await
        .map_err(|_| "timed-out mutation did not finish")?;
        let error = match result {
            Ok(response) => {
                return Err(
                    format!("unanswered mutation unexpectedly returned {response:?}").into(),
                );
            }
            Err(error) => error,
        };
        let message = error.to_string();
        assert!(message.contains("upstream request timed out"));
        assert!(message.contains("mutation outcome is unknown"));
        assert!(started.elapsed() >= Duration::from_millis(50));
        assert_eq!(link.active.available_permits(), 1);
        assert_eq!(link.waiting.available_permits(), 1);

        assert_eq!(
            tokio::time::timeout(
                Duration::from_secs(1),
                link.exchange(
                    &Request::Delete {
                        key: b"new".to_vec(),
                    },
                    Instant::now() + Duration::from_secs(1),
                ),
            )
            .await
            .map_err(|_| "replacement mutation did not finish")??,
            Response::Ok
        );
        assert_eq!(link.active.available_permits(), 1);
        assert_eq!(link.waiting.available_permits(), 1);
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;

    if let Err(error) = body {
        peers.abort_all();
        while peers.join_next().await.is_some() {}
        return Err(error);
    }
    match tokio::time::timeout(Duration::from_secs(1), peers.join_next()).await {
        Ok(Some(Ok(Ok(())))) => Ok(()),
        Ok(Some(Ok(Err(error)))) => Err(error),
        Ok(Some(Err(error))) => Err(error.into()),
        Ok(None) => Err("peer task was missing".into()),
        Err(_) => {
            peers.abort_all();
            while peers.join_next().await.is_some() {}
            Err("peer did not complete within one second".into())
        }
    }
}

#[tokio::test]
async fn wrong_response_kind_retires_link_before_a_mutation_can_be_accepted() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let link = Link::new(listener.local_addr()?.to_string(), 1, 1);
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (input, mut output) = stream.into_split();
        let request = request(input).await;
        write_frame(
            &mut output,
            &ResponseEnvelope {
                request_id: request.request_id,
                response: Response::Value {
                    value: Some(vec![1]),
                },
            },
        )
        .await
        .unwrap();
    });
    assert!(
        link.exchange(
            &Request::Delete { key: b"k".to_vec() },
            Instant::now() + Duration::from_secs(1)
        )
        .await
        .is_err()
    );
    peer.await?;
    Ok(())
}

#[tokio::test]
async fn old_registered_guard_cannot_retire_replacement_generation() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let link = Link::new(listener.local_addr()?.to_string(), 2, 1);
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (input, mut output) = stream.into_split();
        let first = request(input).await;
        output.write_all(&[0, 0, 0, 1, 0xff]).await.unwrap();
        let (stream, _) = listener.accept().await.unwrap();
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);
        for key in [b"replacement".as_slice(), b"still-live".as_slice()] {
            let request =
                RequestEnvelope::decode(&read_frame(&mut input).await.unwrap().unwrap()).unwrap();
            assert_eq!(request.request, Request::Delete { key: key.to_vec() });
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
        drop(first);
    });
    let stream = TcpStream::connect(&link.address).await?;
    let old = Arc::new(Connection::new(stream, 2));
    *link.connection.lock().await = Some(old.clone());
    let permit = link.active.clone().acquire_owned().await?;
    let (failed, old_guard) = old.register(
        &Request::Delete {
            key: b"old".to_vec(),
        },
        permit,
    )?;
    assert!(failed.await?.is_err());
    assert_eq!(
        link.exchange(
            &Request::Delete {
                key: b"replacement".to_vec()
            },
            Instant::now() + Duration::from_secs(1)
        )
        .await?,
        Response::Ok
    );
    drop(old_guard);
    drop(old);
    assert_eq!(
        link.exchange(
            &Request::Delete {
                key: b"still-live".to_vec()
            },
            Instant::now() + Duration::from_secs(1)
        )
        .await?,
        Response::Ok
    );
    peer.await?;
    Ok(())
}

#[tokio::test]
async fn exhausted_ids_retire_without_writing_or_leaking_admission() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let stream = TcpStream::connect(listener.local_addr()?).await?;
    let (peer, _) = listener.accept().await?;
    let connection = Arc::new(Connection::new(stream, 1));
    connection.shared.state.lock().unwrap().last_id = u64::MAX;
    let permits = Arc::new(Semaphore::new(1));
    assert!(
        connection
            .register(
                &Request::Delete {
                    key: b"never".to_vec()
                },
                permits.clone().acquire_owned().await?
            )
            .is_err()
    );
    assert_eq!(permits.available_permits(), 1);
    let mut peer = FrameReader::new(peer);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), read_frame(&mut peer))
            .await??
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn multiplexed_status_responses_remain_correlated_when_replied_out_of_order() -> Result<()> {
    use crate::{ServiceRole, StatusSnapshot};

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?.to_string();
    let peer = tokio::spawn(async move {
        let (stream, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept()).await??;
        let (input, mut output) = stream.into_split();
        let mut input = FrameReader::new(input);
        let first = RequestEnvelope::decode(
            &tokio::time::timeout(Duration::from_secs(1), read_frame(&mut input))
                .await??
                .ok_or("connection closed before first Status")?,
        )?;
        let second = RequestEnvelope::decode(
            &tokio::time::timeout(Duration::from_secs(1), read_frame(&mut input))
                .await??
                .ok_or("connection closed before second Status")?,
        )?;
        assert!(matches!(first.request, Request::Status));
        assert!(matches!(second.request, Request::Status));
        assert_ne!(first.request_id, second.request_id);

        for (request_id, role) in [
            (second.request_id, ServiceRole::Reader),
            (first.request_id, ServiceRole::Writer),
        ] {
            write_frame(
                &mut output,
                &ResponseEnvelope {
                    request_id,
                    response: Response::Status {
                        snapshot: StatusSnapshot {
                            role,
                            connections_current: 0,
                            connections_accepted_total: 0,
                            connections_rejected_total: 0,
                            connections_failed_total: 0,
                            requests_completed_total: 0,
                            error_responses_total: 0,
                        },
                    },
                },
            )
            .await?;
        }
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    });

    let link = Link::new(address, 2, 0);
    let deadline = Instant::now() + Duration::from_secs(1);
    let (first, second) = tokio::join!(
        link.exchange(&Request::Status, deadline),
        link.exchange(&Request::Status, deadline)
    );
    assert!(matches!(
        first?,
        Response::Status {
            snapshot: StatusSnapshot {
                role: ServiceRole::Writer,
                ..
            }
        }
    ));
    assert!(matches!(
        second?,
        Response::Status {
            snapshot: StatusSnapshot {
                role: ServiceRole::Reader,
                ..
            }
        }
    ));
    peer.await??;
    Ok(())
}

// Real default TCP buffers eventually backpressure a finite 128-frame queue.
// Canceling a registered frame must discard a partial binary write and release all permits.
#[tokio::test]
async fn cancel_backpressured_partial_tcp_write_retires_all_owned_work() -> Result<()> {
    use tokio::io::AsyncReadExt;

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let stream = TcpStream::connect(listener.local_addr()?).await?;
    let (mut peer, _) = listener.accept().await?;
    let connection = Arc::new(Connection::new(stream, 128));
    let permits = Arc::new(Semaphore::new(128));
    let request = Request::Put {
        key: b"k".to_vec(),
        value: vec![b'x'; 900_000],
    };
    let frame_len = encode_frame(&RequestEnvelope {
        request_id: 1,
        request: &request,
    })?
    .len();
    let mut registered = Vec::new();
    for _ in 0..128 {
        registered.push(connection.register(&request, permits.clone().acquire_owned().await?)?);
    }

    let mut prefix = [0; 64];
    peer.read_exact(&mut prefix).await?;
    assert!(u32::from_be_bytes(prefix[..4].try_into()?) as usize > prefix.len() - 4);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        connection.send.capacity() < 128,
        "finite TCP buffers must backpressure this bounded workload"
    );

    let (_, guard) = registered.pop().unwrap();
    drop(guard);
    assert_eq!(permits.available_permits(), 128);
    for (response, _guard) in registered {
        assert!(response.await?.is_err());
    }

    let mut remainder = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), peer.read_to_end(&mut remainder)).await??;
    assert!(64 + remainder.len() < 128 * frame_len);
    assert!(connection.shared.state.lock().unwrap().pending.is_empty());
    drop(connection);
    Ok(())
}
