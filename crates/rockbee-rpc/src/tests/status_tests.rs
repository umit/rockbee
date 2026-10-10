use super::*;
use rockbee_protocol::transport::{FrameReader, read_frame, write_frame};
use rockbee_protocol::{
    MAX_FRAME, Request, RequestEnvelope, Response, ResponseEnvelope, ServiceRole,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::timeout;

const WAIT: Duration = Duration::from_secs(1);

async fn status(
    output: &mut tokio::net::tcp::OwnedWriteHalf,
    input: &mut FrameReader<tokio::net::tcp::OwnedReadHalf>,
) -> Result<rockbee_protocol::StatusSnapshot> {
    write_frame(
        output,
        &RequestEnvelope {
            request_id: 0,
            request: Request::Status,
        },
    )
    .await?;
    let frame = timeout(WAIT, read_frame(input))
        .await??
        .ok_or("status peer closed before responding")?;
    let response = ResponseEnvelope::decode(&frame)?;
    let Response::Status { snapshot } = response.response else {
        return Err("Status received a non-status response".into());
    };
    Ok(snapshot)
}

#[tokio::test]
async fn status_is_local_and_excluded_from_crud_completion_and_error_totals() -> Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let rpc = RpcServer::bind("127.0.0.1:0")
        .await?
        .with_role(ServiceRole::Writer);
    let address = rpc.local_addr()?;
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(rpc.run_until(
        {
            let calls = Arc::clone(&calls);
            move |request| {
                let calls = Arc::clone(&calls);
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    match request {
                        Request::Get { key } if key == b"error" => Err("handler failure".into()),
                        Request::Get { key } => Ok(Response::Value { value: Some(key) }),
                        _ => Err("only GET should reach the handler".into()),
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
    let (input, mut output) = stream.into_split();
    let mut input = FrameReader::new(input);
    let initial = status(&mut output, &mut input).await?;
    assert_eq!(initial.role, ServiceRole::Writer);
    assert_eq!(initial.connections_current, 1);
    assert_eq!(initial.connections_accepted_total, 1);
    assert_eq!(initial.requests_completed_total, 0);
    assert_eq!(initial.error_responses_total, 0);

    write_frame(
        &mut output,
        &RequestEnvelope {
            request_id: 0,
            request: Request::Get {
                key: b"ok".to_vec(),
            },
        },
    )
    .await?;
    let frame = timeout(WAIT, read_frame(&mut input))
        .await??
        .ok_or("GET peer closed before responding")?;
    assert_eq!(
        ResponseEnvelope::decode(&frame)?.response,
        Response::Value {
            value: Some(b"ok".to_vec())
        }
    );
    let after_success = status(&mut output, &mut input).await?;
    assert_eq!(after_success.requests_completed_total, 1);
    assert_eq!(after_success.error_responses_total, 0);

    write_frame(
        &mut output,
        &RequestEnvelope {
            request_id: 0,
            request: Request::Get {
                key: b"error".to_vec(),
            },
        },
    )
    .await?;
    let frame = timeout(WAIT, read_frame(&mut input))
        .await??
        .ok_or("error GET peer closed before responding")?;
    assert!(matches!(
        ResponseEnvelope::decode(&frame)?.response,
        Response::Error { .. }
    ));
    let after_error = status(&mut output, &mut input).await?;
    assert_eq!(after_error.requests_completed_total, 2);
    assert_eq!(after_error.error_responses_total, 1);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "Status reached the handler"
    );

    drop((input, output));
    stop.send(()).expect("server is running");
    timeout(WAIT, server).await???;
    Ok(())
}

#[tokio::test]
async fn status_reports_admitted_current_and_rejected_connections_without_sleeping() -> Result<()> {
    let config = RpcConfig::new(1, Duration::from_secs(5), Duration::from_secs(1))?;
    let rpc = RpcServer::bind_with_config("127.0.0.1:0", config)
        .await?
        .with_role(ServiceRole::Reader);
    let address = rpc.local_addr()?;
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(rpc.run_until(
        |_request| async { Err("Status should be intercepted before the handler".into()) },
        async {
            stopped.await?;
            Ok(())
        },
    ));

    let primary = TcpStream::connect(address).await?;
    let (input, mut output) = primary.into_split();
    let mut input = FrameReader::new(input);
    let snapshot = status(&mut output, &mut input).await?;
    assert_eq!(snapshot.role, ServiceRole::Reader);
    assert_eq!(snapshot.connections_current, 1);
    assert_eq!(snapshot.connections_accepted_total, 1);
    assert_eq!(snapshot.connections_rejected_total, 0);

    let rejected = TcpStream::connect(address).await?;
    let (rejected_input, mut rejected_output) = rejected.into_split();
    write_frame(
        &mut rejected_output,
        &RequestEnvelope {
            request_id: 0,
            request: Request::Status,
        },
    )
    .await?;
    let mut rejected_input = FrameReader::new(rejected_input);
    let closed = timeout(WAIT, read_frame(&mut rejected_input)).await?;
    assert!(
        matches!(closed, Ok(None) | Err(_)),
        "rejected connection stayed open"
    );

    let snapshot = status(&mut output, &mut input).await?;
    assert_eq!(snapshot.connections_current, 1);
    assert_eq!(snapshot.connections_accepted_total, 1);
    assert_eq!(snapshot.connections_rejected_total, 1);

    drop((input, output));
    stop.send(()).expect("server is running");
    timeout(WAIT, server).await???;
    Ok(())
}

#[tokio::test]
async fn status_reaps_failed_connections_and_decrements_current_without_sleeping() -> Result<()> {
    let rpc = RpcServer::bind("127.0.0.1:0")
        .await?
        .with_role(ServiceRole::Reader);
    let address = rpc.local_addr()?;
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(rpc.run_until(
        |_request| async { Err("Status should be intercepted before the handler".into()) },
        async {
            stopped.await?;
            Ok(())
        },
    ));

    let primary = TcpStream::connect(address).await?;
    let (input, mut output) = primary.into_split();
    let mut input = FrameReader::new(input);
    assert_eq!(
        status(&mut output, &mut input).await?.connections_current,
        1
    );

    let failed = TcpStream::connect(address).await?;
    let (_failed_input, mut failed_output) = failed.into_split();
    for _ in 0..2 {
        write_frame(
            &mut failed_output,
            &RequestEnvelope {
                request_id: 1,
                request: Request::Status,
            },
        )
        .await?;
    }
    drop(failed_output);

    let mut final_snapshot = None;
    for _ in 0..32 {
        let snapshot = status(&mut output, &mut input).await?;
        if snapshot.connections_failed_total == 1 && snapshot.connections_current == 1 {
            final_snapshot = Some(snapshot);
            break;
        }
        tokio::task::yield_now().await;
    }
    let snapshot = final_snapshot.ok_or("failed connection was not reaped within bounded polls")?;
    assert_eq!(snapshot.connections_accepted_total, 2);
    assert_eq!(snapshot.connections_rejected_total, 0);
    assert_eq!(snapshot.connections_failed_total, 1);
    assert_eq!(snapshot.connections_current, 1);

    drop((input, output));
    stop.send(()).expect("server is running");
    timeout(WAIT, server).await???;
    Ok(())
}

#[tokio::test]
async fn oversized_crud_response_counts_as_a_prepared_error_response() -> Result<()> {
    let rpc = RpcServer::bind("127.0.0.1:0")
        .await?
        .with_role(ServiceRole::Writer);
    let address = rpc.local_addr()?;
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(rpc.run_until(
        |_request| async {
            Ok(Response::Value {
                value: Some(vec![0; MAX_FRAME]),
            })
        },
        async {
            stopped.await?;
            Ok(())
        },
    ));

    let stream = TcpStream::connect(address).await?;
    let (input, mut output) = stream.into_split();
    let mut input = FrameReader::new(input);
    write_frame(
        &mut output,
        &RequestEnvelope {
            request_id: 0,
            request: Request::Get {
                key: b"oversized".to_vec(),
            },
        },
    )
    .await?;
    let frame = timeout(WAIT, read_frame(&mut input))
        .await??
        .ok_or("oversized response peer closed before fallback")?;
    assert!(matches!(
        ResponseEnvelope::decode(&frame)?.response,
        Response::Error { .. }
    ));
    let snapshot = status(&mut output, &mut input).await?;
    assert_eq!(snapshot.requests_completed_total, 1);
    assert_eq!(snapshot.error_responses_total, 1);

    drop((input, output));
    stop.send(()).expect("server is running");
    timeout(WAIT, server).await???;
    Ok(())
}

#[tokio::test]
async fn status_obeys_global_request_admission_while_a_multiplexed_request_is_held() -> Result<()> {
    let config = RpcConfig::new(2, Duration::from_secs(5), Duration::from_secs(1))?
        .with_global_max_in_flight(1)?;
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let rpc = RpcServer::bind_with_config("127.0.0.1:0", config).await?;
    let address = rpc.local_addr()?;
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
                        return Err("only held GET should reach the handler".into());
                    };
                    entered.notify_one();
                    release.notified().await;
                    Ok(Response::Value { value: Some(key) })
                }
            }
        },
        async {
            stopped.await?;
            Ok(())
        },
    ));

    let held = TcpStream::connect(address).await?;
    let (held_input, mut held_output) = held.into_split();
    let mut held_input = FrameReader::new(held_input);
    write_frame(
        &mut held_output,
        &RequestEnvelope {
            request_id: 1,
            request: Request::Get {
                key: b"held".to_vec(),
            },
        },
    )
    .await?;
    timeout(WAIT, entered.notified()).await?;

    let status_peer = TcpStream::connect(address).await?;
    let (status_input, mut status_output) = status_peer.into_split();
    write_frame(
        &mut status_output,
        &RequestEnvelope {
            request_id: 0,
            request: Request::Status,
        },
    )
    .await?;
    let mut status_input = FrameReader::new(status_input);
    let status_closed = timeout(WAIT, read_frame(&mut status_input)).await?;
    assert!(
        matches!(status_closed, Ok(None) | Err(_)),
        "overloaded Status replied"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "Status reached the handler"
    );

    release.notify_one();
    let held_frame = timeout(WAIT, read_frame(&mut held_input))
        .await??
        .ok_or("held request peer closed before responding")?;
    assert_eq!(ResponseEnvelope::decode(&held_frame)?.request_id, 1);

    drop((held_input, held_output));
    stop.send(()).expect("server is running");
    timeout(WAIT, server).await???;
    Ok(())
}
