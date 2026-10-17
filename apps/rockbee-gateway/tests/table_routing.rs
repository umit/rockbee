#![allow(clippy::unwrap_used)]

use rockbee_gateway::{config::UpstreamConfig, router::Router};
use rockbee_protocol::table::{QueryResult, TableRequest};
use rockbee_protocol::transport::{FrameReader, read_frame, write_frame};
use rockbee_protocol::{
    MAX_FRAME, MutationError, MutationOutcome, Request, RequestEnvelope, Response,
    ResponseEnvelope, Result,
};
use serde_json::json;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::{net::TcpListener, task::JoinHandle, time::timeout};

struct Backend {
    address: String,
    seen: Arc<Mutex<Vec<Request>>>,
    task: JoinHandle<()>,
}

impl Drop for Backend {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn backend(reply: Option<Response>) -> Result<Backend> {
    delayed_backend(reply, Duration::ZERO).await
}

async fn delayed_backend(reply: Option<Response>, delay: Duration) -> Result<Backend> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?.to_string();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let calls = Arc::clone(&seen);
    let task = tokio::spawn(async move {
        let mut peers = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (stream, _) = accepted.unwrap();
                    let calls = Arc::clone(&calls);
                    let reply = reply.clone();
                    peers.spawn(async move {
                        let (input, mut output) = stream.into_split();
                        let mut input = FrameReader::new(input);
                        while let Some(frame) = read_frame(&mut input).await.unwrap() {
                            let request = RequestEnvelope::decode(&frame).unwrap();
                            calls.lock().unwrap().push(request.request);
                            let Some(response) = reply.clone() else { return; };
                            tokio::time::sleep(delay).await;
                            if write_frame(&mut output, &ResponseEnvelope { request_id: request.request_id, response }).await.is_err() { return; }
                        }
                    });
                }
                _ = peers.join_next(), if !peers.is_empty() => {}
            }
        }
    });
    Ok(Backend {
        address,
        seen,
        task,
    })
}

fn get() -> Request {
    Request::Table(TableRequest::Get {
        table: "users".into(),
        id: "one".into(),
    })
}
fn query() -> Request {
    Request::Table(TableRequest::Query {
        table: "users".into(),
        sql: "SELECT * FROM users".into(),
    })
}
fn put() -> Request {
    Request::Table(TableRequest::Put {
        table: "users".into(),
        id: "one".into(),
        value: br#"{"name":"Ada"}"#.to_vec(),
        ttl_millis: 123,
    })
}
fn delete() -> Request {
    Request::Table(TableRequest::Delete {
        table: "users".into(),
        id: "one".into(),
    })
}
fn query_response() -> Response {
    Response::Query {
        result: QueryResult {
            rows: vec![json!({"name":"Ada"})],
            exported_wal_file_id: 17,
        },
    }
}

#[tokio::test]
async fn table_mutations_route_once_to_writer_and_uncertain_writes_are_not_replayed() -> Result<()>
{
    for reply in [Some(Response::Ok), None] {
        let writer = backend(reply.clone()).await?;
        let reader = backend(Some(Response::Value { value: None })).await?;
        let router = Router::new(writer.address.clone(), vec![reader.address.clone()])?;
        for request in [put(), delete()] {
            let response = timeout(Duration::from_secs(2), router.route(request)).await?;
            if reply.is_some() {
                assert_eq!(response?, Response::Ok);
            } else {
                assert_eq!(
                    MutationError::from_error(response.unwrap_err()).outcome(),
                    MutationOutcome::Unknown
                );
            }
        }
        assert_eq!(*writer.seen.lock().unwrap(), vec![put(), delete()]);
        assert!(reader.seen.lock().unwrap().is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn table_get_and_query_use_readers_round_robin_and_preserve_query_watermark() -> Result<()> {
    let writer = backend(Some(Response::Ok)).await?;
    let first = backend(Some(Response::Value { value: None })).await?;
    let second = backend(Some(query_response())).await?;
    let router = Router::new(
        writer.address.clone(),
        vec![first.address.clone(), second.address.clone()],
    )?;
    assert_eq!(router.route(get()).await?, Response::Value { value: None });
    assert_eq!(router.route(query()).await?, query_response());
    assert_eq!(*first.seen.lock().unwrap(), vec![get()]);
    assert_eq!(*second.seen.lock().unwrap(), vec![query()]);
    assert!(writer.seen.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn table_read_errors_and_wrong_response_kind_fail_over_but_none_does_not() -> Result<()> {
    for bad_reply in [
        Response::Error {
            message: "export unavailable".into(),
            mutation_outcome: MutationOutcome::Unknown,
        },
        Response::Value { value: None },
    ] {
        let writer = backend(Some(Response::Ok)).await?;
        let first = backend(Some(bad_reply)).await?;
        let second = backend(Some(query_response())).await?;
        let router = Router::new(
            writer.address.clone(),
            vec![first.address.clone(), second.address.clone()],
        )?;
        assert_eq!(router.route(query()).await?, query_response());
        assert_eq!(*first.seen.lock().unwrap(), vec![query()]);
        assert_eq!(*second.seen.lock().unwrap(), vec![query()]);
    }
    let writer = backend(Some(Response::Ok)).await?;
    let first = backend(Some(Response::Value { value: None })).await?;
    let second = backend(Some(Response::Value {
        value: Some(b"fresh".to_vec()),
    }))
    .await?;
    let router = Router::new(
        writer.address.clone(),
        vec![first.address.clone(), second.address.clone()],
    )?;
    assert_eq!(router.route(get()).await?, Response::Value { value: None });
    assert!(second.seen.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn invalid_table_requests_are_rejected_before_any_backend_effect() -> Result<()> {
    let writer = backend(Some(Response::Ok)).await?;
    let reader = backend(Some(query_response())).await?;
    let router = Router::new_with_upstream(
        writer.address.clone(),
        vec![reader.address.clone()],
        UpstreamConfig::default(),
    )?;
    let mut oversized_json = b"{\"x\":\"".to_vec();
    oversized_json.resize(MAX_FRAME + 1, b'x');
    oversized_json.extend_from_slice(b"\"}");
    let mut oversized_frame = b"{\"x\":\"".to_vec();
    oversized_frame.resize(MAX_FRAME - 100, b'x');
    oversized_frame.extend_from_slice(b"\"}");
    rockbee_protocol::table::validate_document(&oversized_frame)?;
    for operation in [
        TableRequest::Put {
            table: "bad name".into(),
            id: "one".into(),
            value: b"{}".to_vec(),
            ttl_millis: 0,
        },
        TableRequest::Put {
            table: "users".into(),
            id: "one".into(),
            value: b"[]".to_vec(),
            ttl_millis: 0,
        },
        TableRequest::Put {
            table: "users".into(),
            id: "one".into(),
            value: oversized_json,
            ttl_millis: 0,
        },
        TableRequest::Delete {
            table: "users".into(),
            id: String::new(),
        },
        TableRequest::Put {
            table: "users".into(),
            id: "i".repeat(1024),
            value: oversized_frame,
            ttl_millis: 0,
        },
    ] {
        let error = router.route(Request::Table(operation)).await.unwrap_err();
        assert_eq!(
            MutationError::from_error(error).outcome(),
            MutationOutcome::Rejected
        );
    }
    let error = router
        .route(Request::Table(TableRequest::Query {
            table: "users".into(),
            sql: String::new(),
        }))
        .await
        .unwrap_err();
    assert!(error.downcast_ref::<MutationError>().is_none());
    assert!(writer.seen.lock().unwrap().is_empty());
    assert!(reader.seen.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn table_query_timeout_fails_over_within_the_shared_request_budget() -> Result<()> {
    let writer = backend(Some(Response::Ok)).await?;
    let slow = delayed_backend(Some(query_response()), Duration::from_secs(2)).await?;
    let healthy = backend(Some(query_response())).await?;
    let config = UpstreamConfig::default()
        .with_timeouts(Duration::from_millis(500), Duration::from_millis(50))?;
    let router = Router::new_with_upstream(
        writer.address.clone(),
        vec![slow.address.clone(), healthy.address.clone()],
        config,
    )?;
    assert_eq!(
        timeout(Duration::from_secs(1), router.route(query())).await??,
        query_response()
    );
    assert_eq!(*slow.seen.lock().unwrap(), vec![query()]);
    assert_eq!(*healthy.seen.lock().unwrap(), vec![query()]);
    assert!(writer.seen.lock().unwrap().is_empty());
    Ok(())
}
