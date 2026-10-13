use super::config::{UpstreamConfig, validate_topology};
use super::upstream::Upstream;
use rockbee_protocol::{MutationError, MutationOutcome, Result};
use rockbee_protocol::{Request, Response, validate_put_with_ttl_bytes, validate_request_bytes};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::time::{Instant, timeout_at};

pub struct Router {
    writer: Upstream,
    readers: Vec<Upstream>,
    next_reader: AtomicUsize,
    request_timeout: Duration,
    reader_attempt_timeout: Duration,
}

impl Router {
    pub fn new(writer: String, readers: Vec<String>) -> Result<Self> {
        Self::new_with_upstream(writer, readers, UpstreamConfig::default())
    }

    pub fn new_with_upstream(
        writer: String,
        readers: Vec<String>,
        upstream: UpstreamConfig,
    ) -> Result<Self> {
        validate_topology(&writer, &readers)?;
        let request_timeout = upstream.request_timeout();
        let reader_attempt_timeout = upstream.reader_attempt_timeout();
        Ok(Self {
            writer: Upstream::new(writer, upstream),
            readers: readers
                .into_iter()
                .map(|reader| Upstream::new(reader, upstream))
                .collect(),
            next_reader: AtomicUsize::new(0),
            request_timeout,
            reader_attempt_timeout,
        })
    }

    pub async fn route(&self, request: Request) -> Result<Response> {
        // Local rejection is known unsent; validate before upstream effects.
        let validation = match &request {
            Request::Put { key, value } => validate_request_bytes(key, Some(value)),
            Request::PutWithTtl {
                key,
                value,
                ttl_millis,
            } => validate_put_with_ttl_bytes(key, value, *ttl_millis),
            Request::Get { key } | Request::Delete { key } => validate_request_bytes(key, None),
            Request::Status => return Err("Status must be handled locally by RPC".into()),
        };
        validation.map_err(|error| -> Box<dyn std::error::Error + Send + Sync> {
            if matches!(request, Request::Get { .. }) {
                error
            } else {
                MutationError::new(MutationOutcome::Rejected, error).into()
            }
        })?;
        let key = match request {
            Request::Get { key } => key,
            mutation @ (Request::Put { .. }
            | Request::PutWithTtl { .. }
            | Request::Delete { .. }) => {
                // Never retry a mutation: an absent reply does not prove it failed.
                return self
                    .writer
                    .exchange(&mutation, Instant::now() + self.request_timeout)
                    .await;
            }
            Request::Status => return Err("Status must be handled locally by RPC".into()),
        };
        let start = self.next_reader.fetch_add(1, Ordering::Relaxed) % self.readers.len();
        let deadline = Instant::now() + self.request_timeout;
        let mut first_reader_error = None;
        for offset in 0..self.readers.len() {
            if Instant::now() >= deadline {
                break;
            }
            let reader = &self.readers[(start + offset) % self.readers.len()];
            let result = if offset + 1 == self.readers.len() {
                reader
                    .exchange(&Request::Get { key: key.clone() }, deadline)
                    .await
            } else {
                let Ok(mut get) = reader.start_get(&key, deadline) else {
                    continue;
                };
                let result = timeout_at(
                    deadline.min(Instant::now() + self.reader_attempt_timeout),
                    &mut get,
                )
                .await;
                drop(get);
                match result {
                    Ok(Ok(result)) => result,
                    Ok(Err(_)) | Err(_) => continue,
                }
            };
            match result {
                Ok(Response::Value { value }) => {
                    // Missing is a successful read, even when another reader may lag.
                    return Ok(Response::Value { value });
                }
                Ok(response @ Response::Error { .. }) => {
                    // A reader can fail while its Link remains usable. Only GET may fail over.
                    first_reader_error.get_or_insert(response);
                }
                _ => {}
            }
        }
        first_reader_error
            .ok_or_else(|| "no reader returned a valid response within the request budget".into())
    }
}
