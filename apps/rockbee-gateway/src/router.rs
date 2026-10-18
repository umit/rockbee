use super::config::{UpstreamConfig, validate_topology};
use super::upstream::Upstream;
use rockbee_protocol::{MutationError, MutationOutcome, Result};
use rockbee_protocol::{
    Request, RequestEnvelope, Response, WireMessage, validate_put_with_ttl_bytes,
    validate_request_bytes,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::time::{Instant, timeout_at};

/// Validates and routes requests through bounded upstream connections.
/// Mutations use the writer once; reads may try other readers within one deadline.
pub struct Router {
    writer: Upstream,
    readers: Vec<Upstream>,
    next_reader: AtomicUsize,
    request_timeout: Duration,
    reader_attempt_timeout: Duration,
}

impl Router {
    /// Creates routing state with default upstream limits; connections open on demand.
    pub fn new(writer: String, readers: Vec<String>) -> Result<Self> {
        Self::new_with_upstream(writer, readers, UpstreamConfig::default())
    }

    /// Creates routing state after validating the writer and nonempty reader topology.
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

    /// Validates the request before any upstream effect, then dispatches by operation.
    ///
    /// A missing value is a successful reader response, even if that reader lags.
    /// A failed mutation is never retried because its effect may be uncertain.
    /// Status requests are handled by RPC before reaching this method.
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
            Request::Table(_) => RequestEnvelope {
                request_id: u64::MAX,
                request: &request,
            }
            .validate(),
            Request::Status => return Err("Status must be handled locally by RPC".into()),
        };
        validation.map_err(|error| -> Box<dyn std::error::Error + Send + Sync> {
            if request.is_mutation() {
                MutationError::new(MutationOutcome::Rejected, error).into()
            } else {
                error
            }
        })?;
        if request.is_mutation() {
            // Never retry a mutation: an absent reply does not prove it failed.
            return self
                .writer
                .exchange(&request, Instant::now() + self.request_timeout)
                .await;
        }
        let start = self.next_reader.fetch_add(1, Ordering::Relaxed) % self.readers.len();
        let deadline = Instant::now() + self.request_timeout;
        let mut first_reader_error = None;
        for offset in 0..self.readers.len() {
            if Instant::now() >= deadline {
                break;
            }
            let reader = &self.readers[(start + offset) % self.readers.len()];
            let result = if offset + 1 == self.readers.len() {
                reader.exchange(&request, deadline).await
            } else {
                let Ok(mut read) = reader.start_read(&request, deadline) else {
                    continue;
                };
                let result = timeout_at(
                    deadline.min(Instant::now() + self.reader_attempt_timeout),
                    &mut read,
                )
                .await;
                drop(read);
                match result {
                    Ok(Ok(result)) => result,
                    Ok(Err(_)) | Err(_) => continue,
                }
            };
            match result {
                Ok(response @ (Response::Value { .. } | Response::Query { .. })) => {
                    // Missing is a successful read, even when another reader may lag.
                    return Ok(response);
                }
                Ok(response @ Response::Error { .. }) => {
                    // A reader can fail while its Link remains usable. Only reads may fail over.
                    first_reader_error.get_or_insert(response);
                }
                _ => {}
            }
        }
        first_reader_error
            .ok_or_else(|| "no reader returned a valid response within the request budget".into())
    }
}
