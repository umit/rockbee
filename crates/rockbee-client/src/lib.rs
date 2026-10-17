mod lifecycle;
mod table;

pub use rockbee_protocol::table::QueryResult;
pub use table::{Table, TablePutHandle};

use lifecycle::{Lifecycle, Operation};
use rockbee_protocol::transport::{Admission, Link};
use rockbee_protocol::{
    INTERNAL_MAX_IN_FLIGHT, Request, Response, validate_put_with_ttl_bytes, validate_request_bytes,
};
pub use rockbee_protocol::{MutationError, MutationOutcome, MutationResult, Result};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{Semaphore, oneshot};
use tokio::time::Instant;

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_MAX_IN_FLIGHT: usize = 128;
const DEFAULT_MAX_PENDING: usize = 128;

/// Bounded connection and request settings for a [`RockBeeClient`].
///
/// The default preserves Rockbee's existing five-second request deadline and
/// 128 in-flight plus 128 pending requests on the client's single multiplexed TCP
/// connection. Limits are per `RockBeeClient` and shared by its clones.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientConfig {
    request_timeout: Duration,
    max_in_flight: usize,
    max_pending: usize,
}

impl ClientConfig {
    /// Create validated client settings.
    ///
    /// The request timeout covers connection establishment, admission waiting,
    /// request registration, and the response. The in-flight limit matches the
    /// protocol's per-connection maximum; the pending limit fits Tokio's
    /// semaphore capacity.
    pub fn new(
        request_timeout: Duration,
        max_in_flight: usize,
        max_pending: usize,
    ) -> Result<Self> {
        if request_timeout.is_zero() || Instant::now().checked_add(request_timeout).is_none() {
            return Err("client request timeout must be positive and representable".into());
        }
        if max_in_flight == 0 || max_in_flight > INTERNAL_MAX_IN_FLIGHT {
            return Err("client in-flight limit must be 1..=128".into());
        }
        if max_pending == 0 || max_pending > Semaphore::MAX_PERMITS {
            return Err("client pending limit must be positive and fit semaphore capacity".into());
        }
        Ok(Self {
            request_timeout,
            max_in_flight,
            max_pending,
        })
    }

    pub fn request_timeout(self) -> Duration {
        self.request_timeout
    }

    pub fn max_in_flight(self) -> usize {
        self.max_in_flight
    }

    pub fn max_pending(self) -> usize {
        self.max_pending
    }
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            max_in_flight: DEFAULT_MAX_IN_FLIGHT,
            max_pending: DEFAULT_MAX_PENDING,
        }
    }
}

/// Cloneable client for opaque bytes and JSON tables over one bounded TCP connection.
///
/// Use [`RockBeeClient::table`] to select a JSON table. Table and byte operations
/// share admission limits, mutation outcomes, and graceful shutdown.
///
/// PUT, GET and DELETE require nonempty keys; PUT also requires a nonempty value.
/// Empty inputs are rejected locally before admission or sending a request.
///
/// Applications that need synchronous calling own a Tokio runtime and call
/// `Runtime::block_on` themselves.
/// Keep the runtime used by [`RockBeeClient::connect`] running while using this client
/// or its clones, and until accepted PUT handles or [`RockBeeClient::shutdown`] complete.
/// Its stored runtime handle does not keep the runtime alive. GET and DELETE may be polled on
/// another runtime; their I/O, timers and reconnect tasks still use the connect
/// runtime. A current-thread connect runtime must be actively driven by `block_on`.
#[derive(Clone)]
pub struct RockBeeClient {
    link: Arc<Link>,
    runtime: tokio::runtime::Handle,
    lifecycle: Arc<Lifecycle>,
    config: ClientConfig,
}

/// Completion of a [`RockBeeClient::put`] call: local rejection or an admitted PUT's result.
///
/// Validation or admission rejection produces a ready error without sending a request.
/// For an admitted PUT, dropping this handle does not cancel the operation. The
/// operation keeps its admission until it completes or its task stops; a completed
/// `Ok(())` is the writer's durable acknowledgement.
#[must_use = "await this handle to observe rejection or a durable acknowledgement"]
pub struct PutHandle {
    receive: oneshot::Receiver<MutationResult>,
}

impl RockBeeClient {
    /// Stop accepting calls on this client and all its clones, drain accepted
    /// operations, and close the TCP connection. Repeated/concurrent calls are safe.
    ///
    /// The gate closes when this future is first polled. Accepted PUTs finish
    /// even if their handles were dropped or never polled. Started GET/DELETE
    /// futures must continue being polled (for example in spawned tasks or
    /// `tokio::join!`) or be dropped; calls first polled after shutdown are not sent.
    /// Existing request deadlines still apply. This does not guarantee all
    /// operations succeeded: collect their individual results, including
    /// uncertain mutation errors. No request is replayed by shutdown.
    ///
    /// Canceling this future leaves admission closed and accepted PUTs running.
    /// Call `shutdown` again to finish draining and disconnecting. Keep the
    /// connect runtime running until shutdown completes; it is not stopped here.
    pub async fn shutdown(&self) {
        tracing::debug!("client shutdown started");
        self.lifecycle.drain().await;
        self.link.disconnect().await;
        tracing::debug!("client shutdown completed");
    }

    pub async fn connect(address: &str) -> Result<Self> {
        Self::connect_with_config(address, ClientConfig::default()).await
    }

    /// Connect using explicit bounded request settings.
    ///
    /// [`RockBeeClient::connect`] uses [`ClientConfig::default`].
    pub async fn connect_with_config(address: &str, config: ClientConfig) -> Result<Self> {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| "RockBeeClient::connect requires a Tokio runtime")?;
        let deadline = deadline(config.request_timeout)?;
        let link = Link::connect(
            address.to_owned(),
            config.max_in_flight,
            config.max_pending,
            deadline,
        )
        .await?;
        Ok(Self {
            link: Arc::new(link),
            runtime,
            lifecycle: Arc::new(Lifecycle::new()),
            config,
        })
    }

    /// Start a durable PUT immediately and return its eventual completion.
    ///
    /// Calls share the configured in-flight and pending Link admissions with GET and
    /// DELETE. The returned handle may be awaited later; dropping it does not
    /// cancel the registered request or affect other requests.
    /// A timeout after registration leaves the mutation's outcome unknown:
    /// the write may still become durable after the handle returns an error.
    /// A server-side storage error can also follow a persisted write. An error
    /// is not a rollback acknowledgement; mutations are not automatically retried.
    /// `None` and `Some(Duration::ZERO)` store a value without expiry. Positive
    /// sub-millisecond durations round up to one millisecond.
    pub fn put(&self, key: &[u8], value: &[u8], ttl: Option<Duration>) -> PutHandle {
        if key.is_empty() {
            return rejected_put("key must not be empty".into());
        }
        if value.is_empty() {
            return rejected_put("value must not be empty".into());
        }
        let ttl_millis = match ttl {
            None => None,
            Some(ttl) => match duration_to_millis(ttl) {
                Ok(0) => None,
                Ok(ttl_millis) => Some(ttl_millis),
                Err(error) => return rejected_put(error),
            },
        };
        self.submit_put(key, value, ttl_millis)
    }

    fn submit_put(&self, key: &[u8], value: &[u8], ttl_millis: Option<u64>) -> PutHandle {
        let validation = match ttl_millis {
            Some(ttl_millis) => validate_put_with_ttl_bytes(key, value, ttl_millis),
            None => validate_request_bytes(key, Some(value)),
        };
        if let Err(error) = validation {
            return rejected_put(error);
        }
        self.submit_put_request(match ttl_millis {
            Some(ttl_millis) => Request::PutWithTtl {
                key: key.to_vec(),
                value: value.to_vec(),
                ttl_millis,
            },
            None => Request::Put {
                key: key.to_vec(),
                value: value.to_vec(),
            },
        })
    }

    // Callers validate and serialize before this shared eager admission path.
    fn submit_put_request(&self, request: Request) -> PutHandle {
        let deadline = match deadline(self.config.request_timeout) {
            Ok(deadline) => deadline,
            Err(error) => return rejected_put(error),
        };
        let (respond, receive) = oneshot::channel();
        let submission = (|| -> Result<(Admission, Operation)> {
            let operation = self.lifecycle.start()?;
            let admission = self.link.try_admit()?;
            Ok((admission, operation))
        })();
        let (admission, operation) = match submission {
            Ok(submission) => submission,
            Err(error) => {
                let _ = respond.send(Err(MutationError::new(MutationOutcome::NotSent, error)));
                return PutHandle { receive };
            }
        };
        let link = Arc::clone(&self.link);
        drop(self.runtime.spawn(async move {
            let _operation = operation;
            let result = link
                .exchange_admitted(&request, deadline, admission)
                .await
                .map_err(MutationError::from_error)
                .and_then(mutation_response);
            let _ = respond.send(result);
        }));
        PutHandle { receive }
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if key.is_empty() {
            return Err("key must not be empty".into());
        }
        validate_request_bytes(key, None)?;
        let _operation = self.lifecycle.start()?;
        match self.exchange(Request::Get { key: key.to_vec() }).await? {
            Response::Value { value } => Ok(value),
            Response::Error { message, .. } => Err(message.into()),
            _ => Err("unexpected response to get".into()),
        }
    }

    /// Delete a key and wait for its durable acknowledgement.
    ///
    /// A timeout or server-side storage error can leave the deletion's outcome
    /// unknown. An error does not guarantee the value remains present, and the
    /// client does not automatically retry the mutation.
    pub async fn delete(&self, key: &[u8]) -> MutationResult {
        if key.is_empty() {
            return Err(MutationError::new(
                MutationOutcome::NotSent,
                "key must not be empty",
            ));
        }
        validate_request_bytes(key, None)
            .map_err(|error| MutationError::new(MutationOutcome::NotSent, error))?;
        let _operation = self
            .lifecycle
            .start()
            .map_err(|error| MutationError::new(MutationOutcome::NotSent, error))?;
        let response = self
            .exchange(Request::Delete { key: key.to_vec() })
            .await
            .map_err(MutationError::from_error)?;
        mutation_response(response)
    }

    async fn exchange(&self, request: Request) -> Result<Response> {
        let request_timeout = self.config.request_timeout;
        let mut exchange = std::pin::pin!(async {
            let deadline = deadline(request_timeout).map_err(|error| {
                if request.is_mutation() {
                    MutationError::new(MutationOutcome::NotSent, error).into()
                } else {
                    error
                }
            })?;
            self.link.exchange(&request, deadline).await
        });
        std::future::poll_fn(|cx| {
            // Reconnect I/O belongs to the same runtime as eager PUTs. Enter
            // only for this poll; never keep the context guard across Pending.
            let _entered = self.runtime.enter();
            exchange.as_mut().poll(cx)
        })
        .await
    }
}

fn deadline(timeout: Duration) -> Result<Instant> {
    Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| "client request timeout is no longer representable".into())
}

fn rejected_put(error: Box<dyn std::error::Error + Send + Sync>) -> PutHandle {
    let (respond, receive) = oneshot::channel();
    let _ = respond.send(Err(MutationError::new(MutationOutcome::NotSent, error)));
    PutHandle { receive }
}

fn duration_to_millis(ttl: Duration) -> Result<u64> {
    let seconds = ttl
        .as_secs()
        .checked_mul(1_000)
        .ok_or("TTL exceeds signed millisecond representation")?;
    let nanos = u64::from(ttl.subsec_nanos());
    let fractional_millis = nanos.div_ceil(1_000_000);
    let millis = seconds
        .checked_add(fractional_millis)
        .ok_or("TTL exceeds signed millisecond representation")?;
    if millis > i64::MAX as u64 {
        return Err("TTL exceeds signed millisecond representation".into());
    }
    Ok(millis)
}

impl Future for PutHandle {
    type Output = MutationResult;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.get_mut().receive).poll(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            Poll::Ready(Err(_)) => Poll::Ready(Err(MutationError::new(
                MutationOutcome::Unknown,
                "PUT task stopped before replying",
            ))),
            Poll::Pending => Poll::Pending,
        }
    }
}

fn mutation_response(response: Response) -> MutationResult {
    match response {
        Response::Ok => Ok(()),
        Response::Error {
            message,
            mutation_outcome,
        } => Err(MutationError::new(
            // A remote error can never establish that the public client did
            // not send its request. Decode also enforces this hop boundary.
            match mutation_outcome {
                MutationOutcome::NotSent | MutationOutcome::Rejected => MutationOutcome::Rejected,
                MutationOutcome::Unknown => MutationOutcome::Unknown,
            },
            message,
        )),
        _ => Err(MutationError::new(
            MutationOutcome::Unknown,
            "unexpected response to mutation",
        )),
    }
}
