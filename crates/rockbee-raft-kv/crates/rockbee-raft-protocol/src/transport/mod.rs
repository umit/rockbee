//! One bounded TCP connection generation. No reconnection or replay.
//!
//! Submission is synchronous and eager. Dropping a response future discards
//! observation, without cancelling accepted work. A handed request's timeout
//! retires the connection: other handed mutations can also become uncertain.
//! Queued work proven never handed fails as `NotSent`.
//!
//! Three owned workers perform writing, reading and deadline coordination.
//! Request reservations survive queued expiration until the queue item is
//! destroyed. Receive/result reservations survive until a result is consumed or
//! dropped. Explicit `close` retires the generation and joins all three workers.

mod admission;
mod config;
mod pending;
pub mod server;
mod state;
mod workers;

use std::{marker::PhantomData, net::SocketAddr, sync::Arc};

use serde::de::DeserializeOwned;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    sync::{Mutex, mpsc},
    task::JoinHandle,
    time::{Instant, timeout_at},
};

use crate::{RequestId, WireRequest};

pub use config::{
    AdmitError, ConfigError, ConnectError, Delivery, LinkConfig, TransportFailure,
    TransportFailureKind,
};
pub use pending::PendingResponse;
use state::{Entry, Phase, Shared};

/// A single immutable connection generation. Clone shares its admission limits.
pub struct Link<Req, Resp>
where
    Req: WireRequest<Resp>,
{
    inner: Arc<Inner<Req, Resp>>,
}

struct Inner<Req: WireRequest<Resp>, Resp> {
    shared: Arc<Shared<Resp, Req::Expected>>,
    queue: mpsc::Sender<Queued<Req>>,
    workers: Mutex<Vec<JoinHandle<()>>>,
    _response: PhantomData<fn() -> Resp>,
}

struct Queued<Req> {
    id: RequestId,
    deadline: Instant,
    request: Req,
    frame_payload_limit: usize,
    _count: Arc<pending::Admission>,
    _bytes: tokio::sync::OwnedSemaphorePermit,
}

impl<Req: WireRequest<Resp>, Resp> Clone for Link<Req, Resp> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<Req, Resp> Link<Req, Resp>
where
    Req: WireRequest<Resp>,
    Resp: DeserializeOwned + Send + 'static,
{
    /// Connects exactly once, spending the caller's absolute connect deadline.
    ///
    /// # Errors
    /// Rejects impossible limits, connection I/O failures and elapsed deadlines.
    pub async fn connect(
        address: SocketAddr,
        config: LinkConfig,
        connect_deadline: Instant,
    ) -> Result<Self, ConnectError> {
        log::info!("raft transport connecting remote={address}");
        config.validate::<Req, Resp>()?;
        if connect_deadline <= Instant::now() {
            log::debug!("raft transport connect deadline elapsed remote={address}");
            return Err(ConnectError::Deadline);
        }
        let stream = match timeout_at(connect_deadline, TcpStream::connect(address)).await {
            Ok(Ok(stream)) => stream,
            Ok(Err(error)) => {
                log::debug!(
                    "raft transport connection failed remote={address} kind={:?}",
                    error.kind()
                );
                return Err(error.into());
            }
            Err(_) => {
                log::debug!("raft transport connection timed out remote={address}");
                return Err(ConnectError::Deadline);
            }
        };
        if connect_deadline <= Instant::now() {
            log::debug!(
                "raft transport connection deadline elapsed after connect remote={address}"
            );
            return Err(ConnectError::Deadline);
        }
        log::info!("raft transport connected remote={address}");
        Ok(Self::start(stream, config))
    }

    fn start<I>(stream: I, config: LinkConfig) -> Self
    where
        I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let shared = Arc::new(Shared::new(config));
        let (send, receive) = mpsc::channel(config.max_outstanding);
        let (reader, writer) = tokio::io::split(stream);
        let workers = vec![
            tokio::spawn(workers::write_requests::<Req, Resp, _>(
                writer,
                receive,
                Arc::clone(&shared),
            )),
            tokio::spawn(workers::read_responses::<Req, Resp, _>(
                reader,
                Arc::clone(&shared),
            )),
            tokio::spawn(workers::coordinate_deadlines(Arc::clone(&shared))),
        ];
        Self {
            inner: Arc::new(Inner {
                shared,
                queue: send,
                workers: Mutex::new(workers),
                _response: PhantomData,
            }),
        }
    }

    /// Validates and admits immediately; polling the result never starts work.
    /// An error is always local and unsent. No worker/task is created per call.
    ///
    /// # Errors
    /// Returns invalid-body, elapsed-deadline, capacity, closed-generation or ID
    /// exhaustion errors. None of these errors leaves an accepted request behind.
    pub fn try_submit(
        &self,
        request: Req,
        deadline: Instant,
    ) -> Result<PendingResponse<Resp>, AdmitError> {
        if let Err(error) = request.validate() {
            log::debug!("raft transport rejected invalid request before admission");
            return Err(AdmitError::InvalidRequest(error));
        }
        if deadline <= Instant::now() {
            log::debug!("raft transport rejected elapsed request deadline before admission");
            return Err(AdmitError::Deadline);
        }
        let shared = &self.inner.shared;
        let admission = match admission::reserve_request(shared, &request) {
            Ok(admission) => admission,
            Err(error) => {
                log::debug!("raft transport admission reservation failed kind={error:?}");
                return Err(error);
            }
        };
        let expected = request.expected_response();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let frame_payload_limit = admission.frame_payload_limit;
        let admitted = {
            let mut state = shared.lock();
            if state.retired.is_some() {
                Err(AdmitError::Closed)
            } else if deadline <= Instant::now() {
                Err(AdmitError::Deadline)
            } else if let Some(raw_id) = state.next_id {
                state.next_id = raw_id.checked_add(1);
                let id = RequestId::new(raw_id).expect("checked nonzero monotonic ID");
                let item = Queued {
                    id,
                    deadline,
                    request,
                    frame_payload_limit,
                    _count: Arc::clone(&admission.lifetime),
                    _bytes: admission.request_bytes,
                };
                // Count admission means at most max_outstanding retained queue entries.
                // Hold state across try_send so a worker cannot hand off before insertion.
                if self.inner.queue.try_send(item).is_err() {
                    Err(AdmitError::Closed)
                } else {
                    state.entries.insert(
                        id,
                        Entry {
                            deadline,
                            expected,
                            phase: Phase::Queued,
                            sender,
                            count: admission.lifetime,
                        },
                    );
                    state.deadlines.insert((deadline, id));
                    Ok(id)
                }
            } else {
                shared.retire_locked(&mut state, TransportFailureKind::IdExhausted);
                Err(AdmitError::IdExhausted)
            }
        };
        let id = match admitted {
            Ok(id) => id,
            Err(error) => {
                log::debug!("raft transport admission failed kind={error:?}");
                return Err(error);
            }
        };
        shared.deadline_changed.notify_one();
        log::trace!(
            "raft transport request queued request_id={} frame_payload_limit={frame_payload_limit}",
            id.get()
        );
        Ok(PendingResponse::new(receiver))
    }

    /// Retires this generation and joins every owned worker. Idempotent.
    pub async fn close(&self) {
        log::info!("raft transport closing connection generation");
        self.inner.shared.retire(TransportFailureKind::Closed);
        let mut workers = self.inner.workers.lock().await;
        // Keep each handle in the owned vector while awaiting it so cancelling
        // close does not detach unjoined workers from a later close call.
        while let Some(worker) = workers.last_mut() {
            let _ = worker.await;
            workers.pop();
        }
        log::info!("raft transport connection generation closed");
    }
}

impl<Req: WireRequest<Resp>, Resp> Drop for Inner<Req, Resp> {
    fn drop(&mut self) {
        self.shared.retire(TransportFailureKind::Closed);
    }
}

#[cfg(test)]
mod tests;
