//! One lazy, bounded multiplexed link per backend.
//! Transport/protocol failures and registered request cancellation retire a generation;
//! local validation/admission errors and valid application error responses do not.
use crate::transport::{FrameReader, read_frame};
use crate::{ExpectedResponse, MutationError, MutationOutcome, Result};
use crate::{Request, RequestEnvelope, Response, ResponseEnvelope, encode_frame};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout_at};

pub struct Link {
    address: String,
    active: Arc<Semaphore>,
    waiting: Arc<Semaphore>,
    admission_owner: Arc<()>,
    connection: tokio::sync::Mutex<Option<Arc<Connection>>>,
    capacity: usize,
}

/// A synchronously reserved slot in a link's shared active/waiting budget.
pub struct Admission {
    owner: Arc<()>,
    kind: AdmissionKind,
}

enum AdmissionKind {
    Active(OwnedSemaphorePermit),
    Waiting(OwnedSemaphorePermit),
}

impl Link {
    pub fn new(address: String, max_in_flight: usize, max_pending: usize) -> Self {
        Self {
            address,
            active: Arc::new(Semaphore::new(max_in_flight)),
            waiting: Arc::new(Semaphore::new(max_pending)),
            admission_owner: Arc::new(()),
            connection: tokio::sync::Mutex::new(None),
            capacity: max_in_flight,
        }
    }

    pub async fn connect(
        address: String,
        max_in_flight: usize,
        max_pending: usize,
        deadline: Instant,
    ) -> Result<Self> {
        let link = Self::new(address, max_in_flight, max_pending);
        link.connection(deadline).await?;
        Ok(link)
    }

    /// Close the current connection and wait for both I/O tasks to stop.
    /// Callers must stop admission and drain exchanges first for graceful
    /// shutdown. A later exchange can reconnect; this is not an admission gate.
    pub async fn disconnect(&self) {
        let mut slot = self.connection.lock().await;
        if let Some(connection) = slot.as_ref() {
            connection.stop().await;
        }
        *slot = None;
    }

    async fn connection(&self, deadline: Instant) -> Result<Arc<Connection>> {
        // Admission and connect waiting share the caller's operation budget.
        let mut slot = timeout_at(deadline, self.connection.lock()).await?;
        if let Some(link) = slot.as_ref().filter(|link| !link.is_closed()) {
            return Ok(link.clone());
        }
        let stream = timeout_at(deadline, TcpStream::connect(&self.address)).await??;
        let link = Arc::new(Connection::new(stream, self.capacity));
        *slot = Some(link.clone());
        Ok(link)
    }

    /// Reserve capacity without waiting, so a caller can admit work before it
    /// returns an awaitable completion handle.
    pub fn try_admit(&self) -> Result<Admission> {
        match self.active.clone().try_acquire_owned() {
            Ok(permit) => Ok(Admission {
                owner: Arc::clone(&self.admission_owner),
                kind: AdmissionKind::Active(permit),
            }),
            Err(_) => self
                .waiting
                .clone()
                .try_acquire_owned()
                .map(|permit| Admission {
                    owner: Arc::clone(&self.admission_owner),
                    kind: AdmissionKind::Waiting(permit),
                })
                .map_err(|_| "upstream pending limit reached".into()),
        }
    }

    pub async fn exchange(&self, request: &Request, deadline: Instant) -> Result<Response> {
        let admission = self
            .try_admit()
            .map_err(|error| mutation_failure(request, MutationOutcome::NotSent, error))?;
        self.exchange_admitted(request, deadline, admission).await
    }

    pub async fn exchange_admitted(
        &self,
        request: &Request,
        deadline: Instant,
        admission: Admission,
    ) -> Result<Response> {
        // A waiting reservation remains owned until it converts to an active
        // permit or this operation ends, so active + waiting stays bounded.
        let registration = async {
            if !Arc::ptr_eq(&self.admission_owner, &admission.owner) {
                return Err("admission belongs to a different link".into());
            }
            let permit = match admission.kind {
                AdmissionKind::Active(permit) => permit,
                AdmissionKind::Waiting(waiting) => {
                    let permit =
                        timeout_at(deadline, self.active.clone().acquire_owned()).await??;
                    drop(waiting);
                    permit
                }
            };
            let connection = self.connection(deadline).await?;
            if Instant::now() >= deadline {
                return Err("upstream deadline expired before registration".into());
            }
            connection.register(request, permit)
        }
        .await;
        // A failed registration did not hand this frame to the socket writer.
        let (receive, mut guard) = registration
            .map_err(|error| mutation_failure(request, MutationOutcome::NotSent, error))?;
        let result = timeout_at(deadline, receive).await;
        match result {
            Ok(Ok(result)) => {
                guard.armed = false;
                result.map_err(|error| {
                    mutation_failure(request, MutationOutcome::Unknown, error.into())
                })
            }
            Ok(Err(_)) => Err(mutation_failure(
                request,
                MutationOutcome::Unknown,
                "upstream retired before replying".into(),
            )),
            Err(_) => Err(mutation_failure(
                request,
                MutationOutcome::Unknown,
                "upstream request timed out".into(),
            )),
        }
    }
}

fn mutation_failure(
    request: &Request,
    outcome: MutationOutcome,
    error: Box<dyn std::error::Error + Send + Sync>,
) -> Box<dyn std::error::Error + Send + Sync> {
    match request {
        request if request.is_mutation() => MutationError::new(outcome, error).into(),
        _ => error,
    }
}

fn expected_response(request: &Request) -> ExpectedResponse {
    match request {
        Request::Get { .. } => ExpectedResponse::Value,
        Request::Status => ExpectedResponse::Status,
        request if request.is_mutation() => ExpectedResponse::Mutation,
        _ => unreachable!("all request variants are classified"),
    }
}

struct Pending {
    expected: ExpectedResponse,
    respond: oneshot::Sender<std::result::Result<Response, String>>,
    _permit: OwnedSemaphorePermit,
}
struct State {
    closed: bool,
    last_id: u64,
    pending: HashMap<u64, Pending>,
}
struct Shared {
    // Poisoning means an internal state update panicked; continuing could use
    // inconsistent pending requests. Treat it as a violated invariant.
    state: Mutex<State>,
    retired: watch::Sender<bool>,
}
impl Shared {
    fn retire(&self, reason: &str) {
        let mut state = self
            .state
            .lock()
            .expect("upstream state mutex must not be poisoned");
        if state.closed {
            return;
        }
        state.closed = true;
        for (_, pending) in state.pending.drain() {
            let _ = pending.respond.send(Err(reason.to_owned()));
        }
        self.retired.send_replace(true);
    }
}
struct Connection {
    shared: Arc<Shared>,
    send: mpsc::Sender<Vec<u8>>,
    tasks: tokio::sync::Mutex<[Option<JoinHandle<()>>; 2]>,
}
impl Connection {
    fn new(stream: TcpStream, capacity: usize) -> Self {
        let (input, mut output) = stream.into_split();
        let (retired, mut read_stop) = watch::channel(false);
        let mut write_stop = read_stop.clone();
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                closed: false,
                last_id: 0,
                pending: HashMap::new(),
            }),
            retired,
        });
        let (send, mut frames) = mpsc::channel::<Vec<u8>>(capacity);
        let writer_state = shared.clone();
        let writer = tokio::spawn(async move {
            let write = async {
                while let Some(frame) = frames.recv().await {
                    output.write_all(&frame).await?;
                    output.flush().await?;
                }
                Ok::<_, std::io::Error>(())
            };
            tokio::select! {
                biased;
                _ = write_stop.changed() => {},
                result = write => {
                    writer_state.retire(&format!("upstream writer stopped: {result:?}"));
                }
            }
            // A canceled partial write is discarded with this socket, never resumed elsewhere.
        });
        let reader_state = shared.clone();
        let reader = tokio::spawn(async move {
            let read = async {
                let mut input = FrameReader::new(input);
                loop {
                    let frame = read_frame(&mut input).await?.ok_or("upstream EOF")?;
                    let envelope = ResponseEnvelope::decode(&frame)?;
                    let mut state = reader_state
                        .state
                        .lock()
                        .expect("upstream state mutex must not be poisoned");
                    if state.closed {
                        return Ok(());
                    }
                    let pending = state
                        .pending
                        .get(&envelope.request_id)
                        .ok_or("unknown or duplicate upstream ID")?;
                    let id = envelope.request_id;
                    let response = envelope.validate(pending.expected)?;
                    let pending = state
                        .pending
                        .remove(&id)
                        .expect("validated request must remain pending while holding state lock");
                    let _ = pending.respond.send(Ok(response));
                }
            };
            tokio::select! {
                biased;
                _ = read_stop.changed() => {},
                result = read => {
                    let result: Result<()> = result;
                    reader_state.retire(&format!("upstream reader stopped: {result:?}"));
                }
            }
            // read_frame is canceled only here, when both socket halves are being retired.
        });
        Self {
            shared,
            send,
            tasks: tokio::sync::Mutex::new([Some(reader), Some(writer)]),
        }
    }

    async fn stop(&self) {
        self.shared.retire("upstream disconnected");
        let mut tasks = self.tasks.lock().await;
        for task in tasks.iter().flatten() {
            task.abort();
        }
        for task in tasks.iter_mut() {
            if let Some(running) = task.as_mut() {
                let _ = running.await;
            }
            // Retain an unfinished handle if stop is canceled; a later call
            // can finish joining it without polling a completed handle twice.
            *task = None;
        }
    }

    fn is_closed(&self) -> bool {
        self.shared
            .state
            .lock()
            .expect("upstream state mutex must not be poisoned")
            .closed
    }

    fn register(
        self: &Arc<Self>,
        request: &Request,
        permit: OwnedSemaphorePermit,
    ) -> Result<(
        oneshot::Receiver<std::result::Result<Response, String>>,
        Registered,
    )> {
        let mut state = self
            .shared
            .state
            .lock()
            .expect("upstream state mutex must not be poisoned");
        if state.closed {
            return Err("upstream link already retired".into());
        }
        let Some(id) = state.last_id.checked_add(1) else {
            drop(state);
            self.shared.retire("upstream IDs exhausted");
            return Err("upstream IDs exhausted".into());
        };
        let frame = encode_frame(&RequestEnvelope {
            request_id: id,
            request,
        })?;
        let (respond, receive) = oneshot::channel();
        state.last_id = id;
        state.pending.insert(
            id,
            Pending {
                expected: expected_response(request),
                respond,
                _permit: permit,
            },
        );
        // ID allocation, registration and enqueue share one short lock: wire IDs cannot reorder.
        if self.send.try_send(frame).is_err() {
            drop(state);
            self.shared.retire("upstream frame queue closed or full");
            return Err("upstream frame queue closed or full".into());
        }
        Ok((
            receive,
            Registered {
                connection: self.clone(),
                armed: true,
            },
        ))
    }
}
impl Drop for Connection {
    fn drop(&mut self) {
        self.shared.retire("upstream owner dropped");
        for task in self.tasks.get_mut().iter().flatten() {
            task.abort();
        }
    }
}
struct Registered {
    connection: Arc<Connection>,
    armed: bool,
}
impl Drop for Registered {
    fn drop(&mut self) {
        if self.armed {
            self.connection
                .shared
                .retire("registered request canceled or expired");
        }
    }
}

#[cfg(test)]
mod tests;
