use std::{collections::BTreeSet, future::Future, sync::Arc, time::Duration};

use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        TcpListener, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc, watch},
    task::{JoinHandle, JoinSet},
    time::{Instant, timeout},
};

use super::{ServerConfig, ServerConfigError};
use crate::{
    MAX_FRAME_PAYLOAD, RequestEnvelope, RequestId, ResponseEnvelope, VERSION, WireRequest,
    decode_frame, encode_frame,
};

/// Application code called after a request is fully decoded and admitted.
///
/// `deadline` is the absolute deadline derived from the received envelope's
/// remaining budget. The server does not cancel an accepted handler at that
/// deadline: a Raft proposal may still be resolving and remains charged until
/// the handler actually returns. A handler should use the deadline to choose a
/// response, while retaining its own durable-work semantics.
pub trait RequestHandler: Send + Sync + 'static {
    /// Concrete decoded request body accepted by this server.
    type Request: WireRequest<Self::Response> + DeserializeOwned;
    /// Concrete response body returned to the caller.
    type Response: Serialize + Send + 'static;

    /// Starts application work for one validated request.
    fn handle(
        &self,
        request: Self::Request,
        deadline: Instant,
    ) -> impl Future<Output = Self::Response> + Send;
}

/// Owns one listener's accept task and all connection tasks it creates.
///
/// Dropping this value stops future acceptance. Call [`Server::close`] during
/// orderly shutdown to join the accept tree. An arbitrary application handler
/// may choose never to finish; in that case `close` waits rather than claiming
/// that accepted work has disappeared.
pub struct Server {
    stop: watch::Sender<bool>,
    accept: Mutex<Option<JoinHandle<()>>>,
}

impl Server {
    /// Starts bounded acceptance on an already-bound listener.
    ///
    /// Binding is deliberately outside this method so tests and applications
    /// can choose their own address and OS-level socket setup.
    ///
    /// # Errors
    /// Returns before starting any task when configuration is impossible.
    pub fn start<H: RequestHandler>(
        listener: TcpListener,
        config: ServerConfig,
        handler: Arc<H>,
    ) -> Result<Self, ServerError> {
        let local = listener.local_addr().ok();
        if let Err(error) = config.validate_for::<H::Request, H::Response>() {
            log::debug!("raft transport server rejected invalid configuration");
            return Err(error.into());
        }
        log::info!("raft transport server listening local={local:?}");
        let (stop, receiver) = watch::channel(false);
        let limits = Arc::new(Limits::new(config));
        let accept = tokio::spawn(accept_connections(
            listener, receiver, config, limits, handler,
        ));
        Ok(Self {
            stop,
            accept: Mutex::new(Some(accept)),
        })
    }

    /// Stops accepting new sockets. Existing accepted work is retained.
    pub fn stop_accepting(&self) {
        log::info!("raft transport server stopping acceptance");
        self.stop.send_replace(true);
    }

    /// Stops acceptance and joins the owned accept/connection/handler tree.
    ///
    /// The join handle remains stored while waiting, so cancellation of this
    /// future does not detach the server. Calling `close` again resumes joining.
    pub async fn close(&mut self) {
        self.stop_accepting();
        let mut accept = self.accept.lock().await;
        if let Some(task) = accept.as_mut() {
            let _ = task.await;
        }
        accept.take();
        log::info!("raft transport server closed");
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}

/// Server startup can fail only before application work is accepted.
#[derive(Debug, Error)]
pub enum ServerError {
    /// Configuration did not reserve space for even one legal frame.
    #[error(transparent)]
    Config(#[from] ServerConfigError),
}

struct Limits {
    connections: Arc<Semaphore>,
    operations: Arc<Semaphore>,
    request_bytes: Arc<Semaphore>,
    response_bytes: Arc<Semaphore>,
}

impl Limits {
    fn new(config: ServerConfig) -> Self {
        Self {
            connections: Arc::new(Semaphore::new(config.max_connections)),
            operations: Arc::new(Semaphore::new(config.max_outstanding)),
            request_bytes: Arc::new(Semaphore::new(config.request_bytes)),
            response_bytes: Arc::new(Semaphore::new(config.response_bytes)),
        }
    }
}

async fn accept_connections<H: RequestHandler>(
    listener: TcpListener,
    mut stop: watch::Receiver<bool>,
    config: ServerConfig,
    limits: Arc<Limits>,
    handler: Arc<H>,
) {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            _ = stop.changed() => break,
            completed = connections.join_next(), if !connections.is_empty() => { let _ = completed; }
            accepted = listener.accept() => {
                let Ok((stream, peer)) = accepted else {
                    log::debug!("raft transport server accept failed");
                    continue;
                };
                let Ok(connection) = Arc::clone(&limits.connections).try_acquire_owned() else {
                    log::debug!("raft transport server rejected connection at capacity remote={peer}");
                    drop(stream);
                    continue;
                };
                log::debug!("raft transport server accepted connection remote={peer}");
                connections.spawn(run_connection(stream, config, Arc::clone(&limits), Arc::clone(&handler), connection, stop.clone()));
            }
        }
    }
    // Close listener by dropping it, then await every accepted connection. Each
    // connection owns and joins its application handlers before it returns.
    while connections.join_next().await.is_some() {}
}

async fn run_connection<H: RequestHandler>(
    stream: TcpStream,
    config: ServerConfig,
    limits: Arc<Limits>,
    handler: Arc<H>,
    _connection: OwnedSemaphorePermit,
    mut stop: watch::Receiver<bool>,
) {
    let peer = stream.peer_addr().ok();
    let (reader, writer) = stream.into_split();
    let (responses, response_receiver) = mpsc::channel(config.connection_response_queue);
    let mut writer = tokio::spawn(write_responses(
        writer,
        response_receiver,
        config.io_timeout,
    ));
    let mut handlers = JoinSet::new();
    let mut active = BTreeSet::new();
    let mut reader = reader;
    let mut writer_finished = false;
    loop {
        // A receive can have already consumed a header or part of a payload.
        // Keep this exact future alive while completed handlers are drained;
        // recreating it in `select!` would treat the remaining bytes as a new
        // frame and corrupt the stream.
        let incoming = {
            let receive = receive_request::<H::Request>(&mut reader, config.io_timeout, &limits);
            tokio::pin!(receive);
            loop {
                tokio::select! {
                    biased;
                    _ = stop.changed() => break None,
                    writer_result = &mut writer => {
                        // A timed out, disconnected, or panicked response
                        // writer cannot report any later request. Retire input
                        // before it can admit more work; accepted handlers are
                        // still joined after the response sender is dropped.
                        let _ = writer_result;
                        log::debug!("raft transport server response writer ended remote={peer:?}");
                        writer_finished = true;
                        break None;
                    }
                    completed = handlers.join_next(), if !handlers.is_empty() => {
                        if let Some(Ok(id)) = completed { active.remove(&id); }
                    }
                    result = &mut receive => break Some(result),
                }
            }
        };
        let Some(Ok((envelope, request_bytes))) = incoming else {
            log::debug!("raft transport server request reader ended remote={peer:?}");
            break;
        };
        let request_id = envelope.request_id.get();
        if envelope.version != VERSION
            || envelope.budget_ms == 0
            || envelope.budget_ms > 60_000
            || envelope.request.validate().is_err()
        {
            log::debug!(
                "raft transport server rejected invalid request remote={peer:?} request_id={request_id}"
            );
            break;
        }
        if !active.insert(envelope.request_id) {
            log::debug!(
                "raft transport server rejected duplicate request remote={peer:?} request_id={request_id}"
            );
            break;
        }
        let deadline = Instant::now() + Duration::from_millis(envelope.budget_ms);
        if deadline <= Instant::now() {
            log::debug!(
                "raft transport server request deadline elapsed before handoff remote={peer:?} request_id={request_id}"
            );
            active.remove(&envelope.request_id);
            continue;
        }
        let Some(work) = reserve_work(&limits) else {
            active.remove(&envelope.request_id);
            // The server cannot safely report whether an application layer
            // would have admitted this request. Retire this stream instead of
            // fabricating a client-visible outcome.
            log::debug!(
                "raft transport server rejected request at operation capacity remote={peer:?} request_id={request_id}"
            );
            break;
        };
        // The maximum legal response is charged before user code runs. This
        // prevents GET handlers from materializing unbudgeted data.
        let Some(response_bytes) = reserve_response::<H::Request, H::Response>(&limits) else {
            active.remove(&envelope.request_id);
            drop(work);
            log::debug!(
                "raft transport server rejected request at response capacity remote={peer:?} request_id={request_id}"
            );
            break;
        };
        let response_sender = responses.clone();
        log::trace!(
            "raft transport server handed request to handler remote={peer:?} request_id={request_id}"
        );
        handlers.spawn(run_handler(
            handler.clone(),
            envelope,
            deadline,
            response_sender,
            work,
            request_bytes,
            response_bytes,
        ));
    }
    // A malformed/duplicate input retires the socket immediately. Abort only
    // the network writer; accepted application futures below are still owned
    // and joined, and their failed response sends release permits on completion.
    if !writer_finished {
        writer.abort();
        let _ = writer.await;
    }
    drop(responses);
    // Reader failure never aborts accepted work. This is essential for durable
    // mutations: socket loss changes observation, not the Raft proposal's life.
    while handlers.join_next().await.is_some() {}
}

async fn run_handler<H: RequestHandler>(
    handler: Arc<H>,
    envelope: RequestEnvelope<H::Request>,
    deadline: Instant,
    sender: mpsc::Sender<EncodedResponse>,
    work: WorkPermit,
    _request_bytes: OwnedSemaphorePermit,
    response_bytes: OwnedSemaphorePermit,
) -> RequestId {
    let request_id = envelope.request_id;
    let request_id_value = request_id.get();
    // The second check prevents a queued reader turn from calling a handler
    // after its budget elapsed. The future itself is never force-cancelled.
    if deadline <= Instant::now() {
        log::debug!("raft transport server handler deadline elapsed request_id={request_id_value}");
        return request_id;
    }
    let response = handler.handle(envelope.request, deadline).await;
    let encoded = ResponseEnvelope {
        version: VERSION,
        request_id,
        response,
    };
    let Ok(frame) = encode_frame(&encoded) else {
        log::debug!("raft transport server response encoding failed request_id={request_id_value}");
        return request_id;
    };
    // Retain the operation and response reservation through slow output. If
    // the peer disconnected, send fails and these values release only now.
    let frame_bytes = frame.len();
    let delivered = sender
        .send(EncodedResponse {
            request_id,
            frame,
            _work: work,
            _response_bytes: response_bytes,
        })
        .await
        .is_ok();
    if delivered {
        log::trace!(
            "raft transport server queued response request_id={request_id_value} frame_bytes={frame_bytes}"
        );
    } else {
        log::debug!("raft transport server response queue closed request_id={request_id_value}");
    }
    request_id
}

struct WorkPermit {
    _operation: OwnedSemaphorePermit,
}

fn reserve_work(limits: &Limits) -> Option<WorkPermit> {
    Arc::clone(&limits.operations)
        .try_acquire_owned()
        .ok()
        .map(|permit| WorkPermit { _operation: permit })
}

fn reserve_response<Request: WireRequest<Response>, Response>(
    limits: &Limits,
) -> Option<OwnedSemaphorePermit> {
    let amount = (MAX_FRAME_PAYLOAD + 4)
        .checked_mul(2)?
        .checked_add(Request::RESPONSE_DECODE_HEADROOM)?;
    let amount = u32::try_from(amount).ok()?;
    Arc::clone(&limits.response_bytes)
        .try_acquire_many_owned(amount)
        .ok()
}

async fn receive_request<R: DeserializeOwned>(
    reader: &mut OwnedReadHalf,
    io_timeout: Duration,
    limits: &Limits,
) -> Result<(RequestEnvelope<R>, OwnedSemaphorePermit), ()> {
    let mut header = [0; 4];
    timeout(io_timeout, reader.read_exact(&mut header))
        .await
        .map_err(|_| ())?
        .map_err(|_| ())?;
    let length = usize::try_from(u32::from_be_bytes(header)).map_err(|_| ())?;
    if length == 0 || length > MAX_FRAME_PAYLOAD {
        return Err(());
    }
    // Charge the globally retained request budget before allocating the
    // payload. The permit moves into the accepted handler after decode, so a
    // disconnected caller cannot release its application work early.
    let retained = (length + 4).checked_mul(2).ok_or(())?;
    let permit = Arc::clone(&limits.request_bytes)
        .try_acquire_many_owned(u32::try_from(retained).map_err(|_| ())?)
        .map_err(|_| ())?;
    let mut frame = vec![0; length + 4];
    frame[..4].copy_from_slice(&header);
    timeout(io_timeout, reader.read_exact(&mut frame[4..]))
        .await
        .map_err(|_| ())?
        .map_err(|_| ())?;
    let envelope = decode_frame(&frame).map_err(|_| ())?;
    Ok((envelope, permit))
}

struct EncodedResponse {
    request_id: RequestId,
    frame: Vec<u8>,
    _work: WorkPermit,
    _response_bytes: OwnedSemaphorePermit,
}

async fn write_responses(
    mut writer: OwnedWriteHalf,
    mut responses: mpsc::Receiver<EncodedResponse>,
    io_timeout: Duration,
) {
    while let Some(response) = responses.recv().await {
        let request_id = response.request_id.get();
        let frame_bytes = response.frame.len();
        if !matches!(
            timeout(io_timeout, writer.write_all(&response.frame)).await,
            Ok(Ok(()))
        ) {
            log::debug!(
                "raft transport server response write failed request_id={request_id} frame_bytes={frame_bytes}"
            );
            break;
        }
        if writer.flush().await.is_err() {
            log::debug!("raft transport server response flush failed request_id={request_id}");
            break;
        }
        log::trace!(
            "raft transport server response written request_id={request_id} frame_bytes={frame_bytes}"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn writer_timeout_closes_the_output_half() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind owned test listener");
        let address = listener.local_addr().expect("read listener address");
        let mut client = TcpStream::connect(address)
            .await
            .expect("connect owned test client");
        let (server, _) = listener.accept().await.expect("accept owned client");
        let (_, writer) = server.into_split();
        let (sender, receiver) = mpsc::channel(1);

        let operation = Arc::new(Semaphore::new(1))
            .try_acquire_owned()
            .expect("reserve operation");
        let response = Arc::new(Semaphore::new(1))
            .try_acquire_owned()
            .expect("reserve response bytes");
        sender
            .send(EncodedResponse {
                // The client deliberately does not read. A live sender keeps
                // the worker waiting after a successful write, so task
                // completion proves the bounded write itself terminated it.
                request_id: RequestId::new(1).expect("nonzero test request ID"),
                frame: vec![0; 16 * 1024 * 1024],
                _work: WorkPermit {
                    _operation: operation,
                },
                _response_bytes: response,
            })
            .await
            .expect("queue output");
        let writer_task =
            tokio::spawn(write_responses(writer, receiver, Duration::from_millis(10)));

        timeout(Duration::from_secs(1), writer_task)
            .await
            .expect("writer exits on its bounded output wait")
            .expect("writer task does not panic");

        let mut output = Vec::new();
        timeout(Duration::from_secs(1), client.read_to_end(&mut output))
            .await
            .expect("writer closes its output half")
            .expect("drain partial output");
    }
}
