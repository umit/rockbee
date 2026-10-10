use super::config::RpcConfig;
use super::stats::RpcStats;
use rockbee_protocol::transport::{FrameReader, read_frame, write_frame};
use rockbee_protocol::{
    INTERNAL_MAX_IN_FLIGHT, MutationError, MutationOutcome, Request, RequestEnvelope, Response,
    ResponseEnvelope, Result, WireMessage,
};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWrite;
use tokio::net::TcpStream;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::timeout;

async fn write_with_timeout(
    output: &mut (impl AsyncWrite + Unpin),
    response: &impl WireMessage,
    budget: Duration,
) -> Result<()> {
    timeout(budget, write_frame(output, response))
        .await
        .map_err(|_| "RPC frame write timed out")?
}

pub(super) async fn handle_connection<F, Fut>(
    stream: TcpStream,
    handler: Arc<F>,
    mut shutdown: watch::Receiver<bool>,
    config: RpcConfig,
    process_admissions: Arc<Semaphore>,
    stats: Arc<RpcStats>,
) -> Result<()>
where
    F: Fn(Request) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Response>> + Send + 'static,
{
    let (input, mut output) = stream.into_split();
    let mut input = FrameReader::new(input);
    let mut sequential = false;
    let mut next_frame = None;
    loop {
        let read = match next_frame.take() {
            Some(frame) => Ok(Some(frame)),
            None => tokio::select! {
                biased;
                _ = shutdown.changed() => return Ok(()),
                read = timeout(config.read_timeout(), read_frame(&mut input)) =>
                    read.map_err(|_| "RPC frame read timed out")?,
            },
        };
        let frame = match read {
            Ok(Some(frame)) => frame,
            Ok(None) => return Ok(()),
            Err(error) => {
                let response = Response::Error {
                    mutation_outcome: MutationOutcome::Unknown,
                    message: error.to_string(),
                };
                tokio::select! {
                    biased;
                    _ = shutdown.changed() => {},
                    result = write_with_timeout(&mut output, &response, config.write_timeout()) => result?,
                }
                return Ok(());
            }
        };
        let envelope = match RequestEnvelope::decode(&frame) {
            Ok(envelope) => envelope,
            Err(error) => {
                let response = Response::Error {
                    mutation_outcome: MutationOutcome::Unknown,
                    message: error.to_string(),
                };
                tokio::select! {
                    biased;
                    _ = shutdown.changed() => {},
                    result = write_with_timeout(&mut output, &response, config.write_timeout()) => result?,
                }
                return Ok(());
            }
        };
        if envelope.request_id > 0 {
            if sequential {
                return Err("internal IDs are not allowed on this connection".into());
            }
            // The first ID is already positive. Move the decoded request and
            // release its raw frame before entering the connection-long await.
            drop(frame);
            return internal_connection(
                input,
                output,
                envelope,
                handler,
                ConnectionContext {
                    shutdown,
                    config,
                    process_admissions,
                    stats,
                },
            )
            .await;
        }
        sequential = true;
        let _process_permit = Arc::clone(&process_admissions)
            .try_acquire_owned()
            .map_err(|_| "RPC process-wide admission limit reached")?;
        let operation = async {
            let response = process_frame(&*handler, &frame, &stats).await;
            // Egress has one response owner per connection. Admission
            // must be released before its bytes become visible to a peer.
            drop(_process_permit);
            write_with_timeout(&mut output, &response, config.write_timeout()).await
        };
        tokio::pin!(operation);
        loop {
            tokio::select! {
                biased;
                _ = shutdown.changed() => return Ok(()),
                read = read_frame(&mut input) => {
                    // EOF (including a write-half-close) retires pending work.
                    // It cannot undo a mutation already accepted by storage.
                    let Some(frame) = read? else { return Ok(()); };
                    if next_frame.is_some() {
                        return Err("sequential pending frame limit reached".into());
                    }
                    // Preserve response order: decode/execute this frame only
                    // after the current response is written. Keep polling for EOF.
                    next_frame = Some(frame);
                }
                // Ready EOF/overflow wins before a completed response can
                // advance to the staged request.
                result = &mut operation => { result?; break; },
            }
        }
        // Lookahead has no idle timeout while the handler runs. Partial bytes
        // stay in FrameReader; the next normal read budget starts after this reply.
    }
}

fn parse_internal(frame: &[u8], last_id: u64) -> Result<RequestEnvelope> {
    let envelope = RequestEnvelope::decode(frame)?;
    if envelope.request_id == 0 || envelope.request_id <= last_id {
        return Err("internal IDs must be positive and strictly increasing".into());
    }
    Ok(envelope)
}

struct AdmittedRequest {
    envelope: RequestEnvelope,
    permit: OwnedSemaphorePermit,
    process_permit: OwnedSemaphorePermit,
}
struct AdmittedResponse {
    envelope: ResponseEnvelope,
    _permit: OwnedSemaphorePermit,
    _process_permit: OwnedSemaphorePermit,
}

struct ConnectionContext {
    shutdown: watch::Receiver<bool>,
    config: RpcConfig,
    process_admissions: Arc<Semaphore>,
    stats: Arc<RpcStats>,
}

async fn drain_responses(
    mut output: impl AsyncWrite + Unpin,
    mut responses: mpsc::Receiver<AdmittedResponse>,
    write_timeout: Duration,
) -> Result<()> {
    while let Some(response) = responses.recv().await {
        let AdmittedResponse {
            envelope,
            _permit,
            _process_permit,
        } = response;
        // The single writer now owns this bounded egress response. Releasing
        // queue/handler admission before writing avoids a peer replenishing
        // its window while our just-written response still owns a permit.
        drop((_permit, _process_permit));
        write_with_timeout(&mut output, &envelope, write_timeout).await?;
    }
    Ok(())
}

async fn internal_connection<F, Fut>(
    mut input: FrameReader<tokio::net::tcp::OwnedReadHalf>,
    output: tokio::net::tcp::OwnedWriteHalf,
    first: RequestEnvelope,
    handler: Arc<F>,
    context: ConnectionContext,
) -> Result<()>
where
    F: Fn(Request) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Response>> + Send + 'static,
{
    let ConnectionContext {
        mut shutdown,
        config,
        process_admissions,
        stats,
    } = context;
    let admissions = Arc::new(Semaphore::new(INTERNAL_MAX_IN_FLIGHT));
    let (send_requests, mut requests) = mpsc::channel::<AdmittedRequest>(INTERNAL_MAX_IN_FLIGHT);
    let (send_responses, responses) = mpsc::channel::<AdmittedResponse>(INTERNAL_MAX_IN_FLIGHT);
    // Both driver and handler JoinSets abort their tasks on owner cancellation/drop.
    let mut drivers = JoinSet::<Result<()>>::new();
    drivers.spawn(async move {
        let mut next = Some(first);
        let mut high_water = 0;
        loop {
            let envelope = match next.take() {
                Some(envelope) => envelope,
                None => {
                    let Some(frame) = timeout(config.read_timeout(), read_frame(&mut input))
                        .await
                        .map_err(|_| "RPC frame read timed out")??
                    else {
                        return Ok(());
                    };
                    parse_internal(&frame, high_water)?
                }
            };
            // Keep reading at capacity so EOF still retires stalled handlers. One bounded
            // unadmitted staging frame is allowed; overload retires instead of queueing it.
            let process_permit = Arc::clone(&process_admissions)
                .try_acquire_owned()
                .map_err(|_| "RPC process-wide admission limit reached")?;
            let permit = admissions
                .clone()
                .try_acquire_owned()
                .map_err(|_| "internal admission limit reached")?;
            high_water = envelope.request_id;
            send_requests
                .send(AdmittedRequest {
                    envelope,
                    permit,
                    process_permit,
                })
                .await?;
        }
    });
    drivers.spawn(drain_responses(output, responses, config.write_timeout()));
    let mut handlers = JoinSet::new();
    let result = loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => break Ok(()),
            stopped = drivers.join_next() => {
                break match stopped {
                    Some(Ok(result)) => result,
                    Some(Err(error)) => Err(error.into()),
                    None => Ok(()),
                };
            }
            completed = handlers.join_next(), if !handlers.is_empty() => {
                match completed {
                    Some(Ok(response)) => {
                        // Cannot overflow: queued responses and handlers share the 128 permits.
                        if send_responses.try_send(response).is_err() { break Err("internal response queue closed or full".into()); }
                    }
                    Some(Err(error)) => break Err(error.into()),
                    None => {}
                }
            }
            request = requests.recv() => {
                let Some(AdmittedRequest{envelope,permit,process_permit}) = request else { break Ok(()); };
                let handler=handler.clone();
                let stats = Arc::clone(&stats);
                handlers.spawn(async move {
                    let envelope = process_request(&*handler, envelope, &stats).await;
                    AdmittedResponse{envelope,_permit:permit,_process_permit:process_permit}
                });
            }
        }
    };
    drivers.abort_all();
    handlers.abort_all();
    while drivers.join_next().await.is_some() {}
    while handlers.join_next().await.is_some() {}
    result
}

pub(crate) async fn process_frame<F, Fut>(handler: &F, frame: &[u8], stats: &RpcStats) -> Response
where
    F: Fn(Request) -> Fut,
    Fut: Future<Output = Result<Response>>,
{
    let envelope = match RequestEnvelope::decode(frame) {
        Ok(envelope) if envelope.request_id == 0 => envelope,
        _ => {
            return Response::Error {
                mutation_outcome: MutationOutcome::Unknown,
                message: "Invalid request: expected a sequential PUT, GET, DELETE or STATUS".into(),
            };
        }
    };
    process_request(handler, envelope, stats).await.response
}

async fn process_request<F, Fut>(
    handler: &F,
    request: RequestEnvelope,
    stats: &RpcStats,
) -> ResponseEnvelope
where
    F: Fn(Request) -> Fut,
    Fut: Future<Output = Result<Response>>,
{
    if matches!(request.request, Request::Status) {
        return ResponseEnvelope {
            request_id: request.request_id,
            response: Response::Status {
                snapshot: stats.snapshot(),
            },
        };
    }
    let mutation = request.request.is_mutation();
    let response = match handler(request.request).await {
        Ok(response) => response,
        Err(error) => {
            // Only an explicit typed guarantee can establish rejection. A
            // generic handler error may follow a successful storage mutation.
            let typed = mutation
                .then(|| error.downcast_ref::<MutationError>())
                .flatten();
            Response::Error {
                mutation_outcome: typed.map_or(MutationOutcome::Unknown, MutationError::outcome),
                message: if typed.is_some() {
                    error
                        .source()
                        .map_or_else(|| error.to_string(), ToString::to_string)
                } else {
                    error.to_string()
                },
            }
        }
    };
    let mut envelope = ResponseEnvelope {
        request_id: request.request_id,
        response,
    };
    // A backend response can fit its internal ID but exceed the frame limit
    // with the caller's larger ID. Isolate that response before any bytes are
    // written; a lost mutation result must never imply rollback.
    if WireMessage::validate(&envelope).is_err() {
        envelope.response = Response::Error {
            mutation_outcome: MutationOutcome::Unknown,
            message: if mutation {
                "RPC response exceeds 1 MiB frame limit; mutation outcome is unknown"
            } else {
                "RPC response exceeds 1 MiB frame limit"
            }
            .into(),
        };
    }
    stats.prepared_crud_response(matches!(envelope.response, Response::Error { .. }));
    envelope
}

#[cfg(test)]
#[path = "tests/connection/mod.rs"]
mod tests;
