//! Test-only backend relay that loses one proven committed PUT response.

use super::Events;
use bytes::Bytes;
use rockbee_raft_protocol::{
    ClusterId, KvRequest, MAX_FRAME_PAYLOAD, MutationOutcome, NodeId, RequestEnvelope,
    ResponseEnvelope, VERSION, WireRequest,
    backend::{BackendOperation, BackendReply, BackendRequest, BackendResponse},
    decode_frame,
};
use std::{
    future::Future,
    io,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    task::{JoinHandle, JoinSet},
    time::{Duration, Instant},
};

const MAX_CONNECTIONS: usize = 16;
const MAX_FRAMES_PER_CONNECTION: usize = 128;
const SHUTDOWN_GRACE: Duration = Duration::from_secs(2);

#[derive(Clone, Debug, Default)]
pub(super) struct BackendProxyReceipt {
    pub(super) accepted_connections: usize,
    pub(super) forwarded_put_frames: usize,
    pub(super) suppressed_response_bytes: usize,
    pub(super) lost_generation_eof_observed: bool,
    pub(super) committed_at: Option<u64>,
}

pub(super) struct BackendReplyLossProxy {
    pub(super) address: SocketAddr,
    shutdown: watch::Sender<Option<Instant>>,
    task: Option<JoinHandle<Result<BackendProxyReceipt, String>>>,
}

impl BackendReplyLossProxy {
    pub(super) fn start(
        listener: TcpListener,
        backend: SocketAddr,
        cluster_id: ClusterId,
        node_id: NodeId,
        target_key: Bytes,
        events: Arc<Events>,
    ) -> io::Result<Self> {
        let address = listener.local_addr()?;
        let (shutdown, stopped) = watch::channel(None);
        let task = tokio::spawn(run_listener(
            listener,
            backend,
            cluster_id,
            node_id,
            target_key,
            events,
            shutdown.clone(),
            stopped,
        ));
        Ok(Self {
            address,
            shutdown,
            task: Some(task),
        })
    }

    pub(super) async fn close(&mut self) -> Result<BackendProxyReceipt, String> {
        let _ = self.shutdown.send(Some(Instant::now() + SHUTDOWN_GRACE));
        let joined = self
            .task
            .as_mut()
            .expect("owned backend reply-loss proxy task")
            .await;
        self.task.take();
        joined.map_err(|error| format!("backend reply-loss proxy task failed: {error}"))?
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_listener(
    listener: TcpListener,
    backend: SocketAddr,
    cluster_id: ClusterId,
    node_id: NodeId,
    target_key: Bytes,
    events: Arc<Events>,
    shutdown: watch::Sender<Option<Instant>>,
    mut stopped: watch::Receiver<Option<Instant>>,
) -> Result<BackendProxyReceipt, String> {
    let consumed = Arc::new(AtomicBool::new(false));
    let mut connections = JoinSet::new();
    let mut receipt = BackendProxyReceipt::default();
    let mut failure = None;
    let mut shutdown_deadline = None;

    while failure.is_none() {
        tokio::select! {
            biased;
            accepted = listener.accept() => {
                let (client, _) = match accepted {
                    Ok(connection) => connection,
                    Err(error) => {
                        failure = Some(error.to_string());
                        continue;
                    }
                };
                receipt.accepted_connections += 1;
                if receipt.accepted_connections > MAX_CONNECTIONS {
                    failure = Some(format!("backend reply-loss proxy exceeded {MAX_CONNECTIONS} total connections"));
                    continue;
                }
                if connections.len() >= MAX_CONNECTIONS {
                    failure = Some(format!("backend reply-loss proxy exceeded {MAX_CONNECTIONS} concurrent connections"));
                    continue;
                }
                connections.spawn(relay_connection(
                    client,
                    backend,
                    cluster_id,
                    node_id,
                    target_key.clone(),
                    Arc::clone(&events),
                    Arc::clone(&consumed),
                    stopped.clone(),
                ));
            }
            joined = connections.join_next(), if !connections.is_empty() => {
                record_connection(joined, &mut receipt, &mut failure);
            }
            changed = stopped.changed(), if shutdown_deadline.is_none() => {
                if changed.is_err() {
                    failure = Some("backend reply-loss proxy shutdown sender disappeared".into());
                } else {
                    shutdown_deadline = *stopped.borrow();
                }
            }
            _ = std::future::ready(()), if shutdown_deadline.is_some() && connections.is_empty() => {
                // Accepted work has reached EOF and the higher-priority accept
                // branch has drained already queued connections.
                break;
            }
            _ = tokio::time::sleep_until(shutdown_deadline.unwrap_or_else(Instant::now)), if shutdown_deadline.is_some() => {
                break;
            }
        }
    }

    // A failed relay must wake every owned connection before joining it. The
    // normal close path also sends this signal after the gateway has closed.
    if failure.is_some() {
        let _ = shutdown.send(Some(Instant::now() + SHUTDOWN_GRACE));
    }
    while let Some(joined) = connections.join_next().await {
        record_connection(Some(joined), &mut receipt, &mut failure);
    }
    match failure {
        Some(error) => Err(error),
        None => Ok(receipt),
    }
}

fn record_connection(
    joined: Option<Result<Result<BackendProxyReceipt, String>, tokio::task::JoinError>>,
    total: &mut BackendProxyReceipt,
    failure: &mut Option<String>,
) {
    let Some(joined) = joined else {
        return;
    };
    match joined {
        Ok(Ok(receipt)) => {
            total.forwarded_put_frames += receipt.forwarded_put_frames;
            total.suppressed_response_bytes += receipt.suppressed_response_bytes;
            total.lost_generation_eof_observed |= receipt.lost_generation_eof_observed;
            if let Some(committed_at) = receipt.committed_at
                && total.committed_at.replace(committed_at).is_some()
            {
                *failure =
                    Some("backend reply-loss proxy suppressed more than one response".into());
            }
        }
        Ok(Err(error)) => {
            if failure.is_none() {
                *failure = Some(error);
            }
        }
        Err(error) => {
            if failure.is_none() {
                *failure = Some(format!(
                    "backend reply-loss connection task failed: {error}"
                ));
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn relay_connection(
    client: TcpStream,
    backend_address: SocketAddr,
    cluster_id: ClusterId,
    node_id: NodeId,
    target_key: Bytes,
    events: Arc<Events>,
    consumed: Arc<AtomicBool>,
    mut stopped: watch::Receiver<Option<Instant>>,
) -> Result<BackendProxyReceipt, String> {
    let (mut gateway_read, mut gateway_write) = client.into_split();
    let mut backend = interruptible(&mut stopped, TcpStream::connect(backend_address)).await?;
    let mut receipt = BackendProxyReceipt::default();

    for _ in 0..MAX_FRAMES_PER_CONNECTION {
        let request_frame =
            interruptible(&mut stopped, read_frame_or_eof(&mut gateway_read)).await?;
        let Some(request_frame) = request_frame else {
            return Ok(receipt);
        };
        let request: RequestEnvelope<BackendRequest> =
            decode_frame(&request_frame).map_err(|error| error.to_string())?;
        validate_request(&request, cluster_id, node_id)?;
        let is_target_put = matches!(
            &request.request.operation,
            BackendOperation::Kv {
                request: KvRequest::Put { key, .. },
                ..
            } if key == &target_key
        );

        if is_target_put {
            receipt.forwarded_put_frames += 1;
        }
        interruptible(&mut stopped, backend.write_all(&request_frame)).await?;
        interruptible(&mut stopped, backend.flush()).await?;

        let response_frame = interruptible(&mut stopped, read_frame(&mut backend)).await?;
        let response: ResponseEnvelope<BackendResponse> =
            decode_frame(&response_frame).map_err(|error| error.to_string())?;
        validate_response(&request, &response)?;

        let committed_target_put = is_target_put
            && matches!(
                &response.response.reply,
                BackendReply::Kv {
                    response: rockbee_raft_protocol::KvResponse::Mutation(
                        MutationOutcome::Committed
                    ),
                    ..
                }
            );
        if !committed_target_put {
            interruptible(&mut stopped, gateway_write.write_all(&response_frame)).await?;
            interruptible(&mut stopped, gateway_write.flush()).await?;
            continue;
        }

        if consumed.swap(true, Ordering::AcqRel) {
            return Err("backend reply-loss proxy observed a second committed target PUT".into());
        }
        receipt.committed_at = Some(events.next());
        receipt.suppressed_response_bytes = response_frame.len();
        interruptible(&mut stopped, gateway_write.shutdown()).await?;
        drain_lost_generation(&mut stopped, &mut gateway_read).await?;
        receipt.lost_generation_eof_observed = true;
        return Ok(receipt);
    }
    Err(format!(
        "backend reply-loss proxy exceeded {MAX_FRAMES_PER_CONNECTION} frames on one connection"
    ))
}

fn validate_request(
    request: &RequestEnvelope<BackendRequest>,
    cluster_id: ClusterId,
    node_id: NodeId,
) -> Result<(), String> {
    if request.version != VERSION || !(1..=60_000).contains(&request.budget_ms) {
        return Err("backend request has an invalid version or budget".into());
    }
    request
        .request
        .validate()
        .map_err(|error| error.to_string())?;
    if request.request.cluster_id != cluster_id || request.request.target_node != node_id {
        return Err("backend request cluster or target node mismatch".into());
    }
    Ok(())
}

fn validate_response(
    request: &RequestEnvelope<BackendRequest>,
    response: &ResponseEnvelope<BackendResponse>,
) -> Result<(), String> {
    if response.version != VERSION || response.request_id != request.request_id {
        return Err("backend response version or request id mismatch".into());
    }
    BackendRequest::validate_response(request.request.expected_response(), &response.response)
        .map_err(|error| error.to_string())
}

async fn interruptible<T, F>(
    stopped: &mut watch::Receiver<Option<Instant>>,
    operation: F,
) -> Result<T, String>
where
    F: Future<Output = io::Result<T>>,
{
    tokio::pin!(operation);
    let shutdown_deadline = *stopped.borrow();
    if let Some(deadline) = shutdown_deadline {
        return complete_before(deadline, &mut operation).await;
    }
    tokio::select! {
        biased;
        result = &mut operation => result.map_err(|error| error.to_string()),
        changed = stopped.changed() => {
            changed.map_err(|_| "backend reply-loss proxy shutdown sender disappeared".to_owned())?;
            let deadline = (*stopped.borrow())
                .ok_or_else(|| "backend reply-loss proxy received an invalid shutdown transition".to_owned())?;
            complete_before(deadline, &mut operation).await
        }
    }
}

async fn complete_before<T>(
    deadline: Instant,
    operation: impl Future<Output = io::Result<T>>,
) -> Result<T, String> {
    tokio::time::timeout_at(deadline, operation)
        .await
        .map_err(|_| "backend reply-loss proxy shutdown grace expired".to_owned())?
        .map_err(|error| error.to_string())
}

async fn drain_lost_generation(
    stopped: &mut watch::Receiver<Option<Instant>>,
    reader: &mut (impl AsyncRead + Unpin),
) -> Result<(), String> {
    interruptible(stopped, read_eof(reader)).await
}

async fn read_frame(reader: &mut (impl AsyncRead + Unpin)) -> io::Result<Vec<u8>> {
    let mut header = [0_u8; 4];
    reader.read_exact(&mut header).await?;
    let length = usize::try_from(u32::from_be_bytes(header))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "frame length conversion"))?;
    if length == 0 || length > MAX_FRAME_PAYLOAD {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid frame length",
        ));
    }
    let mut frame = vec![0; length + header.len()];
    frame[..header.len()].copy_from_slice(&header);
    reader.read_exact(&mut frame[header.len()..]).await?;
    Ok(frame)
}

async fn read_frame_or_eof(reader: &mut (impl AsyncRead + Unpin)) -> io::Result<Option<Vec<u8>>> {
    let mut header = [0_u8; 4];
    let first = reader.read(&mut header).await?;
    if first == 0 {
        return Ok(None);
    }
    reader.read_exact(&mut header[first..]).await?;
    let length = usize::try_from(u32::from_be_bytes(header))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "frame length conversion"))?;
    if length == 0 || length > MAX_FRAME_PAYLOAD {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid frame length",
        ));
    }
    let mut frame = vec![0; length + header.len()];
    frame[..header.len()].copy_from_slice(&header);
    reader.read_exact(&mut frame[header.len()..]).await?;
    Ok(Some(frame))
}

async fn read_eof(reader: &mut (impl AsyncRead + Unpin)) -> io::Result<()> {
    let mut byte = [0_u8; 1];
    if reader.read(&mut byte).await? == 0 {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "lost gateway generation sent extra full or partial request bytes",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shutdown_preserves_buffered_request_on_another_generation() {
        let frame = rockbee_raft_protocol::encode_frame(&7_u8).expect("small complete frame");
        let (mut writer, mut reader) = tokio::io::duplex(16);
        writer
            .write_all(&frame)
            .await
            .expect("buffer complete request");
        writer.shutdown().await.expect("close source");
        let (_shutdown, mut stopped) = watch::channel(Some(Instant::now() + SHUTDOWN_GRACE));
        assert_eq!(
            interruptible(&mut stopped, read_frame_or_eof(&mut reader))
                .await
                .expect("drain buffered ingress"),
            Some(frame),
        );
        assert_eq!(
            interruptible(&mut stopped, read_frame_or_eof(&mut reader))
                .await
                .expect("observe actual EOF after the frame"),
            None,
        );
    }

    #[tokio::test]
    async fn shutdown_rejects_partial_request_on_another_generation() {
        let (mut writer, mut reader) = tokio::io::duplex(8);
        writer
            .write_all(&[0, 0])
            .await
            .expect("buffer partial header");
        writer.shutdown().await.expect("close source");
        let (_shutdown, mut stopped) = watch::channel(Some(Instant::now() + SHUTDOWN_GRACE));
        assert!(
            interruptible(&mut stopped, read_frame_or_eof(&mut reader))
                .await
                .is_err(),
            "shutdown cannot turn unread partial ingress into a successful receipt",
        );
    }

    #[tokio::test]
    async fn shutdown_grace_rejects_buffered_partial_lost_generation_data() {
        let (mut writer, mut reader) = tokio::io::duplex(8);
        writer
            .write_all(&[0, 0])
            .await
            .expect("buffer partial frame");
        writer.shutdown().await.expect("close duplex writer");
        let (_shutdown, mut stopped) = watch::channel(Some(Instant::now() + SHUTDOWN_GRACE));

        assert!(
            drain_lost_generation(&mut stopped, &mut reader)
                .await
                .is_err()
        );
    }
}
