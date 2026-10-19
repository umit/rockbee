//! The receiving private peer listener and its owned request tasks.
use std::{collections::BTreeMap, future::Future, sync::Arc, time::Duration};

use log::{debug, info, trace};
use openraft::Raft;
use rockbee_raft_protocol::{ClusterId, GroupId, NodeId};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::watch,
    task::{JoinHandle, JoinSet},
    time::timeout,
};

use crate::{diagnostics::Span, types::TypeConfig};

use super::{
    PeerError, PeerResources,
    message::{
        PeerHeader, PeerRequest, PeerRequestBody, PeerResponse, PeerResponseBody,
        decode_request_body, encode_response,
    },
    network::{read_peer_body, read_peer_header},
};

/// Stable local identity used to validate every peer request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerIdentity {
    /// The immutable cluster that this node belongs to.
    pub cluster_id: ClusterId,
    /// The stable nonzero identity of this node.
    pub node_id: NodeId,
}

/// Owns the accept loop and every task created for accepted connections.
///
/// `close()` signals the accept loop, then waits for it. The accept loop waits
/// for accepted request tasks before returning, so a disconnected peer cannot
/// leave work running beyond the server lifetime.
pub struct PeerServer {
    node_id: NodeId,
    shutdown: watch::Sender<bool>,
    accept_task: Option<JoinHandle<()>>,
}

impl PeerServer {
    /// Starts accepting private Raft connections.
    pub fn start(
        listener: TcpListener,
        identity: PeerIdentity,
        groups: BTreeMap<GroupId, Raft<TypeConfig>>,
        resources: Arc<PeerResources>,
    ) -> Self {
        info!("raft_peer_server_start node={}", identity.node_id.get());
        let (shutdown, receiver) = watch::channel(false);
        let groups = Arc::new(groups);
        let accept_task = tokio::spawn(run_accept_loop(
            listener, identity, groups, resources, receiver,
        ));
        Self {
            node_id: identity.node_id,
            shutdown,
            accept_task: Some(accept_task),
        }
    }

    /// Stops acceptance and joins every child task owned by this server.
    pub async fn close(&mut self) {
        info!("raft_peer_server_stop node={}", self.node_id.get());
        let _ = self.shutdown.send(true);
        // Keep the handle in `self` while awaiting. If this future is cancelled,
        // a later `close()` (or Drop's signal) still owns the accept task.
        if let Some(task) = self.accept_task.as_mut() {
            let _ = task.await;
        }
        self.accept_task.take();
    }
}

impl Drop for PeerServer {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
    }
}

async fn run_accept_loop(
    listener: TcpListener,
    identity: PeerIdentity,
    groups: Arc<BTreeMap<GroupId, Raft<TypeConfig>>>,
    resources: Arc<PeerResources>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut tasks = JoinSet::new();
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { break; }
            }
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    // Do not create a queued task for an already-admitted-saturated
                    // process. Closing this socket asks the peer to retry through
                    // its normal Raft backoff path.
                    let connection = match resources.try_acquire_incoming_connection() {
                        Ok(connection) => connection,
                        Err(error) => {
                            let span = Span::start(
                                identity.node_id.get(),
                                0,
                                None,
                                "peer_connection_admission",
                                false,
                            );
                            span.failure(&error);
                            debug!("raft_peer_connection_rejected node={} error={}", identity.node_id.get(), error);
                            continue;
                        }
                    };
                    let child_groups = Arc::clone(&groups);
                    let child_resources = Arc::clone(&resources);
                    let child_shutdown = shutdown.clone();
                    tasks.spawn(async move {
                        let _connection = connection;
                        let span = Span::start(
                            identity.node_id.get(),
                            0,
                            None,
                            "peer_connection",
                            false,
                        );
                        match serve_connection(stream, identity, child_groups, child_resources, child_shutdown).await {
                            Ok(()) => span.success(),
                            Err(error) => {
                                span.failure(&error);
                                debug!("raft_peer_connection_closed node={} error={}", identity.node_id.get(), error);
                            }
                        }
                    });
                }
                Err(error) => {
                    let span = Span::start(
                        identity.node_id.get(),
                        0,
                        None,
                        "peer_accept",
                        false,
                    );
                    span.failure(&error);
                    debug!("raft_peer_accept_failed node={} error={}", identity.node_id.get(), error);
                    break;
                }
            },
            Some(_) = tasks.join_next(), if !tasks.is_empty() => {}
        }
    }
    while tasks.join_next().await.is_some() {}
}

async fn serve_connection(
    mut stream: TcpStream,
    identity: PeerIdentity,
    groups: Arc<BTreeMap<GroupId, Raft<TypeConfig>>>,
    resources: Arc<PeerResources>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), PeerError> {
    const PHASE_TIMEOUT: Duration = Duration::from_secs(10);
    let mut pinned_source = None;
    let mut connection_is_control = None;
    let mut control_connection = None;
    loop {
        // Each pre-dispatch phase is interruptible. Once `dispatch()` begins,
        // its Raft operation is owned by this child task and is drained before
        // close can return; a socket disconnect never cancels consensus work.
        let (header, body_len) = wait_for_phase(
            &mut shutdown,
            timeout(
                PHASE_TIMEOUT,
                read_peer_header(&mut stream, resources.limits().max_frame_bytes),
            ),
        )
        .await??;
        header.validate_for_receiver(identity.cluster_id, identity.node_id)?;
        trace!(
            "raft_peer_request node={} group={} source={} op={:?}",
            identity.node_id.get(),
            header.group_id.get(),
            header.source.get(),
            header.kind,
        );
        if let Some(source) = pinned_source
            && header.source != source
        {
            return Err(PeerError::Identity("connection source node"));
        }
        pinned_source = Some(header.source);
        let is_snapshot = header.kind == super::message::PeerKind::InstallSnapshot;
        if let Some(previous_is_control) = connection_is_control
            && previous_is_control == is_snapshot
        {
            return Err(PeerError::Protocol(
                "peer connection changed RPC class".into(),
            ));
        }
        if !is_snapshot && control_connection.is_none() {
            control_connection = Some(resources.try_acquire_incoming_control_connection()?);
            connection_is_control = Some(true);
        }
        let operation = if is_snapshot {
            // A saturated bulk lane is rejected before reading its body. It
            // cannot accumulate snapshot waiters that hold connection permits.
            resources.try_acquire_incoming_snapshot()?
        } else {
            wait_for_phase(
                &mut shutdown,
                timeout(PHASE_TIMEOUT, resources.acquire_incoming_control()),
            )
            .await??
        };
        let body = wait_for_phase(
            &mut shutdown,
            timeout(PHASE_TIMEOUT, read_peer_body(&mut stream, body_len)),
        )
        .await??;
        let kind = header.kind;
        let request = PeerRequest {
            header,
            body: decode_request_body(kind, &body)?,
        };
        request.validate_shape(resources.limits().max_snapshot_chunk_bytes)?;
        let response_header = PeerHeader::response_for(&request.header, identity.node_id);
        let response_body = dispatch(request, Arc::clone(&groups)).await;
        let response = PeerResponse {
            header: response_header,
            body: response_body,
        };
        let frame = encode_response(&response, resources.limits().max_frame_bytes)?;
        // Dispatch completed above. Timing out this write only releases the
        // completed operation; it does not report a synthetic Raft success.
        timeout(PHASE_TIMEOUT, async {
            stream.write_all(&frame).await?;
            stream.flush().await
        })
        .await
        .map_err(|_| PeerError::Deadline)??;
        drop(operation);
        if is_snapshot {
            return Ok(());
        }
    }
}

async fn wait_for_phase<T>(
    shutdown: &mut watch::Receiver<bool>,
    operation: tokio::time::Timeout<T>,
) -> Result<T::Output, PeerError>
where
    T: Future,
{
    tokio::select! {
        result = operation => result.map_err(|_| PeerError::Deadline),
        changed = shutdown.changed() => {
            let _ = changed;
            Err(PeerError::Shutdown)
        }
    }
}

async fn dispatch(
    request: PeerRequest,
    groups: Arc<BTreeMap<GroupId, Raft<TypeConfig>>>,
) -> PeerResponseBody {
    let Some(raft) = groups.get(&request.header.group_id).cloned() else {
        return PeerResponseBody::Failure {
            message: PeerError::UnknownGroup(request.header.group_id.get()).to_string(),
        };
    };
    let result = match request.body {
        PeerRequestBody::AppendEntries(rpc) => raft
            .append_entries(rpc)
            .await
            .map(PeerResponseBody::AppendEntries)
            .map_err(|error| error.to_string()),
        PeerRequestBody::Vote(rpc) => raft
            .vote(rpc)
            .await
            .map(PeerResponseBody::Vote)
            .map_err(|error| error.to_string()),
        PeerRequestBody::InstallSnapshot(rpc) => raft
            .install_snapshot(rpc)
            .await
            .map(PeerResponseBody::InstallSnapshot)
            .map_err(|error| error.to_string()),
    };
    match result {
        Ok(response) => response,
        Err(message) => PeerResponseBody::Failure {
            message: message.chars().take(512).collect(),
        },
    }
}
