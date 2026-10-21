#![allow(clippy::unwrap_used)]
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use rockbee_raft_protocol::{ClusterId, GroupId, NodeId};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, copy_bidirectional},
    net::{TcpListener, TcpStream},
    sync::{Notify, Semaphore, watch},
    task::{AbortHandle, JoinHandle, JoinSet},
    time::timeout,
};

use crate::rpc::{
    PeerIdentity, PeerLimits,
    wire::message::{PEER_HEADER_BYTES, PeerHeader},
};

#[derive(Debug)]
struct Connection {
    source: Option<NodeId>,
    target: NodeId,
    accepted_generation: u64,
    abort: AbortHandle,
}

#[derive(Default)]
pub(super) struct State {
    generation: u64,
    next_connection: u64,
    pub(super) blocked: BTreeSet<NodeId>,
    pub(super) blocked_edges: BTreeSet<(NodeId, NodeId)>,
    pub(super) blocked_groups: BTreeSet<GroupId>,
    connections: BTreeMap<u64, Connection>,
    pub(super) dispatches: BTreeMap<(NodeId, NodeId), usize>,
    pub(super) admitted: BTreeMap<(NodeId, NodeId), u64>,
    pub(super) group_dispatches:
        BTreeMap<(GroupId, NodeId, NodeId), super::dispatch::GroupDispatchCounts>,
}

#[derive(Default)]
pub(super) struct Shared {
    pub(super) state: Mutex<State>,
    pub(super) changed: Notify,
}

/// Evidence returned only after the fixture's cut has drained its owned work.
#[derive(Debug)]
pub(crate) struct CutAck {
    pub(crate) generation: u64,
    pub(crate) blocked_node: NodeId,
    pub(crate) surviving_connections: usize,
    pub(crate) earlier_dispatches: usize,
    pub(crate) blocked_edges: Vec<(NodeId, NodeId)>,
}

/// Installation receipt, deliberately distinct from a fully drained cut ACK.
#[derive(Debug)]
pub(crate) struct EdgeCutInstall {
    pub(crate) generation: u64,
    pub(crate) blocked_edges: BTreeSet<(NodeId, NodeId)>,
}

#[derive(Debug)]
pub(crate) struct EdgeCutAck {
    pub(crate) generation: u64,
    pub(crate) blocked_edges: BTreeSet<(NodeId, NodeId)>,
    pub(crate) surviving_connections: usize,
    pub(crate) earlier_dispatches: usize,
}

/// A receiver seal across every edge of one group, not a socket cut.
#[derive(Debug)]
pub(crate) struct GroupCutInstall {
    pub(crate) generation: u64,
    pub(crate) group_id: GroupId,
}

/// Earlier native dispatches drained while this exact group seal remained set.
#[derive(Debug)]
pub(crate) struct GroupCutAck {
    pub(crate) generation: u64,
    pub(crate) group_id: GroupId,
    pub(crate) earlier_dispatches: usize,
}

/// All advertised peer listeners and their bounded, joined forwarding tasks.
/// Backend listeners are deliberately outside this owner.
pub(crate) struct PeerRelays {
    shared: Arc<Shared>,
    shutdown: watch::Sender<bool>,
    tasks: Vec<JoinHandle<()>>,
    bindings: BTreeMap<NodeId, (SocketAddr, SocketAddr)>,
}

impl PeerRelays {
    /// Seal every decoded request of this group immediately before native RPC
    /// admission. Pooled sockets can carry other groups and are not aborted.
    pub(crate) fn install_group_cut(&self, group_id: GroupId) -> GroupCutInstall {
        let mut state = self.shared.state.lock().unwrap();
        state.generation += 1;
        state.blocked_groups.insert(group_id);
        GroupCutInstall {
            generation: state.generation,
            group_id,
        }
    }

    pub(crate) async fn drain_group_cut(&self, installed: &GroupCutInstall) -> GroupCutAck {
        loop {
            let changed = self.shared.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let earlier_dispatches = {
                let state = self.shared.state.lock().unwrap();
                assert_eq!(state.generation, installed.generation);
                assert!(state.blocked_groups.contains(&installed.group_id));
                state
                    .group_dispatches
                    .iter()
                    .filter(|((group, _, _), _)| *group == installed.group_id)
                    .map(|(_, counts)| counts.active)
                    .sum::<usize>()
            };
            if earlier_dispatches == 0 {
                return GroupCutAck {
                    generation: installed.generation,
                    group_id: installed.group_id,
                    earlier_dispatches,
                };
            }
            changed.await;
        }
    }

    /// Atomically seals the complete edge set and aborts affected sockets.
    /// This receipt does not claim that their owners or native RPCs have drained.
    pub(crate) fn install_edge_cut(
        &self,
        blocked_edges: BTreeSet<(NodeId, NodeId)>,
    ) -> EdgeCutInstall {
        let mut state = self.shared.state.lock().unwrap();
        state.generation += 1;
        state.blocked_edges.extend(blocked_edges);
        let installed = EdgeCutInstall {
            generation: state.generation,
            blocked_edges: state.blocked_edges.clone(),
        };
        for connection in state.connections.values() {
            if affected_edges(connection, &installed) {
                connection.abort.abort();
            }
        }
        installed
    }

    pub(crate) async fn drain_edge_cut(&self, installed: &EdgeCutInstall) -> EdgeCutAck {
        loop {
            let changed = self.shared.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let (surviving_connections, earlier_dispatches) = {
                let state = self.shared.state.lock().unwrap();
                // A receipt belongs to one installed generation. Healing or
                // installing another cut cannot silently satisfy its drain.
                assert_eq!(state.generation, installed.generation);
                assert_eq!(state.blocked_edges, installed.blocked_edges);
                let connections = state
                    .connections
                    .values()
                    .filter(|connection| affected_edges(connection, installed))
                    .count();
                let dispatches = state
                    .dispatches
                    .iter()
                    .filter(|(edge, _)| installed.blocked_edges.contains(*edge))
                    .map(|(_, count)| *count)
                    .sum();
                (connections, dispatches)
            };
            if surviving_connections == 0 && earlier_dispatches == 0 {
                return EdgeCutAck {
                    generation: installed.generation,
                    blocked_edges: installed.blocked_edges.clone(),
                    surviving_connections,
                    earlier_dispatches,
                };
            }
            changed.await;
        }
    }

    pub(crate) fn dispatch_tracker(&self) -> super::DispatchTracker {
        super::DispatchTracker::new(Arc::clone(&self.shared))
    }
    pub(crate) fn start(
        cluster_id: ClusterId,
        listeners: Vec<(NodeId, TcpListener, SocketAddr)>,
        limits: PeerLimits,
    ) -> Self {
        let shared = Arc::new(Shared::default());
        let (shutdown, receiver) = watch::channel(false);
        let bindings = listeners
            .iter()
            .map(|(node, listener, private)| (*node, (listener.local_addr().unwrap(), *private)))
            .collect();
        let tasks = listeners
            .into_iter()
            .map(|(node_id, listener, private)| {
                tokio::spawn(accept_loop(
                    listener,
                    private,
                    PeerIdentity {
                        cluster_id,
                        node_id,
                    },
                    limits,
                    Arc::clone(&shared),
                    receiver.clone(),
                ))
            })
            .collect();
        Self {
            shared,
            shutdown,
            tasks,
            bindings,
        }
    }

    /// Both addresses come from the actual owned fixture listeners.
    pub(crate) fn matches_binding(
        &self,
        node: NodeId,
        advertised: SocketAddr,
        private: SocketAddr,
    ) -> bool {
        self.bindings.get(&node) == Some(&(advertised, private))
    }

    /// Installs the seal before aborting owned sockets, then awaits their joins.
    /// Listener loops retain their JoinSets if this caller is cancelled.
    pub(crate) async fn cut(&self, node: NodeId) -> CutAck {
        let generation = {
            let mut state = self.shared.state.lock().unwrap();
            state.generation += 1;
            state.blocked.insert(node);
            let generation = state.generation;
            for connection in state.connections.values() {
                if affected(connection, node, generation) {
                    connection.abort.abort();
                }
            }
            generation
        };
        loop {
            let changed = self.shared.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let (surviving_connections, earlier_dispatches) = {
                let state = self.shared.state.lock().unwrap();
                let connections = state
                    .connections
                    .values()
                    .filter(|connection| affected(connection, node, generation))
                    .count();
                let dispatches = state
                    .dispatches
                    .iter()
                    .filter(|((source, target), _)| *source == node || *target == node)
                    .map(|(_, count)| *count)
                    .sum::<usize>();
                (connections, dispatches)
            };
            if surviving_connections == 0 && earlier_dispatches == 0 {
                return CutAck {
                    generation,
                    blocked_node: node,
                    surviving_connections,
                    earlier_dispatches,
                    blocked_edges: self
                        .bindings
                        .keys()
                        .filter(|peer| **peer != node)
                        .flat_map(|peer| [(node, *peer), (*peer, node)])
                        .collect(),
                };
            }
            changed.await;
        }
    }

    pub(crate) fn heal(&self) {
        let mut state = self.shared.state.lock().unwrap();
        state.generation += 1;
        state.blocked.clear();
        state.blocked_edges.clear();
        state.blocked_groups.clear();
        drop(state);
        self.shared.changed.notify_waiters();
    }

    pub(crate) async fn close(&mut self) {
        let _ = self.shutdown.send(true);
        // Retain every handle while awaiting: cancelled cleanup can be resumed.
        while let Some(task) = self.tasks.last_mut() {
            let _ = task.await;
            self.tasks.pop();
        }
    }

    pub(super) async fn wait_connections(&self, minimum: usize) {
        loop {
            let changed = self.shared.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.shared.state.lock().unwrap().connections.len() >= minimum {
                return;
            }
            changed.await;
        }
    }
}

fn affected(connection: &Connection, node: NodeId, generation: u64) -> bool {
    connection.target == node
        || connection.source == Some(node)
        || (connection.source.is_none() && connection.accepted_generation < generation)
}

fn affected_edges(connection: &Connection, installed: &EdgeCutInstall) -> bool {
    match connection.source {
        Some(source) => installed
            .blocked_edges
            .contains(&(source, connection.target)),
        None => connection.accepted_generation < installed.generation,
    }
}

impl Drop for PeerRelays {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
    }
}

async fn accept_loop(
    listener: TcpListener,
    private: SocketAddr,
    identity: PeerIdentity,
    limits: PeerLimits,
    shared: Arc<Shared>,
    mut shutdown: watch::Receiver<bool>,
) {
    let permits = Arc::new(Semaphore::new(limits.max_connections));
    let mut tasks = JoinSet::new();
    let mut ids = HashMap::new();
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { break; }
            }
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else { break; };
                // Include finished-but-not-yet-joined handles in the bound.
                // Their permits may already be released, but the owner still
                // retains their JoinSet slots and registry entries.
                if tasks.len() >= limits.max_connections { continue; }
                let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else { continue; };
                let mut state = shared.state.lock().unwrap();
                if state.blocked.contains(&identity.node_id) { continue; }
                let id = state.next_connection;
                state.next_connection += 1;
                let accepted_generation = state.generation;
                let child_shared = Arc::clone(&shared);
                let task = tasks.spawn(async move {
                    let _permit = permit;
                    let _ = forward(stream, private, identity, limits, id, child_shared).await;
                });
                ids.insert(task.id(), id);
                state.connections.insert(id, Connection {
                    source: None, target: identity.node_id, accepted_generation, abort: task,
                });
                shared.changed.notify_waiters();
            }
            Some(result) = tasks.join_next_with_id(), if !tasks.is_empty() => {
                let task_id = match result { Ok((id, ())) => id, Err(error) => error.id() };
                if let Some(id) = ids.remove(&task_id) {
                    shared.state.lock().unwrap().connections.remove(&id);
                    shared.changed.notify_waiters();
                }
            }
        }
    }
    tasks.abort_all();
    while let Some(result) = tasks.join_next_with_id().await {
        let task_id = match result {
            Ok((id, ())) => id,
            Err(error) => error.id(),
        };
        if let Some(id) = ids.remove(&task_id) {
            shared.state.lock().unwrap().connections.remove(&id);
        }
    }
    shared.changed.notify_waiters();
}

async fn forward(
    mut incoming: TcpStream,
    private: SocketAddr,
    identity: PeerIdentity,
    limits: PeerLimits,
    id: u64,
    shared: Arc<Shared>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Keep and forward the original bytes, including the original length prefix.
    let mut prefix = [0_u8; 4 + PEER_HEADER_BYTES];
    timeout(Duration::from_secs(10), incoming.read_exact(&mut prefix)).await??;
    let frame_len = usize::try_from(u32::from_be_bytes(prefix[..4].try_into()?))?;
    if !(PEER_HEADER_BYTES..=limits.max_frame_bytes).contains(&frame_len) {
        return Err("invalid peer frame length".into());
    }
    let header = PeerHeader::decode_fixed(prefix[4..].try_into()?)?;
    header.validate_for_receiver(identity.cluster_id, identity.node_id)?;
    {
        let mut state = shared.state.lock().unwrap();
        let connection = state
            .connections
            .get_mut(&id)
            .ok_or("unregistered relay task")?;
        connection.source = Some(header.source);
        if state.blocked.contains(&header.source)
            || state.blocked.contains(&header.target)
            || state
                .blocked_edges
                .contains(&(header.source, header.target))
        {
            return Err("fixture peer edge is cut".into());
        }
    }
    let mut outgoing = timeout(Duration::from_secs(10), TcpStream::connect(private)).await??;
    outgoing.write_all(&prefix).await?;
    copy_bidirectional(&mut incoming, &mut outgoing).await?;
    Ok(())
}
