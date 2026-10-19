//! Process-wide admission limits for private Raft traffic.
use std::{collections::VecDeque, io::ErrorKind, sync::Arc, time::Duration};

use rockbee_raft_protocol::{ClusterId, NodeId};
use tokio::{
    net::TcpStream,
    sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore},
    time::Instant,
};

use super::{PeerError, PeerIdentity};

const CONTROL_IDLE_CONNECTION_TTL: Duration = Duration::from_secs(5);

/// Limits shared by every Raft group hosted in one node process.
///
/// These are admission limits, not memory guarantees. A permit is retained from
/// acceptance until the request and its response are finished, so a slow snapshot
/// cannot create unbounded detached work.
#[derive(Debug, Clone, Copy)]
pub struct PeerLimits {
    /// Maximum accepted connections in each direction.
    pub max_connections: usize,
    /// Maximum vote or append operations in each direction.
    pub max_control_operations: usize,
    /// Maximum snapshot operations in each direction.
    pub max_snapshot_operations: usize,
    /// Maximum private frame payload, excluding the four-byte TCP length.
    pub max_frame_bytes: usize,
    /// Maximum bytes carried by one snapshot chunk.
    pub max_snapshot_chunk_bytes: usize,
}

impl Default for PeerLimits {
    fn default() -> Self {
        Self {
            max_connections: 128,
            max_control_operations: 64,
            max_snapshot_operations: 8,
            max_frame_bytes: rockbee_raft_protocol::MAX_FRAME_PAYLOAD,
            max_snapshot_chunk_bytes: 256 * 1024,
        }
    }
}

impl PeerLimits {
    /// Checks construction-time limits before any listener accepts work.
    pub fn validate(self) -> Result<Self, PeerError> {
        for (name, value) in [
            ("max_connections", self.max_connections),
            ("max_control_operations", self.max_control_operations),
            ("max_snapshot_operations", self.max_snapshot_operations),
            ("max_frame_bytes", self.max_frame_bytes),
            ("max_snapshot_chunk_bytes", self.max_snapshot_chunk_bytes),
        ] {
            if value == 0 {
                return Err(PeerError::InvalidLimit { name });
            }
        }
        if self.max_frame_bytes > rockbee_raft_protocol::MAX_FRAME_PAYLOAD {
            return Err(PeerError::FrameTooLarge {
                actual: self.max_frame_bytes,
                maximum: rockbee_raft_protocol::MAX_FRAME_PAYLOAD,
            });
        }
        if self.max_snapshot_chunk_bytes > self.max_frame_bytes {
            return Err(PeerError::FrameTooLarge {
                actual: self.max_snapshot_chunk_bytes,
                maximum: self.max_frame_bytes,
            });
        }
        if self.max_snapshot_chunk_bytes > 256 * 1024 {
            return Err(PeerError::FrameTooLarge {
                actual: self.max_snapshot_chunk_bytes,
                maximum: 256 * 1024,
            });
        }
        // Every active snapshot owns one generic connection slot. Reserve at
        // least one slot for control traffic even when the bulk lane is full.
        if self.max_snapshot_operations >= self.max_connections {
            return Err(PeerError::InvalidLimit {
                name: "max_snapshot_operations",
            });
        }
        for (name, value) in [
            ("max_connections", self.max_connections),
            ("max_control_operations", self.max_control_operations),
            ("max_snapshot_operations", self.max_snapshot_operations),
        ] {
            if value > Semaphore::MAX_PERMITS {
                return Err(PeerError::InvalidLimit { name });
            }
        }
        Ok(self)
    }
}

/// Shared permit pools. Control RPCs and snapshot RPCs use different pools so a
/// large snapshot cannot consume all capacity needed for elections and heartbeats.
#[derive(Debug)]
pub struct PeerResources {
    limits: PeerLimits,
    incoming_connections: Arc<Semaphore>,
    incoming_control_connections: Arc<Semaphore>,
    outgoing_connections: Arc<Semaphore>,
    incoming_control: Arc<Semaphore>,
    outgoing_control: Arc<Semaphore>,
    incoming_snapshots: Arc<Semaphore>,
    outgoing_snapshots: Arc<Semaphore>,
    idle_control_connections: Mutex<VecDeque<IdleControlConnection>>,
    idle_control_changed: Notify,
}

impl PeerResources {
    /// Creates the single resource owner shared by all local groups and clients.
    pub fn new(limits: PeerLimits) -> Result<Self, PeerError> {
        let limits = limits.validate()?;
        Ok(Self {
            incoming_connections: Arc::new(Semaphore::new(limits.max_connections)),
            incoming_control_connections: Arc::new(Semaphore::new(
                limits.max_connections - limits.max_snapshot_operations,
            )),
            outgoing_connections: Arc::new(Semaphore::new(limits.max_connections)),
            incoming_control: Arc::new(Semaphore::new(limits.max_control_operations)),
            outgoing_control: Arc::new(Semaphore::new(limits.max_control_operations)),
            incoming_snapshots: Arc::new(Semaphore::new(limits.max_snapshot_operations)),
            outgoing_snapshots: Arc::new(Semaphore::new(limits.max_snapshot_operations)),
            idle_control_connections: Mutex::new(VecDeque::new()),
            idle_control_changed: Notify::new(),
            limits,
        })
    }

    pub fn limits(&self) -> PeerLimits {
        self.limits
    }

    pub(crate) async fn acquire_outgoing_connection(
        &self,
    ) -> Result<OwnedSemaphorePermit, PeerError> {
        loop {
            let notified = self.idle_control_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.outgoing_connections.clone().try_acquire_owned() {
                Ok(permit) => return Ok(permit),
                Err(_) => {
                    let evicted = self.idle_control_connections.lock().await.pop_front();
                    if evicted.is_some() {
                        self.idle_control_changed.notify_waiters();
                        continue;
                    }
                }
            }
            tokio::select! {
                permit = self.outgoing_connections.clone().acquire_owned() => {
                    return permit.map_err(|_| PeerError::Protocol("connection admission closed".into()));
                }
                _ = &mut notified => {}
            }
        }
    }

    /// Checks out a process-shared control stream or reserves one physical
    /// connection slot for a new stream. Snapshot traffic never uses this pool.
    pub(crate) async fn checkout_outgoing_control(
        &self,
        key: ControlConnectionKey,
    ) -> Result<ControlCheckout, PeerError> {
        loop {
            let notified = self.idle_control_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let evicted = {
                let mut idle = self.idle_control_connections.lock().await;
                discard_stale_connections(&mut idle);
                if let Some(index) = idle.iter().position(|connection| connection.key == key) {
                    return Ok(ControlCheckout::Reused(
                        idle.remove(index).expect("idle connection index is valid"),
                    ));
                }
                match self.outgoing_connections.clone().try_acquire_owned() {
                    Ok(permit) => return Ok(ControlCheckout::Fresh(permit)),
                    Err(_) => idle.pop_front(),
                }
            };
            if evicted.is_some() {
                self.idle_control_changed.notify_waiters();
                continue;
            }
            tokio::select! {
                permit = self.outgoing_connections.clone().acquire_owned() => {
                    let permit = permit.map_err(|_| PeerError::Protocol("connection admission closed".into()))?;
                    return Ok(ControlCheckout::Fresh(permit));
                }
                _ = &mut notified => {}
            }
        }
    }

    /// Returns a fully validated control response stream to the bounded idle pool.
    /// A full pool closes the newest stream so each retained entry keeps its own
    /// physical admission permit and the total remains bounded.
    pub(crate) async fn return_outgoing_control(&self, mut connection: IdleControlConnection) {
        let mut idle = self.idle_control_connections.lock().await;
        if idle.len() < self.limits.max_connections {
            connection.returned_at = Instant::now();
            idle.push_back(connection);
            drop(idle);
            self.idle_control_changed.notify_waiters();
        }
    }

    /// Attempts server-side admission without queueing accepted sockets in user space.
    pub(crate) fn try_acquire_incoming_connection(
        &self,
    ) -> Result<OwnedSemaphorePermit, PeerError> {
        self.incoming_connections
            .clone()
            .try_acquire_owned()
            .map_err(|_| PeerError::Protocol("connection admission is full or closed".into()))
    }

    /// Reserves a validated control socket for its full lifetime. The reserved
    /// snapshot capacity remains available even when control streams are idle.
    pub(crate) fn try_acquire_incoming_control_connection(
        &self,
    ) -> Result<OwnedSemaphorePermit, PeerError> {
        self.incoming_control_connections
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                PeerError::Protocol("incoming control connection admission is full".into())
            })
    }

    pub(crate) async fn acquire_outgoing_control(&self) -> Result<OwnedSemaphorePermit, PeerError> {
        self.outgoing_control
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| PeerError::Protocol("control admission closed".into()))
    }

    pub(crate) async fn acquire_outgoing_snapshot(
        &self,
    ) -> Result<OwnedSemaphorePermit, PeerError> {
        self.outgoing_snapshots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| PeerError::Protocol("snapshot admission closed".into()))
    }

    pub(crate) async fn acquire_incoming_control(&self) -> Result<OwnedSemaphorePermit, PeerError> {
        self.incoming_control
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| PeerError::Protocol("incoming control admission closed".into()))
    }

    /// Rejects excess bulk work instead of letting it wait while retaining an
    /// accepted TCP connection that control traffic needs to enter.
    pub(crate) fn try_acquire_incoming_snapshot(&self) -> Result<OwnedSemaphorePermit, PeerError> {
        self.incoming_snapshots
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                PeerError::Protocol("incoming snapshot admission is full or closed".into())
            })
    }
}

/// Identifies one reusable control stream. Groups are deliberately absent: a
/// physical peer connection may serve sequential requests for different groups.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ControlConnectionKey {
    cluster_id: ClusterId,
    source: NodeId,
    target: NodeId,
    address: String,
}

impl ControlConnectionKey {
    pub(crate) fn new(identity: PeerIdentity, target: NodeId, address: String) -> Self {
        Self {
            cluster_id: identity.cluster_id,
            source: identity.node_id,
            target,
            address,
        }
    }
}

/// An exclusive, validated control stream plus its physical admission permit.
/// Dropping this value closes the stream and releases the permit.
#[derive(Debug)]
pub(crate) struct IdleControlConnection {
    key: ControlConnectionKey,
    stream: TcpStream,
    _permit: OwnedSemaphorePermit,
    returned_at: Instant,
}

impl IdleControlConnection {
    pub(crate) fn new(
        key: ControlConnectionKey,
        stream: TcpStream,
        permit: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            key,
            stream,
            _permit: permit,
            returned_at: Instant::now(),
        }
    }

    pub(crate) fn stream_mut(&mut self) -> &mut TcpStream {
        &mut self.stream
    }
}

/// Either an idle control stream or a permit that must be paired with one new stream.
pub(crate) enum ControlCheckout {
    Reused(IdleControlConnection),
    Fresh(OwnedSemaphorePermit),
}

fn discard_stale_connections(connections: &mut VecDeque<IdleControlConnection>) {
    connections.retain(can_reuse_control_connection);
}

fn can_reuse_control_connection(connection: &IdleControlConnection) -> bool {
    if connection.returned_at.elapsed() >= CONTROL_IDLE_CONNECTION_TTL {
        return false;
    }
    let mut byte = [0_u8; 1];
    match connection.stream.try_read(&mut byte) {
        Err(error) if error.kind() == ErrorKind::WouldBlock => true,
        Ok(_) | Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_zero_and_oversized_limits() {
        assert!(matches!(
            PeerResources::new(PeerLimits {
                max_connections: 0,
                ..PeerLimits::default()
            }),
            Err(PeerError::InvalidLimit {
                name: "max_connections"
            })
        ));
        assert!(matches!(
            PeerResources::new(PeerLimits {
                max_frame_bytes: rockbee_raft_protocol::MAX_FRAME_PAYLOAD + 1,
                ..PeerLimits::default()
            }),
            Err(PeerError::FrameTooLarge { .. })
        ));
        assert!(matches!(
            PeerResources::new(PeerLimits {
                max_connections: 1,
                max_snapshot_operations: 1,
                ..PeerLimits::default()
            }),
            Err(PeerError::InvalidLimit {
                name: "max_snapshot_operations"
            })
        ));
    }

    #[tokio::test]
    async fn saturated_snapshot_lane_cannot_consume_control_connection_admission() {
        let resources = PeerResources::new(PeerLimits {
            max_connections: 2,
            max_control_operations: 1,
            max_snapshot_operations: 1,
            ..PeerLimits::default()
        })
        .expect("valid test limits");
        let _active_snapshot = resources
            .try_acquire_incoming_snapshot()
            .expect("first snapshot owns the only bulk lane");
        assert!(resources.try_acquire_incoming_snapshot().is_err());
        let _control_connection = resources
            .try_acquire_incoming_connection()
            .expect("rejected bulk waiter did not consume a connection slot");
        let _control_operation = resources
            .acquire_incoming_control()
            .await
            .expect("control lane remains available while bulk is saturated");
    }
}
