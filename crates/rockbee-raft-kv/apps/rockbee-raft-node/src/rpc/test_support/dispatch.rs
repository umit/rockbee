//! Synchronous receiver admission seal and ownership of invoked native RPCs.
#![allow(clippy::unwrap_used)]
use std::{collections::BTreeSet, sync::Arc};

use rockbee_raft_protocol::NodeId;

use super::relay::Shared;
use crate::rpc::{PeerError, message::PeerHeader};

#[derive(Clone)]
pub(crate) struct DispatchTracker {
    shared: Arc<Shared>,
}

impl DispatchTracker {
    pub(super) fn new(shared: Arc<Shared>) -> Self {
        Self { shared }
    }

    /// Call after decoding, immediately before invoking the native Raft future.
    /// The same mutex installs the cut seal and registers this admission.
    pub(crate) fn try_admit(&self, header: &PeerHeader) -> Result<DispatchGuard, PeerError> {
        let edge = (header.source, header.target);
        let mut state = self.shared.state.lock().unwrap();
        if state.blocked.contains(&edge.0)
            || state.blocked.contains(&edge.1)
            || state.blocked_edges.contains(&edge)
        {
            return Err(PeerError::Protocol(
                "fixture peer dispatch edge is cut".into(),
            ));
        }
        *state.dispatches.entry(edge).or_default() += 1;
        *state.admitted.entry(edge).or_default() += 1;
        Ok(DispatchGuard {
            shared: Arc::clone(&self.shared),
            edge,
        })
    }

    pub(crate) fn admitted_for(&self, node: NodeId) -> u64 {
        self.shared
            .state
            .lock()
            .unwrap()
            .admitted
            .iter()
            .filter(|((source, target), _)| *source == node || *target == node)
            .map(|(_, count)| *count)
            .sum()
    }

    pub(crate) fn admitted_for_edges(&self, edges: &BTreeSet<(NodeId, NodeId)>) -> u64 {
        self.shared
            .state
            .lock()
            .unwrap()
            .admitted
            .iter()
            .filter(|(edge, _)| edges.contains(*edge))
            .map(|(_, count)| *count)
            .sum()
    }

    pub(super) fn active_for(&self, node: NodeId) -> usize {
        self.shared
            .state
            .lock()
            .unwrap()
            .dispatches
            .iter()
            .filter(|((source, target), _)| *source == node || *target == node)
            .map(|(_, count)| *count)
            .sum()
    }
}

/// The server retains this guard across its already-invoked native await.
/// Socket closure and cut cancellation cannot release it early.
pub(crate) struct DispatchGuard {
    shared: Arc<Shared>,
    edge: (NodeId, NodeId),
}

impl Drop for DispatchGuard {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock().unwrap();
        let count = state.dispatches.get_mut(&self.edge).unwrap();
        *count -= 1;
        if *count == 0 {
            state.dispatches.remove(&self.edge);
        }
        self.shared.changed.notify_waiters();
    }
}
