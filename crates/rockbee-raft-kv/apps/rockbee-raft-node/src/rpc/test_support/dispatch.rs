//! Synchronous receiver admission seal and ownership of invoked native RPCs.
#![allow(clippy::unwrap_used)]
use std::{collections::BTreeSet, sync::Arc};

use rockbee_raft_protocol::{GroupId, NodeId};

use super::relay::Shared;
use crate::rpc::{PeerError, wire::message::PeerHeader};

#[derive(Clone)]
pub(crate) struct DispatchTracker {
    shared: Arc<Shared>,
}

/// Exact receiver admissions, including native owners retained after a cut.
/// Totals survive healing; rejection never creates an active native owner.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct GroupDispatchCounts {
    pub(crate) active: usize,
    pub(crate) admitted: u64,
    pub(crate) rejected: u64,
}

impl DispatchTracker {
    pub(super) fn new(shared: Arc<Shared>) -> Self {
        Self { shared }
    }

    /// Call after decoding, immediately before invoking the native Raft future.
    /// The same mutex installs the cut seal and registers this admission.
    pub(crate) fn try_admit(&self, header: &PeerHeader) -> Result<DispatchGuard, PeerError> {
        let edge = (header.source, header.target);
        let group_edge = (header.group_id, header.source, header.target);
        let mut state = self.shared.state.lock().unwrap();
        if state.blocked.contains(&edge.0)
            || state.blocked.contains(&edge.1)
            || state.blocked_edges.contains(&edge)
            || state.blocked_groups.contains(&header.group_id)
        {
            state
                .group_dispatches
                .entry(group_edge)
                .or_default()
                .rejected += 1;
            return Err(PeerError::Protocol(
                "fixture peer dispatch edge or group is cut".into(),
            ));
        }
        *state.dispatches.entry(edge).or_default() += 1;
        *state.admitted.entry(edge).or_default() += 1;
        let counts = state.group_dispatches.entry(group_edge).or_default();
        counts.active += 1;
        counts.admitted += 1;
        Ok(DispatchGuard {
            shared: Arc::clone(&self.shared),
            edge,
            group_id: header.group_id,
        })
    }

    pub(crate) fn group_counts(&self, group: GroupId) -> GroupDispatchCounts {
        let state = self.shared.state.lock().unwrap();
        state
            .group_dispatches
            .iter()
            .filter(|((id, _, _), _)| *id == group)
            .fold(GroupDispatchCounts::default(), |mut total, (_, counts)| {
                total.active += counts.active;
                total.admitted += counts.admitted;
                total.rejected += counts.rejected;
                total
            })
    }

    pub(crate) fn group_edge_counts(
        &self,
        group: GroupId,
        source: NodeId,
        target: NodeId,
    ) -> GroupDispatchCounts {
        self.shared
            .state
            .lock()
            .unwrap()
            .group_dispatches
            .get(&(group, source, target))
            .copied()
            .unwrap_or_default()
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
    group_id: GroupId,
}

impl Drop for DispatchGuard {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock().unwrap();
        let count = state.dispatches.get_mut(&self.edge).unwrap();
        *count -= 1;
        if *count == 0 {
            state.dispatches.remove(&self.edge);
        }
        let counts = state
            .group_dispatches
            .get_mut(&(self.group_id, self.edge.0, self.edge.1))
            .expect("native owner has a registered group admission");
        counts.active -= 1;
        self.shared.changed.notify_waiters();
    }
}
