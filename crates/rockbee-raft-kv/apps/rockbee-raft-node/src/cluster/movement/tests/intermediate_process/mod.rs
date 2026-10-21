//! One B NodeRuntime OS-process death at its actual durable joint apply return.
//! A/C/D stay in this test process. This is neither four CLI deaths nor power loss.
#![allow(clippy::unwrap_used, clippy::panic)]
use super::super::*;
use crate::{cluster::NodeRuntime, types::StoredMembership};
use bytes::Bytes;
use rockbee_raft_protocol::{
    ClusterId, ShardConfig,
    routing::{ClusterDirectory, NodeEndpoint},
};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tokio::time::{sleep, timeout_at};

mod child;
mod scenario;
mod wire;

const CHILD_FILTER: &str =
    "cluster::movement::tests::intermediate_process::child::joint_process_child";
const MARKER: &str = "ROCKBEE_PRIVATE_JOINT_PROCESS_CONFIG";
const GROUP: GroupId = GroupId::new(1);

fn joint_configs() -> Vec<BTreeSet<u64>> {
    vec![BTreeSet::from([1, 2, 3]), BTreeSet::from([1, 2, 4])]
}
fn native(position: ObservedLogPosition) -> LogId {
    LogId::new(
        openraft::CommittedLeaderId::new(position.term, position.leader_node_id),
        position.index,
    )
}
fn require_joint(
    applied: Option<LogId>,
    membership: &StoredMembership,
    endpoints: &[NodeEndpoint],
    baseline: LogId,
) {
    assert_eq!(membership.membership().get_joint_config(), &joint_configs());
    assert!(covers(applied, *membership.log_id()));
    assert!(covers(applied, Some(baseline)));
    assert_eq!(membership.nodes().count(), 4);
    for endpoint in endpoints {
        assert_eq!(
            membership
                .nodes()
                .find(|(id, _)| **id == endpoint.node_id.get())
                .unwrap()
                .1
                .addr,
            endpoint.peer_address.to_string()
        );
    }
}
fn require_local_target(
    applied: Option<LogId>,
    membership: &StoredMembership,
    config: &wire::Config,
    published: LogId,
    baseline: LogId,
) {
    assert_eq!(
        membership.membership().get_joint_config(),
        &vec![BTreeSet::from([1, 2, 4])]
    );
    assert!(covers(applied, *membership.log_id()));
    assert!(covers(applied, Some(published)) && covers(applied, Some(baseline)));
    assert!(covers(*membership.log_id(), Some(published)));
    assert_eq!(membership.nodes().count(), 3);
    for endpoint in &config.endpoints {
        let actual = membership
            .nodes()
            .find(|(id, _)| **id == endpoint.node_id.get())
            .map(|(_, node)| node.addr.as_str());
        let address = endpoint.peer_address.to_string();
        assert_eq!(
            actual,
            (endpoint.node_id.get() != 3).then_some(address.as_str())
        );
    }
}
