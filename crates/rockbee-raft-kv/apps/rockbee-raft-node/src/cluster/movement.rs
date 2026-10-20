//! Finite execution of one immutable lifetime replica replacement per data group.
//! Local exclusion bounds work; the durable lifetime target makes delayed native
//! OpenRaft membership calls safe. It is not an application epoch fence.
use super::service::{
    HostedGroup, NodeService, authority_barrier, check_applied_covers, check_read_deadline,
    compatible_directory, failure,
};
use crate::{
    config::validate_metadata_candidate,
    control::{ConflictReason, MembershipObservation, MetadataCommand, MetadataResult},
    types::{ApplyResult, Command, LogId},
};
use log::{info, warn};
use openraft::BasicNode;
use rockbee_raft_protocol::{
    FailureReason, GroupId, NodeId, ReadFailure,
    backend::{
        AppliedMembership, BackendOperation, BackendReply, BackendRequest, BackendResponse,
        MoveState, ObservedLogPosition, ReconcileStatus,
    },
    control::{
        AdminResult, CommittedLogPosition, MoveIntent, MoveJobId, MoveProgress, MoveRecord,
        MoveReplica,
    },
    transport::{Link, LinkConfig},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::Semaphore,
    task::JoinHandle,
    time::{Instant, sleep_until},
};

mod group;
mod metadata;
#[cfg(test)]
mod native_membership_observation;
mod observation;
mod runners;
#[cfg(test)]
mod stale_source;
#[cfg(test)]
mod stale_source_tests;
mod transport;

#[cfg(test)]
use observation::position;
use observation::{
    Topology, check_promotion_authority, completion_position, covers, require_target, topology,
};
pub(super) use runners::MoveRunners;
#[cfg(test)]
use runners::RunningMove;

type BackendLink = Link<BackendRequest, BackendResponse>;
const RUN_BUDGET: Duration = Duration::from_secs(30);

fn read_error(reason: FailureReason, message: impl std::fmt::Display) -> ReadFailure {
    ReadFailure(failure(reason, message))
}

#[cfg(test)]
#[derive(Default)]
struct TestPause {
    reached: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
#[cfg(test)]
async fn pause_once(slot: &Mutex<Option<Arc<TestPause>>>) {
    let pause = slot.lock().ok().and_then(|mut slot| slot.take());
    if let Some(pause) = pause {
        pause.reached.notify_one();
        pause.release.notified().await;
    }
}

#[cfg(test)]
mod delayed_executor_tests;
#[cfg(test)]
mod failure_tests;
#[cfg(test)]
mod joint_partition_tests;
#[cfg(test)]
mod tests;
