//! One-shot scheduling hook owned by one actual storage Group instance.
#![allow(clippy::unwrap_used)]
use std::{collections::BTreeSet, sync::Arc};
use tokio::sync::watch;

use super::{Group, state::StateRecord};
use crate::types::{LogId, StoredMembership};

#[derive(Clone, Debug)]
pub(crate) struct JointApplyObservation {
    pub(crate) applied: Option<LogId>,
    pub(crate) membership: StoredMembership,
}

pub(crate) struct JointApplyPause {
    expected: Vec<BTreeSet<u64>>,
    reached: watch::Sender<Option<JointApplyObservation>>,
    release: watch::Sender<bool>,
    exited: watch::Sender<bool>,
}

impl JointApplyPause {
    pub(super) fn new(expected: Vec<BTreeSet<u64>>) -> Self {
        Self {
            expected,
            reached: watch::channel(None).0,
            release: watch::channel(false).0,
            exited: watch::channel(false).0,
        }
    }

    pub(crate) async fn wait_reached(&self) -> JointApplyObservation {
        let mut receiver = self.reached.subscribe();
        loop {
            let observed = receiver.borrow_and_update().clone();
            if let Some(observed) = observed {
                return observed;
            }
            receiver.changed().await.unwrap();
        }
    }

    pub(crate) fn release(&self) {
        self.release.send_replace(true);
    }

    pub(crate) fn held_observation(&self) -> Option<JointApplyObservation> {
        // The owning scenario alone releases this pause. These synchronous
        // reads have no intervening await and cannot manufacture a held pause
        // after that owner has released it.
        if *self.release.borrow() || *self.exited.borrow() {
            return None;
        }
        self.reached.borrow().clone()
    }

    pub(crate) async fn wait_exited(&self) {
        let mut receiver = self.exited.subscribe();
        loop {
            let exited = *receiver.borrow_and_update();
            if exited {
                return;
            }
            receiver.changed().await.unwrap();
        }
    }
}

pub(super) async fn after_apply(group: &Arc<Group>, state: &StateRecord) {
    let pause = {
        let mut slot = group.joint_apply_pause.lock().unwrap();
        if slot.as_ref().is_some_and(|pause| {
            &pause.expected == state.membership.membership().get_joint_config()
        }) {
            slot.take()
        } else {
            None
        }
    };
    let Some(pause) = pause else {
        return;
    };
    // The caller completed its durable WriteSession before entering here.
    // Neither the group write permit nor the slot mutex crosses this await.
    pause.reached.send_replace(Some(JointApplyObservation {
        applied: state.applied,
        membership: state.membership.clone(),
    }));
    let mut release = pause.release.subscribe();
    loop {
        let released = *release.borrow_and_update();
        if released {
            break;
        }
        release.changed().await.unwrap();
    }
    pause.exited.send_replace(true);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{OpenMode, StateMachine, Storage, StorageOptions, tests::identity};
    use crate::types::Entry;
    use openraft::{
        EntryPayload,
        storage::{RaftLogStorage, RaftStateMachine},
    };
    use rockbee_raft_protocol::{GroupId, NodeId};
    use std::time::Duration;
    use tokio::time::timeout;

    fn membership(index: u64, joint: bool) -> Entry {
        let mut configs = vec![BTreeSet::from([1, 2, 3])];
        if joint {
            configs.push(BTreeSet::from([1, 2, 4]));
        }
        Entry {
            log_id: LogId::new(openraft::CommittedLeaderId::new(7, 2), index),
            payload: EntryPayload::Membership(openraft::Membership::new(configs, None)),
        }
    }

    // Keep every apply future in an owned task, even if a faulty hook blocks
    // the wrong instance. Release before joining; never cancel native work.
    async fn apply_unpaused(
        mut machine: StateMachine,
        entry: Entry,
        pause: &JointApplyPause,
    ) -> (StateMachine, bool) {
        let mut task = tokio::spawn(async move {
            let result = machine.apply([entry]).await;
            (machine, result)
        });
        let (result, unpaused) = match timeout(Duration::from_secs(1), &mut task).await {
            Ok(result) => (result.unwrap(), true),
            Err(_) => {
                pause.release();
                (task.await.unwrap(), false)
            }
        };
        assert!(result.1.is_ok());
        (result.0, unpaused)
    }

    #[tokio::test]
    async fn joint_apply_return_pause_releases_storage_session_and_is_instance_scoped_once() {
        let root = tempfile::tempdir().unwrap();
        let storage = Storage::open(
            root.path().join("one"),
            identity(),
            StorageOptions::default(),
            OpenMode::CreateNew,
        )
        .await
        .unwrap();
        let stores = storage.open_group(GroupId::new(1)).unwrap();
        let reader = stores.state_machine.reader();
        let expected = vec![BTreeSet::from([1, 2, 3]), BTreeSet::from([1, 2, 4])];
        let pause = reader.pause_next_joint_apply_return(expected.clone());
        let (machine, uniform_unpaused) =
            apply_unpaused(stores.state_machine, membership(0, false), &pause).await;
        let other = storage.open_group(GroupId::new(2)).unwrap();
        let (other_machine, other_group_unpaused) =
            apply_unpaused(other.state_machine, membership(1, true), &pause).await;
        let mut other_identity = identity();
        other_identity.node_id = NodeId::new(2).unwrap();
        let other_storage = Storage::open(
            root.path().join("two"),
            other_identity,
            StorageOptions::default(),
            OpenMode::CreateNew,
        )
        .await
        .unwrap();
        let other_node = other_storage.open_group(GroupId::new(1)).unwrap();
        let (other_node_machine, other_node_unpaused) =
            apply_unpaused(other_node.state_machine, membership(2, true), &pause).await;
        let mut task = tokio::spawn(async move {
            let mut machine = machine;
            let result = machine.apply([membership(3, true)]).await;
            (machine, result)
        });
        let reached = timeout(Duration::from_secs(2), pause.wait_reached()).await;
        let blocked = !task.is_finished();
        let durable = reader.read_membership().await;
        let mut log = stores.log_store;
        let mut vote_task = tokio::spawn(async move {
            let result = log.save_vote(&openraft::Vote::new(8, 1)).await;
            (log, result)
        });
        let gate_released = match timeout(Duration::from_secs(1), &mut vote_task).await {
            Ok(result) => result.unwrap().1.is_ok(),
            Err(_) => {
                pause.release();
                let _ = vote_task.await.unwrap();
                false
            }
        };
        // Cleanup precedes every pause-related assertion, including RED.
        pause.release();
        let exit = timeout(Duration::from_secs(2), pause.wait_exited()).await;
        let (machine, result) = (&mut task).await.unwrap();
        let (machine, second_unpaused) = apply_unpaused(machine, membership(4, true), &pause).await;
        let reached_again = timeout(Duration::from_millis(100), pause.wait_reached()).await;
        drop((machine, other_machine, other_node_machine));
        assert!(
            reached.is_ok(),
            "exact durable joint must reach the one-shot apply-return pause"
        );
        let reached = reached.unwrap();
        assert!(
            uniform_unpaused && other_group_unpaused && other_node_unpaused,
            "pause must match exact joint and actual node/group instance"
        );
        assert!(blocked, "durable apply must remain owned until release");
        let (applied, membership) = durable.unwrap();
        assert_eq!(applied, reached.applied);
        assert_eq!(membership, reached.membership);
        assert_eq!(membership.membership().get_joint_config(), &expected);
        assert_eq!(*membership.log_id(), Some(membership_entry_id(3)));
        assert!(
            gate_released,
            "session must release the same-group write gate before pausing"
        );
        assert!(
            exit.is_ok() && result.is_ok(),
            "release requires positive apply-return exit acknowledgement"
        );
        assert!(
            second_unpaused,
            "the matching hook must be consumed exactly once"
        );
        assert_eq!(
            reached_again.unwrap().applied,
            reached.applied,
            "later matching apply cannot replace the first reached identity"
        );
    }

    fn membership_entry_id(index: u64) -> LogId {
        LogId::new(openraft::CommittedLeaderId::new(7, 2), index)
    }
}
