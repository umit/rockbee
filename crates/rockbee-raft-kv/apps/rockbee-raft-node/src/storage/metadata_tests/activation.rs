//! These synthetic witnesses test atomic metadata storage, not a live handoff.
use super::successor::{prepare, raw_job, seed_completed, successor};
use super::*;
use crate::control::{HandoffFenceReceipt, HandoffProof, SealReceipt, SuccessorState};
use rockbee_raft_protocol::backend::{AppliedMembership, ObservedLogPosition};

fn synthetic_handoff(first: &MoveRecord, next: &MoveIntent) -> HandoffProof {
    let committed = CommittedLogPosition {
        term: 7,
        leader_node_id: NodeId::new(1).unwrap(),
        index: 21,
    };
    let mut peers = [None; 16];
    // Production change_membership(target, false) removes former voters.
    for (slot, id) in first.intent.target_voters.iter().enumerate() {
        peers[slot] = Some((*id, endpoint(id.get()).peer_address));
    }

    HandoffProof {
        seals: std::array::from_fn(|index| SealReceipt {
            endpoint: endpoint(u64::try_from(index + 1).unwrap()),
            data_group: first.intent.data_group,
            predecessor_revision: first.intent.expected_directory_revision,
            predecessor_job_id: first.intent.job_id,
        }),
        fence: HandoffFenceReceipt {
            data_group: first.intent.data_group,
            predecessor_revision: first.intent.expected_directory_revision,
            predecessor_job_id: first.intent.job_id,
            successor_job_id: next.job_id,
            committed,
        },
        observed: AppliedMembership {
            data_group: first.intent.data_group,
            applied: ObservedLogPosition {
                term: 7,
                leader_node_id: 1,
                index: 21,
            },
            membership: ObservedLogPosition {
                term: 7,
                leader_node_id: 1,
                index: 19,
            },
            voters: first.intent.target_voters,
            joint_voters: None,
            peers,
        },
    }
}

fn activation(first: &MoveRecord, next: &MoveIntent, proof: &HandoffProof) -> MetadataCommand {
    MetadataCommand::ActivateSuccessor {
        expected_directory_revision: 2,
        data_group: first.intent.data_group,
        predecessor_job_id: first.intent.job_id,
        successor_job_id: next.job_id,
        proof: Box::new(proof.clone()),
    }
}

async fn seed_prepared(stores: &mut GroupStores) -> (MoveRecord, MoveIntent, HandoffProof) {
    let first = seed_completed(stores).await;
    let state = stores.state_machine.reader().read_metadata().await.unwrap();
    let next = successor(&first, state.view.directory().unwrap());
    let proof = synthetic_handoff(&first, &next);
    let result = stores
        .state_machine
        .apply([metadata(4, prepare(&first, &next))])
        .await
        .unwrap();
    assert_eq!(
        result,
        vec![ApplyResult::Metadata(MetadataResult::Prepared {
            data_group: next.data_group,
            job_id: next.job_id,
        })]
    );
    (first, next, proof)
}

fn completion(record: &MoveIntent, index: u64, revision: u64) -> MetadataCommand {
    MetadataCommand::CompleteMove {
        expected_directory_revision: revision,
        data_group: record.data_group,
        job_id: record.job_id,
        observation: MembershipObservation {
            uniform: true,
            voters: record.target_voters,
            replacement_endpoint: record.add_node.clone(),
            membership: CommittedLogPosition {
                term: 7,
                leader_node_id: NodeId::new(1).unwrap(),
                index,
            },
        },
    }
}

#[tokio::test]
async fn successor_activation_pending_survives_reopen_and_snapshot() {
    roundtrip(false).await;
}

#[tokio::test]
async fn successor_activation_completed_survives_reopen_and_snapshot() {
    roundtrip(true).await;
}

async fn roundtrip(complete_successor: bool) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let storage = open(path.clone(), OpenMode::CreateNew, StorageOptions::default()).await;
    let mut stores = storage.open_group(GroupId::new(0)).unwrap();
    let (first, next, proof) = seed_prepared(&mut stores).await;
    let before = stores.state_machine.reader().read_metadata().await.unwrap();
    let command = activation(&first, &next, &proof);
    let results = stores
        .state_machine
        .apply([metadata(5, command.clone()), metadata(6, command.clone())])
        .await
        .unwrap();
    assert_eq!(
        results,
        vec![
            ApplyResult::Metadata(MetadataResult::Activated {
                data_group: next.data_group,
                job_id: next.job_id
            }),
            ApplyResult::Metadata(MetadataResult::AlreadyActivated {
                data_group: next.data_group,
                job_id: next.job_id
            }),
        ]
    );
    let activated = stores.state_machine.reader().read_metadata().await.unwrap();
    assert_eq!(activated.view.directory(), before.view.directory());
    assert_eq!(activated.view.origin(), before.view.origin());
    assert_eq!(activated.view.first_job(next.data_group), Some(&first));
    assert_eq!(
        activated.view.job(next.data_group),
        Some(&MoveRecord {
            intent: next.clone(),
            progress: MoveProgress::Pending
        })
    );
    assert!(activated.view.prepared_successor(next.data_group).is_none());
    assert_eq!(activated.applied, Some(metadata(6, command.clone()).log_id));
    let mut last_index = 6;
    if complete_successor {
        let completed = stores
            .state_machine
            .apply([metadata(7, completion(&next, 30, 2))])
            .await
            .unwrap();
        assert_eq!(
            completed,
            vec![ApplyResult::Metadata(MetadataResult::Completed {
                data_group: next.data_group,
                job_id: next.job_id,
                revision: 3,
            })]
        );
        last_index = 7;
    }
    let completed_view = stores.state_machine.reader().read_metadata().await.unwrap();
    let mut expected_directory = before.view.directory().unwrap().clone();
    let expected_progress = if complete_successor {
        expected_directory.revision = 3;
        expected_directory
            .placements
            .iter_mut()
            .find(|placement| placement.group_id == next.data_group)
            .unwrap()
            .voters = next.target_voters.to_vec();
        MoveProgress::Completed {
            membership: CommittedLogPosition {
                term: 7,
                leader_node_id: NodeId::new(1).unwrap(),
                index: 30,
            },
        }
    } else {
        MoveProgress::Pending
    };
    assert_eq!(completed_view.view.directory(), Some(&expected_directory));
    assert_eq!(
        completed_view.view.job(next.data_group),
        Some(&MoveRecord {
            intent: next.clone(),
            progress: expected_progress,
        })
    );
    assert_eq!(completed_view.view.origin(), before.view.origin());
    let current = completed_view.view.directory().unwrap();
    let third = MoveReplica {
        job_id: MoveJobId::new([23; 16]).unwrap(),
        data_group: next.data_group,
        remove_node: NodeId::new(1).unwrap(),
        add_node: endpoint(5),
        expected_directory_revision: current.revision,
    }
    .derive_intent(current)
    .unwrap();
    let retry = stores
        .state_machine
        .apply([
            metadata(last_index + 1, MetadataCommand::BeginMove { intent: third }),
            metadata(last_index + 2, completion(&first.intent, 999, 1)),
        ])
        .await
        .unwrap();
    assert_eq!(
        retry,
        vec![
            ApplyResult::Metadata(MetadataResult::Conflict {
                reason: ConflictReason::ExistingMove
            }),
            ApplyResult::Metadata(MetadataResult::AlreadyCompleted {
                data_group: next.data_group,
                job_id: first.intent.job_id,
                revision: current.revision,
            }),
        ]
    );
    last_index += 2;
    let after = stores.state_machine.reader().read_metadata().await.unwrap();
    assert_eq!(after.view, completed_view.view);
    assert_eq!(
        after.applied,
        Some(metadata(last_index, command.clone()).log_id)
    );
    assert_eq!(after.view.first_job(next.data_group), Some(&first));
    let raw = raw_job(&stores, next.data_group).await;
    let decoded = control::decode_stored_moves(&raw).unwrap();
    let Some(SuccessorState::Accepted(accepted)) = decoded.successor else {
        panic!("accepted successor must be retained")
    };
    assert_eq!(decoded.first, first);
    assert_eq!(accepted.record, *after.view.job(next.data_group).unwrap());
    assert_eq!(accepted.handoff, proof);
    assert_eq!(raw[20], 3, "activation writes accepted-only v3");
    assert!(rockbee_raft_protocol::decode_frame::<MoveRecord>(&raw).is_err());
    assert_eq!(
        read(&stores, b"ordinary-metadata-value").await,
        Some(Bytes::from_static(b"retained"))
    );
    drop(stores);
    drop(storage);

    let reopened = open(path, OpenMode::OpenExisting, StorageOptions::default()).await;
    let mut stores = reopened.open_group(GroupId::new(0)).unwrap();
    let restored = stores.state_machine.reader().read_metadata().await.unwrap();
    assert_eq!(restored.view, after.view);
    assert_eq!(restored.applied, after.applied);
    assert_eq!(raw_job(&stores, next.data_group).await, raw);
    let mut image = stores
        .state_machine
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();
    let target_root = tempfile::tempdir().unwrap();
    let target_path = target_root.path().join("db");
    let target = open(
        target_path.clone(),
        OpenMode::CreateNew,
        StorageOptions::default(),
    )
    .await;
    let mut target_group = target.open_group(GroupId::new(0)).unwrap();
    let mut sibling = target.open_group(GroupId::new(2)).unwrap();
    sibling
        .state_machine
        .apply([entry(
            0,
            Command::Put {
                key: Bytes::from_static(b"sibling"),
                value: Bytes::from_static(b"untouched"),
            },
        )])
        .await
        .unwrap();
    let mut receive = target_group
        .state_machine
        .begin_receiving_snapshot()
        .await
        .unwrap();
    image.snapshot.rewind().await.unwrap();
    tokio::io::copy(&mut image.snapshot, &mut receive)
        .await
        .unwrap();
    receive.flush().await.unwrap();
    target_group
        .state_machine
        .install_snapshot(&image.meta, receive)
        .await
        .unwrap();
    drop(target_group);
    drop(sibling);
    drop(target);

    let target = open(
        target_path,
        OpenMode::OpenExisting,
        StorageOptions::default(),
    )
    .await;
    let target_group = target.open_group(GroupId::new(0)).unwrap();
    let sibling = target.open_group(GroupId::new(2)).unwrap();
    let installed = target_group
        .state_machine
        .reader()
        .read_metadata()
        .await
        .unwrap();
    assert_eq!(installed.view, after.view);
    assert_eq!(installed.applied, after.applied);
    assert_eq!(raw_job(&target_group, next.data_group).await, raw);
    assert_eq!(
        read(&target_group, b"ordinary-metadata-value").await,
        Some(Bytes::from_static(b"retained"))
    );
    assert_eq!(
        read(&sibling, b"sibling").await,
        Some(Bytes::from_static(b"untouched"))
    );
}

#[tokio::test]
async fn successor_activation_failed_write_keeps_preparing_bytes_and_applied_position() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let storage = open(path.clone(), OpenMode::CreateNew, StorageOptions::default()).await;
    let mut stores = storage.open_group(GroupId::new(0)).unwrap();
    let (first, next, proof) = seed_prepared(&mut stores).await;
    let before = stores.state_machine.reader().read_metadata().await.unwrap();
    let raw = raw_job(&stores, next.data_group).await;
    assert_eq!(raw[20], 2);
    stores
        .state_machine
        .group
        .test_writes
        .lock()
        .unwrap()
        .push_back(TestWriteAction::Fail);
    let command = activation(&first, &next, &proof);
    assert!(
        stores
            .state_machine
            .apply([metadata(5, command.clone())])
            .await
            .is_err()
    );
    let failed = stores.state_machine.reader().read_metadata().await.unwrap();
    assert_eq!(failed.view, before.view);
    assert_eq!(failed.applied, before.applied);
    assert_eq!(raw_job(&stores, next.data_group).await, raw);
    let mut sibling = storage.open_group(GroupId::new(2)).unwrap();
    sibling
        .state_machine
        .apply([entry(
            0,
            Command::Put {
                key: Bytes::from_static(b"sibling"),
                value: Bytes::from_static(b"healthy"),
            },
        )])
        .await
        .unwrap();
    drop(sibling);
    drop(stores);
    drop(storage);

    let storage = open(path, OpenMode::OpenExisting, StorageOptions::default()).await;
    let mut stores = storage.open_group(GroupId::new(0)).unwrap();
    let restored = stores.state_machine.reader().read_metadata().await.unwrap();
    assert_eq!(restored.view, before.view);
    assert_eq!(restored.applied, before.applied);
    assert_eq!(raw_job(&stores, next.data_group).await, raw);
    assert_eq!(
        stores
            .state_machine
            .apply([metadata(5, command)])
            .await
            .unwrap(),
        vec![ApplyResult::Metadata(MetadataResult::Activated {
            data_group: next.data_group,
            job_id: next.job_id,
        })]
    );
    assert_eq!(raw_job(&stores, next.data_group).await[20], 3);
    let sibling = storage.open_group(GroupId::new(2)).unwrap();
    assert_eq!(
        read(&sibling, b"sibling").await,
        Some(Bytes::from_static(b"healthy"))
    );
}
