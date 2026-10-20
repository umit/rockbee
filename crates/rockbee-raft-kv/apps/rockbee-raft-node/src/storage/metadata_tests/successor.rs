//! Preparation is durable metadata only; it never selects a new native job.
use super::*;

pub(super) async fn seed_completed(stores: &mut GroupStores) -> MoveRecord {
    let first = movement(1, 21, 4);
    let proof = CommittedLogPosition {
        term: 7,
        leader_node_id: NodeId::new(1).unwrap(),
        index: 19,
    };
    stores
        .state_machine
        .apply([
            metadata(0, initialization()),
            metadata(
                1,
                MetadataCommand::BeginMove {
                    intent: first.clone(),
                },
            ),
            metadata(
                2,
                MetadataCommand::CompleteMove {
                    expected_directory_revision: 1,
                    data_group: first.data_group,
                    job_id: first.job_id,
                    observation: MembershipObservation {
                        uniform: true,
                        voters: first.target_voters,
                        replacement_endpoint: first.add_node.clone(),
                        membership: proof,
                    },
                },
            ),
            entry(
                3,
                Command::Put {
                    key: Bytes::from_static(b"ordinary-metadata-value"),
                    value: Bytes::from_static(b"retained"),
                },
            ),
        ])
        .await
        .unwrap();
    MoveRecord {
        intent: first,
        progress: MoveProgress::Completed { membership: proof },
    }
}

pub(super) fn successor(first: &MoveRecord, directory: &ClusterDirectory) -> MoveIntent {
    MoveReplica {
        job_id: MoveJobId::new([22; 16]).unwrap(),
        data_group: first.intent.data_group,
        remove_node: NodeId::new(2).unwrap(),
        add_node: endpoint(3),
        expected_directory_revision: directory.revision,
    }
    .derive_intent(directory)
    .unwrap()
}

pub(super) fn prepare(first: &MoveRecord, intent: &MoveIntent) -> MetadataCommand {
    MetadataCommand::PrepareSuccessor {
        predecessor_job_id: first.intent.job_id,
        intent: intent.clone(),
    }
}

pub(super) async fn raw_job(stores: &GroupStores, group: GroupId) -> Bytes {
    stores
        .state_machine
        .reader()
        .read_local(Bytes::from(control::job_key(group)))
        .await
        .unwrap()
        .value
        .unwrap()
}

#[tokio::test]
async fn successor_preparation_survives_reopen_and_snapshot_without_becoming_runnable() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let storage = open(path.clone(), OpenMode::CreateNew, StorageOptions::default()).await;
    let mut stores = storage.open_group(GroupId::new(0)).unwrap();
    let first = seed_completed(&mut stores).await;
    let before = stores.state_machine.reader().read_metadata().await.unwrap();
    let next = successor(&first, before.view.directory().unwrap());
    let results = stores
        .state_machine
        .apply([
            metadata(4, prepare(&first, &next)),
            metadata(5, prepare(&first, &next)),
            metadata(
                6,
                MetadataCommand::BeginMove {
                    intent: next.clone(),
                },
            ),
        ])
        .await
        .unwrap();
    assert_eq!(
        results,
        vec![
            ApplyResult::Metadata(MetadataResult::Prepared {
                data_group: next.data_group,
                job_id: next.job_id
            }),
            ApplyResult::Metadata(MetadataResult::AlreadyPrepared {
                data_group: next.data_group,
                job_id: next.job_id
            }),
            ApplyResult::Metadata(MetadataResult::Conflict {
                reason: ConflictReason::ExistingMove
            }),
        ]
    );
    let after = stores.state_machine.reader().read_metadata().await.unwrap();
    let preparation = after.view.prepared_successor(next.data_group).unwrap();
    assert_eq!(preparation.intent, next);
    assert_eq!(
        preparation.executors,
        [endpoint(1), endpoint(2), endpoint(3), endpoint(4)]
    );
    assert_eq!(after.view.job(first.intent.data_group), Some(&first));
    assert_eq!(after.view.directory(), before.view.directory());
    assert_eq!(after.view.origin(), before.view.origin());
    assert_eq!(
        after.applied,
        Some(metadata(6, prepare(&first, &next)).log_id)
    );
    let encoded = raw_job(&stores, next.data_group).await;
    assert!(rockbee_raft_protocol::decode_frame::<MoveRecord>(&encoded).is_err());
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
    assert_eq!(raw_job(&stores, next.data_group).await, encoded);
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
    assert_eq!(raw_job(&target_group, next.data_group).await, encoded);
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
async fn failed_successor_preparation_preserves_exact_legacy_record_and_applied_position() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let storage = open(path.clone(), OpenMode::CreateNew, StorageOptions::default()).await;
    let mut stores = storage.open_group(GroupId::new(0)).unwrap();
    let first = seed_completed(&mut stores).await;
    let before = stores.state_machine.reader().read_metadata().await.unwrap();
    let original = raw_job(&stores, first.intent.data_group).await;
    let next = successor(&first, before.view.directory().unwrap());
    stores
        .state_machine
        .group
        .test_writes
        .lock()
        .unwrap()
        .push_back(TestWriteAction::Fail);
    let result = stores
        .state_machine
        .apply([metadata(4, prepare(&first, &next))])
        .await;
    assert!(
        result.is_err(),
        "actual metadata persistence must encounter the injected adapter error"
    );
    let after = stores.state_machine.reader().read_metadata().await.unwrap();
    assert_eq!(after.view, before.view);
    assert_eq!(after.applied, before.applied);
    assert_eq!(raw_job(&stores, first.intent.data_group).await, original);
    assert!(after.view.prepared_successor(next.data_group).is_none());
    assert_eq!(
        rockbee_raft_protocol::decode_frame::<MoveRecord>(&original).unwrap(),
        first
    );
    let mut sibling = storage.open_group(GroupId::new(2)).unwrap();
    sibling
        .state_machine
        .apply([entry(
            0,
            Command::Put {
                key: Bytes::from_static(b"sibling"),
                value: Bytes::from_static(b"writable"),
            },
        )])
        .await
        .unwrap();
    drop(sibling);
    drop(stores);
    drop(storage);

    let reopened = open(path, OpenMode::OpenExisting, StorageOptions::default()).await;
    let mut stores = reopened.open_group(GroupId::new(0)).unwrap();
    let restored = stores.state_machine.reader().read_metadata().await.unwrap();
    assert_eq!(restored.view, before.view);
    assert_eq!(restored.applied, before.applied);
    assert_eq!(raw_job(&stores, first.intent.data_group).await, original);
    let results = stores
        .state_machine
        .apply([metadata(4, prepare(&first, &next))])
        .await
        .unwrap();
    assert_eq!(
        results,
        vec![ApplyResult::Metadata(MetadataResult::Prepared {
            data_group: next.data_group,
            job_id: next.job_id,
        })]
    );
    assert_eq!(
        stores
            .state_machine
            .reader()
            .read_metadata()
            .await
            .unwrap()
            .view
            .job(next.data_group),
        Some(&first)
    );
    let sibling = reopened.open_group(GroupId::new(2)).unwrap();
    assert_eq!(
        read(&sibling, b"sibling").await,
        Some(Bytes::from_static(b"writable"))
    );
}
