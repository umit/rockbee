#![allow(clippy::unwrap_used)]
use super::super::{GroupStores, OpenMode, Storage, StorageOptions, TestWriteAction};
use super::*;
use crate::types::{Command, Entry};
use bytes::Bytes;
use openraft::{EntryPayload, storage::RaftStateMachine};
use rockbee_raft_protocol::{ClusterId, GroupId, NodeId, ShardConfig};
use std::sync::atomic::Ordering as AtomicOrdering;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn identity() -> StorageIdentity {
    StorageIdentity {
        cluster_id: ClusterId::new([7; 16]).unwrap(),
        node_id: NodeId::new(1).unwrap(),
        shard_config: ShardConfig::new(4, 71).unwrap(),
    }
}
fn put(index: u64, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Entry {
    Entry {
        log_id: LogId::new(openraft::CommittedLeaderId::new(3, 0), index),
        payload: EntryPayload::Normal(Command::Put {
            key: key.into(),
            value: value.into(),
        }),
    }
}
async fn store(options: StorageOptions) -> (tempfile::TempDir, Storage, GroupStores) {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(
        dir.path().join("db"),
        identity(),
        options,
        OpenMode::CreateNew,
    )
    .await
    .unwrap();
    let group = storage.open_group(GroupId::new(1)).unwrap();
    (dir, storage, group)
}
async fn source() -> (tempfile::TempDir, Snapshot<TypeConfig>) {
    let (dir, _storage, mut group) = store(StorageOptions::default()).await;
    group
        .state_machine
        .apply([
            put(10, b"a".as_slice(), b"new".as_slice()),
            put(11, b"b".as_slice(), b"other".as_slice()),
        ])
        .await
        .unwrap();
    let image = group
        .state_machine
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();
    (dir, image)
}
async fn old_state(group: &mut GroupStores) -> Meta {
    group
        .state_machine
        .apply([
            put(1, b"a".as_slice(), b"old".as_slice()),
            put(2, b"gone".as_slice(), b"old".as_slice()),
        ])
        .await
        .unwrap();
    group
        .state_machine
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap()
        .meta
}
fn inject(group: &Group, stage: Stage, action: TestWriteAction) {
    group
        .snapshot_hooks
        .lock()
        .unwrap()
        .push_back((stage, action));
}

#[tokio::test]
async fn snapshot_failure_boundaries_reopen_one_complete_generation() {
    let (_source_dir, image) = source().await;
    for stage in [
        Stage::BeforeFileSync,
        Stage::AfterRename,
        Stage::BeforeActivation,
        Stage::AfterActivation,
        Stage::DuringCleanup,
    ] {
        let (dir, storage, mut group) = store(StorageOptions {
            io_chunk_entries: 1,
            ..StorageOptions::default()
        })
        .await;
        let old = old_state(&mut group).await;
        inject(&group.state_machine.group, stage, TestWriteAction::Fail);
        assert!(
            group
                .state_machine
                .install_snapshot(
                    &image.meta,
                    Box::new(image.snapshot.try_clone().await.unwrap())
                )
                .await
                .is_err(),
            "stage {stage:?}"
        );
        drop(group);
        drop(storage);
        let storage = Storage::open(
            dir.path().join("db"),
            identity(),
            StorageOptions::default(),
            OpenMode::OpenExisting,
        )
        .await
        .unwrap();
        let mut group = storage.open_group(GroupId::new(1)).unwrap();
        let committed = matches!(stage, Stage::AfterActivation | Stage::DuringCleanup);
        let expected = if committed { &image.meta } else { &old };
        assert_eq!(
            &group
                .state_machine
                .get_current_snapshot()
                .await
                .unwrap()
                .unwrap()
                .meta,
            expected,
            "stage {stage:?}"
        );
        let read = group
            .state_machine
            .reader()
            .read_local(Bytes::from_static(b"a"))
            .await
            .unwrap();
        assert_eq!(
            read.value,
            Some(Bytes::from_static(if committed { b"new" } else { b"old" }))
        );
        assert_eq!(read.applied, expected.last_log_id);
        assert_eq!(
            group
                .state_machine
                .reader()
                .read_local(Bytes::from_static(b"gone"))
                .await
                .unwrap()
                .value
                .is_none(),
            committed
        );
    }
}

#[tokio::test]
async fn corrupt_streams_never_activate_and_orphans_are_reclaimed_on_reopen() {
    let (_source_dir, mut image) = source().await;
    let mut original = Vec::new();
    image.snapshot.read_to_end(&mut original).await.unwrap();
    let header_len = u32::from_be_bytes(original[8..12].try_into().unwrap()) as usize;
    let first = 12 + header_len;
    let first_key_len = u32::from_be_bytes(original[first..first + 4].try_into().unwrap()) as usize;
    let first_value_len =
        u32::from_be_bytes(original[first + 4..first + 8].try_into().unwrap()) as usize;
    let second = first + 8 + first_key_len + first_value_len;
    let mut cases: Vec<(&str, Vec<u8>, Meta)> = Vec::new();
    for (name, offset) in [
        ("magic", 0),
        ("cluster", 12),
        ("group", 35),
        ("hash-version", 36),
        ("checksum", original.len() - 1),
        ("count", original.len() - 5),
    ] {
        let mut bytes = original.clone();
        bytes[offset] ^= 0x40;
        cases.push((name, bytes, image.meta.clone()));
    }
    for (name, offset) in [
        ("huge-header", 8),
        ("huge-key", first),
        ("huge-value", first + 4),
    ] {
        let mut bytes = original.clone();
        bytes[offset..offset + 4].copy_from_slice(&u32::MAX.to_be_bytes());
        cases.push((name, bytes, image.meta.clone()));
    }
    let mut duplicate = original.clone();
    duplicate[second + 8] = b'a';
    cases.push(("duplicate", duplicate, image.meta.clone()));
    let mut unordered = original.clone();
    unordered[second + 8] = 0;
    cases.push(("unordered", unordered, image.meta.clone()));
    for length in [0, 9, 12 + header_len / 2, first + 9, original.len() - 3] {
        cases.push(("truncated", original[..length].to_vec(), image.meta.clone()));
    }
    let mut trailing = original.clone();
    trailing.push(0);
    cases.push(("trailing", trailing, image.meta.clone()));
    let mut mismatch = image.meta.clone();
    mismatch.snapshot_id.push_str("wrong");
    cases.push(("metadata", original.clone(), mismatch));
    // A huge nested configuration count is rejected against remaining header
    // bytes before constructing any Vec/BTreeSet allocation.
    let mut hostile = original.clone();
    let joint_count = 12 + 16 + 8 + 1 + 2 + 8 + 25 + 1;
    hostile[joint_count..joint_count + 4].copy_from_slice(&u32::MAX.to_be_bytes());
    cases.push(("nested-count", hostile, image.meta.clone()));
    for (name, bytes, meta) in cases {
        let (dir, storage, mut group) = store(StorageOptions {
            io_chunk_entries: 1,
            ..StorageOptions::default()
        })
        .await;
        let old = old_state(&mut group).await;
        let mut received = group
            .state_machine
            .begin_receiving_snapshot()
            .await
            .unwrap();
        received.write_all(&bytes).await.unwrap();
        received.flush().await.unwrap();
        assert!(
            group
                .state_machine
                .install_snapshot(&meta, received)
                .await
                .is_err(),
            "accepted {name}"
        );
        assert_eq!(
            group
                .state_machine
                .reader()
                .read_local(Bytes::from_static(b"a"))
                .await
                .unwrap()
                .value,
            Some(Bytes::from_static(b"old")),
            "changed active state: {name}"
        );
        drop(group);
        drop(storage);
        let storage = Storage::open(
            dir.path().join("db"),
            identity(),
            StorageOptions::default(),
            OpenMode::OpenExisting,
        )
        .await
        .unwrap();
        let mut group = storage.open_group(GroupId::new(1)).unwrap();
        assert_eq!(
            group
                .state_machine
                .get_current_snapshot()
                .await
                .unwrap()
                .unwrap()
                .meta,
            old,
            "changed descriptor: {name}"
        );
        let files = fs::read_dir(directory(&group.state_machine.group))
            .unwrap()
            .count();
        assert_eq!(files, 1, "orphan image remains: {name}");
    }
}

#[tokio::test]
async fn stale_builder_and_pinned_read_survive_install_and_cleanup() {
    let (_source_dir, image) = source().await;
    let (_dir, storage, mut group) = store(StorageOptions::default()).await;
    let old = old_state(&mut group).await;
    let mut adjacent = storage.open_group(GroupId::new(u64::MAX)).unwrap();
    adjacent
        .state_machine
        .apply([put(1, b"a".as_slice(), b"adjacent".as_slice())])
        .await
        .unwrap();
    let (read_entered, entered) = tokio::sync::oneshot::channel();
    let (read_resume, resume) = std::sync::mpsc::channel();
    let owner = group.state_machine.group.clone();
    let held_read = tokio::spawn(async move {
        owner
            .read(ErrorSubject::StateMachine, move |group| {
                let view = group.shared.db.snapshot();
                let state = state::read_record(group, &view)?;
                let key = keys::data(group.id, state.generation, b"a");
                let before = view.get_cf(group.cf("state")?, &key).map_err(fail)?;
                let _ = read_entered.send(());
                resume.recv().map_err(fail)?;
                let after = view.get_cf(group.cf("state")?, key).map_err(fail)?;
                Ok((state.applied, before, after))
            })
            .await
    });
    entered.await.unwrap();
    let (build_entered, entered) = tokio::sync::oneshot::channel();
    let (build_resume, resume) = std::sync::mpsc::channel();
    inject(
        &group.state_machine.group,
        Stage::BeforePublication,
        TestWriteAction::Hold {
            entered: build_entered,
            resume,
        },
    );
    let mut builder = group.state_machine.get_snapshot_builder().await;
    let building = tokio::spawn(async move { builder.build_snapshot().await });
    entered.await.unwrap();
    assert_eq!(
        group
            .state_machine
            .group
            .shared
            .snapshot_lane
            .available_permits(),
        1,
        "builder must release lane before publication gate"
    );
    group
        .state_machine
        .install_snapshot(&image.meta, image.snapshot)
        .await
        .unwrap();
    build_resume.send(()).unwrap();
    let built = building.await.unwrap().unwrap();
    assert_eq!(built.meta, image.meta);
    assert_eq!(
        group
            .state_machine
            .get_current_snapshot()
            .await
            .unwrap()
            .unwrap()
            .meta,
        image.meta
    );
    read_resume.send(()).unwrap();
    let (applied, before, after) = held_read.await.unwrap().unwrap();
    assert_eq!(applied, old.last_log_id);
    assert_eq!(before, Some(b"old".to_vec()));
    assert_eq!(before, after);
    assert_eq!(
        group
            .state_machine
            .reader()
            .read_local(Bytes::from_static(b"a"))
            .await
            .unwrap()
            .value,
        Some(Bytes::from_static(b"new"))
    );
    assert_eq!(
        adjacent
            .state_machine
            .reader()
            .read_local(Bytes::from_static(b"a"))
            .await
            .unwrap()
            .value,
        Some(Bytes::from_static(b"adjacent"))
    );
}

#[tokio::test]
async fn cancellation_keeps_install_gate_native_owner_and_lane_until_completion() {
    let (_source_dir, image) = source().await;
    let (_dir, storage, mut group) = store(StorageOptions::default()).await;
    old_state(&mut group).await;
    let owner = group.state_machine.group.clone();
    let (entered_tx, entered) = tokio::sync::oneshot::channel();
    let (resume_tx, resume) = std::sync::mpsc::channel();
    inject(
        &owner,
        Stage::BeforeActivation,
        TestWriteAction::Hold {
            entered: entered_tx,
            resume,
        },
    );
    let mut state = group.state_machine;
    let install =
        tokio::spawn(async move { state.install_snapshot(&image.meta, image.snapshot).await });
    entered.await.unwrap();
    install.abort();
    assert!(install.await.unwrap_err().is_cancelled());
    assert!(owner.retired.load(AtomicOrdering::Acquire));
    assert_eq!(owner.gate.available_permits(), 0);
    assert_eq!(owner.shared.snapshot_lane.available_permits(), 0);
    assert!(storage.open_group(GroupId::new(1)).is_err());
    assert_eq!(
        group.log_store.group.shared.state_lane.available_permits(),
        2
    );
    // Drop every caller-owned Group handle while the actual native install is
    // still paused. Only the closure may keep the registry owner alive now.
    let gate = owner.gate.clone();
    let lane = owner.shared.snapshot_lane.clone();
    drop(owner);
    drop(group.log_store);
    assert!(storage.open_group(GroupId::new(1)).is_err());
    resume_tx.send(()).unwrap();
    let replacement = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if let Ok(group) = storage.open_group(GroupId::new(1)) {
                break group;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(gate.available_permits(), 1);
    assert_eq!(lane.available_permits(), 1);
    assert_eq!(
        replacement
            .state_machine
            .reader()
            .read_local(Bytes::from_static(b"a"))
            .await
            .unwrap()
            .value,
        Some(Bytes::from_static(b"new"))
    );
}

#[tokio::test]
async fn large_image_streams_with_bounded_batches_and_maximum_legal_record() {
    let (_dir, _storage, mut source) = store(StorageOptions::default()).await;
    let max_key = vec![0xff; rockbee_raft_protocol::MAX_KEY_BYTES];
    let max_value = vec![0x41; rockbee_raft_protocol::MAX_VALUE_BYTES];
    let mut entries = Vec::new();
    for index in 0..10 {
        entries.push(put(
            index,
            format!("key-{index:03}").into_bytes(),
            vec![index as u8; 600 * 1024],
        ));
    }
    entries.push(put(10, max_key.clone(), max_value.clone()));
    source.state_machine.apply(entries).await.unwrap();
    let image = source
        .state_machine
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();
    let length = image.snapshot.metadata().await.unwrap().len();
    let options = StorageOptions {
        io_chunk_entries: 2,
        ..StorageOptions::default()
    };
    assert!(length > options.io_chunk_bytes as u64);
    let (_target_dir, _target_storage, mut target) = store(options.clone()).await;
    target
        .state_machine
        .install_snapshot(&image.meta, image.snapshot)
        .await
        .unwrap();
    let owner = &target.state_machine.group;
    let peak_batch = owner.snapshot_peak_batch.load(AtomicOrdering::Relaxed);
    let peak_records = owner.snapshot_peak_records.load(AtomicOrdering::Relaxed);
    assert!(peak_batch <= options.io_chunk_bytes && peak_batch > max_value.len());
    assert!(peak_records <= options.io_chunk_entries && peak_records > 0);
    let peak_record_bytes = owner
        .snapshot_peak_record_bytes
        .load(AtomicOrdering::Relaxed);
    let record_bound =
        2 * rockbee_raft_protocol::MAX_KEY_BYTES + rockbee_raft_protocol::MAX_VALUE_BYTES;
    assert!(peak_record_bytes <= record_bound && peak_record_bytes > max_value.len());
    println!(
        "snapshot image={length} bytes; peak batch={peak_batch} bytes/{peak_records} records; peak input record plus previous key={peak_record_bytes} bytes (bound {record_bound}); fixed I/O buffers={} bytes",
        2 * format::IO_BUFFER
    );
    assert_eq!(
        target
            .state_machine
            .reader()
            .read_local(Bytes::from(max_key))
            .await
            .unwrap()
            .value,
        Some(Bytes::from(max_value))
    );
}

#[tokio::test]
async fn published_file_corruption_fails_closed_and_preserves_evidence() {
    for remove in [false, true] {
        let (dir, storage, mut group) = store(StorageOptions::default()).await;
        old_state(&mut group).await;
        let descriptor = read_descriptor(&group.state_machine.group)
            .unwrap()
            .unwrap();
        let path = final_path(&group.state_machine.group, descriptor.image);
        if remove {
            fs::remove_file(&path).unwrap();
        } else {
            let mut bytes = fs::read(&path).unwrap();
            let last = bytes.len() - 1;
            bytes[last] ^= 1;
            fs::write(&path, &bytes).unwrap();
        }
        assert!(group.state_machine.get_current_snapshot().await.is_err());
        drop(group);
        drop(storage);
        assert!(
            Storage::open(
                dir.path().join("db"),
                identity(),
                StorageOptions::default(),
                OpenMode::OpenExisting
            )
            .await
            .is_err()
        );
        assert_eq!(
            path.exists(),
            !remove,
            "recovery removed corrupt authoritative evidence"
        );
    }
}

#[tokio::test]
async fn replayed_delete_can_advance_state_beyond_current_image_and_open_handles_survive_gc() {
    let (_source_dir, image) = source().await;
    let (dir, storage, mut group) = store(StorageOptions::default()).await;
    group
        .state_machine
        .install_snapshot(&image.meta, image.snapshot)
        .await
        .unwrap();
    let mut old_handle = group
        .state_machine
        .get_current_snapshot()
        .await
        .unwrap()
        .unwrap()
        .snapshot;
    group
        .state_machine
        .apply([Entry {
            log_id: LogId::new(openraft::CommittedLeaderId::new(3, 0), 12),
            payload: EntryPayload::Normal(Command::Delete {
                key: Bytes::from_static(b"a"),
            }),
        }])
        .await
        .unwrap();
    drop(group);
    drop(storage);
    let storage = Storage::open(
        dir.path().join("db"),
        identity(),
        StorageOptions::default(),
        OpenMode::OpenExisting,
    )
    .await
    .unwrap();
    let mut group = storage.open_group(GroupId::new(1)).unwrap();
    assert_eq!(
        group
            .state_machine
            .reader()
            .read_local(Bytes::from_static(b"a"))
            .await
            .unwrap()
            .value,
        None
    );
    assert_eq!(
        group
            .state_machine
            .get_current_snapshot()
            .await
            .unwrap()
            .unwrap()
            .meta,
        image.meta
    );
    let newer = group
        .state_machine
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();
    assert_eq!(newer.meta.last_log_id.unwrap().index, 12);
    let mut retained = Vec::new();
    old_handle.read_to_end(&mut retained).await.unwrap();
    assert!(retained.starts_with(format::MAGIC));
    assert_eq!(
        fs::read_dir(directory(&group.state_machine.group))
            .unwrap()
            .count(),
        1
    );
}

#[tokio::test]
async fn missing_or_rewound_sequence_cannot_reuse_an_activated_generation() {
    for missing in [false, true] {
        let (_source_dir, image) = source().await;
        let (dir, storage, mut group) = store(StorageOptions::default()).await;
        group
            .state_machine
            .install_snapshot(&image.meta, image.snapshot)
            .await
            .unwrap();
        let owner = &group.state_machine.group;
        if missing {
            owner
                .shared
                .db
                .delete_cf(owner.cf("snapshot_meta").unwrap(), keys::meta(1, SEQUENCE))
                .unwrap();
        } else {
            owner
                .shared
                .db
                .put_cf(
                    owner.cf("snapshot_meta").unwrap(),
                    keys::meta(1, SEQUENCE),
                    encode(&0u64, ErrorSubject::Snapshot(None)).unwrap(),
                )
                .unwrap();
        }
        drop(group);
        drop(storage);
        assert!(
            Storage::open(
                dir.path().join("db"),
                identity(),
                StorageOptions::default(),
                OpenMode::OpenExisting
            )
            .await
            .is_err()
        );
    }
}

#[tokio::test]
async fn equal_applied_identity_cannot_replace_different_logical_state() {
    let (_source_dir, image) = source().await;
    let (_dir, _storage, mut target) = store(StorageOptions::default()).await;
    target
        .state_machine
        .install_snapshot(
            &image.meta,
            Box::new(image.snapshot.try_clone().await.unwrap()),
        )
        .await
        .unwrap();
    // A genuine duplicate is fully parsed and remains durable.
    target
        .state_machine
        .install_snapshot(&image.meta, image.snapshot)
        .await
        .unwrap();
    let (_other_dir, _other_storage, mut other) = store(StorageOptions::default()).await;
    other
        .state_machine
        .apply([
            put(10, b"a".as_slice(), b"different".as_slice()),
            put(11, b"b".as_slice(), b"other".as_slice()),
        ])
        .await
        .unwrap();
    let conflict = other
        .state_machine
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();
    assert_eq!(conflict.meta, image.meta);
    assert!(
        target
            .state_machine
            .install_snapshot(&conflict.meta, conflict.snapshot)
            .await
            .is_err()
    );
    assert_eq!(
        target
            .state_machine
            .reader()
            .read_local(Bytes::from_static(b"a"))
            .await
            .unwrap()
            .value,
        Some(Bytes::from_static(b"new"))
    );
}

#[tokio::test]
async fn membership_cannot_conflict_with_applied_identity_or_regress_during_install() {
    use std::collections::{BTreeMap, BTreeSet};
    let membership_id = LogId::new(openraft::CommittedLeaderId::new(3, 0), 5);
    let membership = |voters| {
        openraft::Membership::new(
            vec![BTreeSet::from(voters)],
            BTreeMap::from([
                (0, openraft::BasicNode::new("zero")),
                (1, openraft::BasicNode::new("one")),
                (2, openraft::BasicNode::new("two")),
            ]),
        )
    };
    for variant in 0..4 {
        let (_dir, _storage, mut group) = store(StorageOptions::default()).await;
        group
            .state_machine
            .apply([
                Entry {
                    log_id: membership_id,
                    payload: EntryPayload::Membership(membership([0, 1])),
                },
                put(6, b"a".as_slice(), b"old".as_slice()),
            ])
            .await
            .unwrap();
        let state = state::read_record(
            &group.state_machine.group,
            &group.state_machine.group.shared.db.snapshot(),
        )
        .unwrap();
        let mut header = header_for(&group.state_machine.group, &state, 999);
        header.meta.last_log_id = Some(LogId::new(openraft::CommittedLeaderId::new(3, 0), 11));
        header.meta.last_membership = match variant {
            0 => StoredMembership::new(
                Some(LogId::new(openraft::CommittedLeaderId::new(9, 0), 11)),
                membership([0, 1]),
            ),
            1 => StoredMembership::default(),
            2 => StoredMembership::new(Some(membership_id), membership([0, 2])),
            _ => StoredMembership::new(
                Some(LogId::new(openraft::CommittedLeaderId::new(9, 0), 5)),
                membership([0, 1]),
            ),
        };
        let file = tempfile::tempfile().unwrap();
        let mut writer =
            ImageWriter::new(file, &header, StorageOptions::default().max_entry_bytes).unwrap();
        writer.record(b"a", b"new").unwrap();
        let (file, _) = writer.finish().unwrap();
        assert!(
            group
                .state_machine
                .install_snapshot(&header.meta, Box::new(tokio::fs::File::from_std(file)))
                .await
                .is_err(),
            "accepted incoherent membership variant {variant}"
        );
        assert_eq!(
            group.state_machine.applied_state().await.unwrap(),
            (state.applied, state.membership)
        );
        assert_eq!(
            group
                .state_machine
                .reader()
                .read_local(Bytes::from_static(b"a"))
                .await
                .unwrap()
                .value,
            Some(Bytes::from_static(b"old"))
        );
    }
}

#[tokio::test]
async fn reopen_rejects_changed_membership_at_exact_snapshot_applied_progress() {
    let (dir, storage, mut group) = store(StorageOptions::default()).await;
    old_state(&mut group).await;
    let owner = &group.state_machine.group;
    let mut state = state::read_record(owner, &owner.shared.db.snapshot()).unwrap();
    state.membership = StoredMembership::new(
        state.applied,
        openraft::Membership::new(vec![std::collections::BTreeSet::from([0])], None),
    );
    owner
        .shared
        .db
        .put_cf(
            owner.cf("state").unwrap(),
            keys::meta(1, 0),
            encode(&state, ErrorSubject::StateMachine).unwrap(),
        )
        .unwrap();
    drop(group);
    drop(storage);
    assert!(
        Storage::open(
            dir.path().join("db"),
            identity(),
            StorageOptions::default(),
            OpenMode::OpenExisting
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn snapshot_indices_cannot_move_opposite_to_committed_leader_identity() {
    let mut accepted = Vec::new();
    for variant in 0..2 {
        let (_dir, _storage, mut group) = store(StorageOptions::default()).await;
        let active_id = LogId::new(openraft::CommittedLeaderId::new(5, 1), 10);
        group
            .state_machine
            .apply([Entry {
                log_id: active_id,
                payload: EntryPayload::Blank,
            }])
            .await
            .unwrap();
        let state = state::read_record(
            &group.state_machine.group,
            &group.state_machine.group.shared.db.snapshot(),
        )
        .unwrap();
        let mut header = header_for(&group.state_machine.group, &state, 999);
        header.meta.last_log_id = Some(LogId::new(
            openraft::CommittedLeaderId::new(if variant == 0 { 1 } else { 6 }, 1),
            11,
        ));
        if variant == 1 {
            header.meta.last_membership = StoredMembership::new(
                Some(LogId::new(openraft::CommittedLeaderId::new(9, 1), 3)),
                openraft::Membership::default(),
            );
        }
        let writer = ImageWriter::new(
            tempfile::tempfile().unwrap(),
            &header,
            StorageOptions::default().max_entry_bytes,
        )
        .unwrap();
        let (file, _) = writer.finish().unwrap();
        if group
            .state_machine
            .install_snapshot(&header.meta, Box::new(tokio::fs::File::from_std(file)))
            .await
            .is_ok()
        {
            accepted.push(variant);
        } else {
            assert_eq!(
                group.state_machine.applied_state().await.unwrap().0,
                Some(active_id)
            );
        }
    }
    assert!(
        accepted.is_empty(),
        "accepted contradictory index/leader ordering: {accepted:?}"
    );
}
