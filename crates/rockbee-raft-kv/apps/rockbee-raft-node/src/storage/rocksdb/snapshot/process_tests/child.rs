use super::*;
use openraft::{
    Vote,
    storage::{RaftLogStorage, RaftLogStorageExt},
};
use tokio::task::JoinHandle;

type Install = JoinHandle<(crate::storage::StateMachine, Result<(), StoreError>)>;

// Without the private marker this exact-filter child is an inert workspace test.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_install_child() {
    let Some(path) = std::env::var_os(MARKER) else {
        return;
    };
    let config = wire::read::<wire::Config>(Path::new(&path))
        .expect("read private config")
        .expect("private config exists");
    config.validate().expect("valid private config");
    run(config).await.expect("owned snapshot child operation");
}

async fn run(config: wire::Config) -> Result<(), String> {
    let source_storage = Storage::open(
        config.database("source"),
        identity(),
        StorageOptions::default(),
        OpenMode::CreateNew,
    )
    .await
    .map_err(message)?;
    let mut source = source_storage
        .open_group(GroupId::new(1))
        .map_err(message)?;
    source
        .state_machine
        .apply(evidence::entries(true))
        .await
        .map_err(message)?;
    let incoming = source
        .state_machine
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .map_err(message)?;
    evidence::validate_meta(&incoming.meta, true)?;
    drop(source);
    drop(source_storage);

    let storage = Storage::open(
        config.database("target"),
        identity(),
        StorageOptions {
            io_chunk_entries: 1,
            ..StorageOptions::default()
        },
        OpenMode::CreateNew,
    )
    .await
    .map_err(message)?;
    let mut target = storage.open_group(GroupId::new(1)).map_err(message)?;
    let baseline = evidence::entries(false);
    target
        .log_store
        .save_vote(&Vote::new(3, 1))
        .await
        .map_err(message)?;
    target
        .log_store
        .blocking_append(baseline.clone())
        .await
        .map_err(message)?;
    target
        .log_store
        .save_committed(Some(evidence::id(3, 3)))
        .await
        .map_err(message)?;
    target
        .state_machine
        .apply(baseline)
        .await
        .map_err(message)?;
    let old_meta = target
        .state_machine
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .map_err(message)?
        .meta;
    evidence::validate_meta(&old_meta, false)?;
    let mut sibling = storage.open_group(GroupId::new(2)).map_err(message)?;
    sibling
        .log_store
        .save_vote(&Vote::new(6, 1))
        .await
        .map_err(message)?;
    sibling
        .log_store
        .blocking_append([evidence::sibling_entry()])
        .await
        .map_err(message)?;
    sibling
        .log_store
        .save_committed(Some(evidence::id(6, 0)))
        .await
        .map_err(message)?;
    sibling
        .state_machine
        .apply([evidence::sibling_entry()])
        .await
        .map_err(message)?;
    evidence::sibling(&mut sibling).await?;

    // Build and publication also visit AfterRename: arm only after both builds
    // have finished, and only in the target group's one install operation.
    let group = Arc::clone(&target.state_machine.group);
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    {
        let mut hooks = group.snapshot_hooks.lock().map_err(message)?;
        require(hooks.is_empty(), "snapshot hook was already armed")?;
        hooks.push_back((
            Stage::AfterRename,
            crate::storage::rocksdb::TestWriteAction::Hold {
                entered: entered_tx,
                resume: resume_rx,
            },
        ));
    }
    let incoming_meta = incoming.meta.clone();
    let mut state_machine = target.state_machine;
    let install = tokio::spawn(async move {
        let result = state_machine
            .install_snapshot(&incoming.meta, incoming.snapshot)
            .await;
        (state_machine, result)
    });
    // These owners outlive the control result. No timeout/abort is applied to
    // install; only the parent-owned process kill bypasses release + real join.
    let result = control(
        &config,
        &group,
        &mut sibling,
        &install,
        entered_rx,
        old_meta,
        incoming_meta,
    )
    .await;
    let _ = resume_tx.send(());
    let installed = install.await.map_err(message)?.1.map_err(message);
    drop(target.log_store);
    drop(group);
    drop(sibling);
    drop(storage);
    result.and(installed)
}

async fn held(
    group: &Arc<Group>,
    sibling: &mut GroupStores,
    install: &Install,
    old_meta: &Meta,
    incoming_meta: &Meta,
) -> Result<wire::Held, String> {
    let descriptor = read_descriptor(group)
        .map_err(message)?
        .ok_or("old descriptor disappeared")?;
    require(
        descriptor.header.meta == *old_meta,
        "descriptor activated while install was held",
    )?;
    let result = wire::Held {
        // Called only after the target install's actual AfterRename hook has
        // signaled; the retained sender still prevents native activation.
        phase: wire::Phase::AfterRename,
        group: group.id,
        old_meta: old_meta.clone(),
        incoming_meta: incoming_meta.clone(),
        old: evidence::view(group).await?,
        sibling: evidence::sibling(sibling).await?,
        old_image: descriptor.image,
        imported: evidence::imported(group)?,
        install_pending: !install.is_finished(),
        release_sent: false,
    };
    evidence::validate_held(&result)?;
    require(result.install_pending, "native install already returned")?;
    require(
        result.old.generation == descriptor.generation,
        "old state/descriptor generation mismatch",
    )?;
    Ok(result)
}

async fn control(
    config: &wire::Config,
    group: &Arc<Group>,
    sibling: &mut GroupStores,
    install: &Install,
    entered: tokio::sync::oneshot::Receiver<()>,
    old_meta: Meta,
    incoming_meta: Meta,
) -> Result<(), String> {
    entered.await.map_err(message)?;
    let initial = held(group, sibling, install, &old_meta, &incoming_meta).await?;
    wire::write(
        &config.path("held.bin"),
        &wire::envelope(config, 0, initial.clone()),
    )?;
    // No timer releases the native hold. The parent owns the bounded wait and
    // exact Child handle; Stop is only the non-fault graceful exit path.
    for sequence in 1..=2 {
        let command = loop {
            if let Some(command) =
                wire::read::<wire::Envelope<wire::Command>>(&config.path("command.bin"))?
            {
                if command.sequence == sequence {
                    wire::validate_identity(&command, config, std::process::id(), sequence)?;
                    break command.body;
                }
                wire::validate_identity(&command, config, std::process::id(), sequence - 1)?;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        match (sequence, command) {
            (1, wire::Command::ConfirmHeld) => {
                let confirmed = held(group, sibling, install, &old_meta, &incoming_meta).await?;
                require(confirmed == initial, "held evidence changed before kill")?;
                wire::write(
                    &config.path("confirmed.bin"),
                    &wire::envelope(config, 1, confirmed),
                )?;
            }
            (2, wire::Command::Stop) => return Ok(()),
            _ => return Err("unexpected child command".into()),
        }
    }
    Err("child command limit".into())
}
