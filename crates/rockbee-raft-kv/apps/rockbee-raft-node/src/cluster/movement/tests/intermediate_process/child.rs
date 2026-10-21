use super::super::native_membership_observation::{
    NativeMembershipCall, NativeMembershipIdentity, NativeMembershipObservation,
};
use super::wire::{Command, Config, Envelope, Held, Receipt};
use super::*;
use crate::{
    config::NodeConfig,
    storage::{JointApplyPause, OpenMode},
};

struct Armed {
    pause: Arc<JointApplyPause>,
    observer: Arc<NativeMembershipObservation>,
    baseline: LogId,
    original: Mutex<Option<(NativeMembershipCall, Held)>>,
}

// Exact-filter child entry: default workspace execution is an unarmed no-op.
// The child never launches a process, initializes a cluster or resumes a job.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn joint_process_child() {
    let Some(path) = std::env::var_os(MARKER) else {
        return;
    };
    let config: Config =
        wire::read(Path::new(&path)).expect("armed child requires bounded valid config");
    config.validate();
    let _ = env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("rockbee_raft_node=info"),
    )
    .is_test(true)
    .try_init();
    let deadline = Instant::now() + Duration::from_secs(180);
    let node_config = NodeConfig::new(
        config.directory.clone(),
        NodeId::new(2).unwrap(),
        config.database.clone(),
        if config.reopen {
            OpenMode::OpenExisting
        } else {
            OpenMode::CreateNew
        },
    )
    .unwrap();
    let mut node = timeout_at(
        Instant::now() + Duration::from_secs(20),
        NodeRuntime::start(node_config),
    )
    .await
    .unwrap()
    .unwrap();
    let armed: Arc<Mutex<Option<Arc<Armed>>>> = Arc::new(Mutex::new(None));
    let observer_task = Arc::new(Mutex::new(None));
    wire::emit(
        &config,
        0,
        "ready",
        Receipt::Ready {
            endpoint: node.config.endpoint.clone(),
            reopen: config.reopen,
            runner_count: node.service.movement.tasks.lock().unwrap().len(),
        },
    );
    // Runtime/pause/task ownership stays outside the assertion task on all
    // panic/timeout paths. Only the actual process kill bypasses this cleanup.
    let service = Arc::clone(&node.service);
    let task_armed = Arc::clone(&armed);
    let task_observer = Arc::clone(&observer_task);
    let task_config = config.clone();
    let mut control = tokio::spawn(async move {
        commands(service, task_config, task_armed, task_observer, deadline).await
    });
    let outcome = match timeout_at(deadline, &mut control).await {
        Ok(outcome) => outcome,
        Err(_) => {
            control.abort();
            control.await
        }
    };
    if let Some(armed) = armed.lock().unwrap().as_ref() {
        armed.pause.release();
    }
    let observer = observer_task.lock().unwrap().take();
    if let Some(observer) = observer {
        observer.abort();
        let _ = observer.await;
    }
    node.close().await;
    if let Ok(sequence) = outcome.as_ref() {
        wire::emit(&config, *sequence, "stopped", Receipt::Stopped);
    }
    outcome.unwrap();
}

async fn commands(
    service: Arc<NodeService>,
    config: Config,
    armed_slot: Arc<Mutex<Option<Arc<Armed>>>>,
    observer_slot: Arc<Mutex<Option<JoinHandle<()>>>>,
    deadline: Instant,
) -> u8 {
    for sequence in 1..=wire::MAX_COMMANDS {
        let command = loop {
            if let Some(command) = wire::read::<Envelope<Command>>(&config.path("command.bin")) {
                if command.sequence == sequence {
                    config.check(&command, sequence, std::process::id());
                    break command.body;
                }
                // The one previous command may remain until atomic replacement.
                assert_eq!(command.sequence, sequence - 1, "stale or skipped command");
                config.check(&command, sequence - 1, std::process::id());
            }
            assert!(Instant::now() < deadline, "child command deadline");
            sleep(Duration::from_millis(5)).await;
        };
        match command {
            Command::Arm { baseline } => {
                assert!(!config.reopen && sequence == 1);
                assert!(armed_slot.lock().unwrap().is_none());
                let source = service.observe_membership(GROUP, deadline).await.unwrap();
                let intent = config.request.derive_intent(&config.directory).unwrap();
                assert_eq!(topology(&source, &intent).unwrap(), Topology::Source);
                assert!(covers(Some(native(source.applied)), Some(baseline)));
                for endpoint in &config.endpoints[..3] {
                    assert_eq!(source.peer(endpoint.node_id), Some(endpoint.peer_address));
                }
                assert_eq!(source.peers.iter().flatten().count(), 3);
                let pause = service.groups[&GROUP]
                    .reader
                    .pause_next_joint_apply_return(joint_configs());
                let observer =
                    Arc::new(NativeMembershipObservation::new(NativeMembershipIdentity {
                        node: NodeId::new(2).unwrap(),
                        group: GROUP,
                        job: config.request.job_id,
                        target: BTreeSet::from([1, 2, 4]),
                    }));
                *service.movement.native_membership.lock().unwrap() = Some(Arc::clone(&observer));
                let armed = Arc::new(Armed {
                    pause,
                    observer,
                    baseline,
                    original: Mutex::new(None),
                });
                *armed_slot.lock().unwrap() = Some(Arc::clone(&armed));
                wire::emit(
                    &config,
                    sequence,
                    "armed",
                    Receipt::Armed {
                        source: Box::new(source),
                    },
                );
                let observed_service = Arc::clone(&service);
                let observed_config = config.clone();
                *observer_slot.lock().unwrap() = Some(tokio::spawn(async move {
                    let joint = armed.pause.wait_reached().await;
                    let hook_observed = Instant::now();
                    require_joint(
                        joint.applied,
                        &joint.membership,
                        &observed_config.endpoints,
                        baseline,
                    );
                    let original = armed
                        .observer
                        .snapshot()
                        .expect("actual matching native entry required");
                    assert!(original.entered_at <= hook_observed);
                    let proof =
                        inspect_pending(&observed_service, &armed, &observed_config, &original);
                    *armed.original.lock().unwrap() = Some((original, proof.clone()));
                    wire::emit(&observed_config, sequence, "held", Receipt::Held(proof));
                }));
            }
            Command::ConfirmHeld => {
                assert!(!config.reopen && sequence == 2);
                let armed = armed_slot.lock().unwrap().as_ref().unwrap().clone();
                let (original, first) = armed.original.lock().unwrap().as_ref().unwrap().clone();
                let proof = inspect_pending(&service, &armed, &config, &original);
                assert_eq!(
                    proof, first,
                    "confirmation requires the identical held native invocation and runner"
                );
                wire::emit(&config, sequence, "held", Receipt::Held(proof));
                // No release or shutdown: parent must kill this live process.
            }
            Command::ObserveFinal {
                published,
                baseline,
            } => {
                assert!(config.reopen && sequence == 1);
                assert!(armed_slot.lock().unwrap().is_none());
                let (applied, membership) = loop {
                    let (applied, membership) = service.groups[&GROUP]
                        .reader
                        .read_membership()
                        .await
                        .unwrap();
                    if membership.membership().get_joint_config()
                        == &vec![BTreeSet::from([1, 2, 4])]
                        && covers(applied, Some(published))
                        && covers(applied, Some(baseline))
                        && covers(*membership.log_id(), Some(published))
                    {
                        break (applied, membership);
                    }
                    assert!(
                        Instant::now() < deadline,
                        "restarted B eventual local target deadline"
                    );
                    sleep(Duration::from_millis(10)).await;
                };
                require_local_target(applied, &membership, &config, published, baseline);
                wire::emit(
                    &config,
                    sequence,
                    "target",
                    Receipt::LocalTarget {
                        applied,
                        membership,
                    },
                );
            }
            Command::Stop => return sequence,
        }
    }
    panic!("bounded child command budget exhausted");
}

fn inspect_pending(
    service: &NodeService,
    armed: &Armed,
    config: &Config,
    original: &NativeMembershipCall,
) -> Held {
    let joint = armed
        .pause
        .held_observation()
        .expect("actual apply-return pause must stay held");
    require_joint(
        joint.applied,
        &joint.membership,
        &config.endpoints,
        armed.baseline,
    );
    let current = armed.observer.snapshot().unwrap();
    assert_eq!(current.identity, original.identity);
    assert_eq!(current.identity.node.get(), 2);
    assert_eq!(current.identity.group, GROUP);
    assert_eq!(current.identity.job, config.request.job_id);
    assert_eq!(current.identity.target, BTreeSet::from([1, 2, 4]));
    assert_ne!(current.invocation, 0);
    assert_eq!(current.invocation, original.invocation);
    assert_eq!(current.entered_at, original.entered_at);
    assert!(current.returned_at.is_none() && current.result.is_none());
    let tasks = service.movement.tasks.lock().unwrap();
    assert_eq!(tasks.len(), 1);
    let running = tasks.get(&GROUP).unwrap();
    assert_eq!(running.job_id, config.request.job_id);
    assert!(!running.task.is_finished());
    assert_eq!(service.movement.active.available_permits(), 3);
    assert!(!service.movement.stopping.load(Ordering::Acquire));
    Held {
        applied: joint.applied,
        membership: joint.membership,
        invocation: current.invocation,
        runner: format!("{}", running.task.id()),
        native_entered: true,
        native_returned: false,
        native_result: false,
        runner_finished: false,
        permits_available: 3,
        stopping: false,
        entry_before_hook: true,
    }
}
