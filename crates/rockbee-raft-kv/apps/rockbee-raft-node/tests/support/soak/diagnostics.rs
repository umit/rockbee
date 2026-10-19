//! Opt-in bounded observations for one failure investigation, not a metrics API.
//! Leader replies are local hints, not term observations or quorum authority.

use super::{OP_NAMES, OperationResult, ProcessCluster};
use rockbee_raft_protocol::{
    GroupId,
    backend::{BackendOperation, BackendReply, BackendRequest, BackendResponse},
    routing::ClusterDirectory,
    transport::{Link, LinkConfig},
};
use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{Instant, MissedTickBehavior, interval},
};

const MAX_EVENTS: usize = 8192;

#[derive(Clone, Default)]
pub(super) struct Recorder(Arc<Mutex<History>>);
#[derive(Default)]
struct History {
    rows: Vec<Row>,
    omitted: u64,
}
struct Row {
    kind: &'static str,
    start_us: u128,
    end_us: u128,
    node_or_worker: u64,
    group: u64,
    detail: String,
}

impl Recorder {
    fn push(&self, row: Row) {
        let mut history = self.0.lock().unwrap();
        if history.rows.len() == MAX_EVENTS {
            history.omitted += 1;
        } else {
            history.rows.push(row);
        }
    }

    pub(super) fn operation(
        &self,
        worker: usize,
        group: usize,
        step: usize,
        elapsed: std::ops::Range<Duration>,
        result: &OperationResult,
    ) {
        if elapsed.end - elapsed.start < Duration::from_millis(100) && result.is_ok() {
            return;
        }
        let outcome = result.as_ref().map_or_else(
            |error| format!("{:?}: {}", error.outcome, error.detail),
            |_| "success".to_owned(),
        );
        self.push(Row {
            kind: "operation",
            start_us: elapsed.start.as_micros(),
            end_us: elapsed.end.as_micros(),
            node_or_worker: u64::try_from(worker).unwrap(),
            group: u64::try_from(group + 1).unwrap(),
            detail: format!("{} {outcome}", OP_NAMES[step]),
        });
    }

    fn write(&self, directory: &std::path::Path) -> std::io::Result<()> {
        let history = self.0.lock().unwrap();
        let mut file = fs::File::create(directory.join("diagnostics.csv"))?;
        writeln!(file, "kind,start_us,end_us,node_or_worker,group,detail")?;
        for row in &history.rows {
            writeln!(
                file,
                "{},{},{},{},{},\"{}\"",
                row.kind,
                row.start_us,
                row.end_us,
                row.node_or_worker,
                row.group,
                row.detail.replace('"', "\"\"")
            )?;
        }
        fs::write(
            directory.join("diagnostics.txt"),
            format!(
                "diagnostic_run=true\nprobe_cadence_ms=250\nprobe_request_timeout_ms=200\nprobe_nodes=3\nprobe_groups=0..=4\nprobe_links=3 extra backend links; one per node\nleader_semantics=local hint only; no term or quorum authority\nsnapshot_semantics=observed file name changes; not exact publication time\noperation_filter=failed or latency>=100ms\nmax_events={MAX_EVENTS}\nrecorded_events={}\nomitted_events={}\nprobe_tasks_joined_and_links_closed=true\nobserver_traffic_included=true\n",
                history.rows.len(),
                history.omitted
            ),
        )
    }
}

pub(super) struct Diagnostics {
    pub(super) recorder: Recorder,
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl Diagnostics {
    pub(super) async fn start(
        cluster: &ProcessCluster,
        mut start: watch::Receiver<Option<Instant>>,
    ) -> Result<Option<Self>, String> {
        if std::env::var("ROCKBEE_SOAK_DIAGNOSTICS").ok().as_deref() != Some("1") {
            return Ok(None);
        }
        // Connections are created before worker spawning. Failure cannot strand workers.
        let mut links = Vec::new();
        for node in &cluster.directory.nodes {
            match Link::<BackendRequest, BackendResponse>::connect(
                node.data_address,
                LinkConfig::default(),
                Instant::now() + Duration::from_secs(2),
            )
            .await
            {
                Ok(link) => links.push((node.node_id, link)),
                Err(error) => {
                    for (_, link) in links {
                        link.close().await;
                    }
                    return Err(error.to_string());
                }
            }
        }
        let (stop, receiver) = watch::channel(false);
        let recorder = Recorder::default();
        let task_recorder = recorder.clone();
        let directory = cluster.directory.clone();
        let root = cluster.root.path().to_path_buf();
        let task = tokio::spawn(async move {
            if start.wait_for(Option::is_some).await.is_ok() {
                let started = (*start.borrow()).unwrap();
                observe(&links, &directory, root, started, receiver, task_recorder).await;
            }
            for (_, link) in links {
                link.close().await;
            }
        });
        Ok(Some(Self {
            recorder,
            stop,
            task,
        }))
    }

    pub(super) async fn finish(self, report_dir: &std::path::Path) {
        let _ = self.stop.send(true);
        self.task.await.unwrap();
        self.recorder.write(report_dir).unwrap();
    }
}

async fn observe(
    links: &[(
        rockbee_raft_protocol::NodeId,
        Link<BackendRequest, BackendResponse>,
    )],
    directory: &ClusterDirectory,
    root: PathBuf,
    started: Instant,
    mut stop: watch::Receiver<bool>,
    recorder: Recorder,
) {
    let mut ticks = interval(Duration::from_millis(250));
    ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut previous_files = BTreeMap::new();
    loop {
        tokio::select! {
            _ = stop.changed() => break,
            _ = ticks.tick() => {},
        }
        for (node, link) in links {
            for group in 0..=4 {
                if *stop.borrow() {
                    return;
                }
                let before = Instant::now();
                let request = BackendRequest {
                    cluster_id: directory.cluster_id,
                    target_node: *node,
                    operation: BackendOperation::Leader {
                        group_id: GroupId::new(group),
                    },
                };
                let detail = match link.try_submit(request, before + Duration::from_millis(200)) {
                    Ok(pending) => match pending.await {
                        Ok(response) => match response.reply {
                            BackendReply::Leader { leader, .. } => format!(
                                "leader={}",
                                leader.map_or_else(|| "none".to_owned(), |id| id.get().to_string())
                            ),
                            _ => "unexpected_response".to_owned(),
                        },
                        Err(error) => format!("error={error}"),
                    },
                    Err(error) => format!("admission={error}"),
                };
                recorder.push(Row {
                    kind: "leader_hint",
                    start_us: (before - started).as_micros(),
                    end_us: started.elapsed().as_micros(),
                    node_or_worker: node.get(),
                    group,
                    detail: detail.chars().take(256).collect(),
                });
            }
        }
        let root = root.clone();
        // Directory reads stay off runtime workers; this exact blocking job is
        // awaited before the observer can finish and the fixture can be deleted.
        let current = tokio::task::spawn_blocking(move || snapshot_files(root))
            .await
            .unwrap();
        let observed = started.elapsed().as_micros();
        for ((node, group), files) in &current {
            if previous_files.get(&(*node, *group)) != Some(files) {
                recorder.push(Row {
                    kind: "snapshot_files",
                    start_us: observed,
                    end_us: observed,
                    node_or_worker: *node,
                    group: *group,
                    detail: files.clone(),
                });
            }
        }
        previous_files = current;
    }
}

fn snapshot_files(root: PathBuf) -> BTreeMap<(u64, u64), String> {
    let mut result = BTreeMap::new();
    for node in 1..=3 {
        for group in 0..=4 {
            let path = root.join(format!("node-{node}/snapshots/group-{group:016x}"));
            let mut files = match fs::read_dir(path) {
                Ok(entries) => entries
                    .take(16)
                    .filter_map(Result::ok)
                    .map(|entry| {
                        entry
                            .file_name()
                            .to_string_lossy()
                            .chars()
                            .take(64)
                            .collect::<String>()
                    })
                    .collect::<Vec<_>>(),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
                Err(error) => vec![format!("error={error}")],
            };
            files.sort();
            result.insert((node, group), files.join(";"));
        }
    }
    result
}

#[test]
fn diagnostic_history_is_bounded_and_omission_is_explicit() {
    let recorder = Recorder::default();
    for _ in 0..MAX_EVENTS + 3 {
        recorder.push(Row {
            kind: "test",
            start_us: 0,
            end_us: 0,
            node_or_worker: 1,
            group: 1,
            detail: String::new(),
        });
    }
    let history = recorder.0.lock().unwrap();
    assert_eq!(history.rows.len(), MAX_EVENTS);
    assert_eq!(history.omitted, 3);
}
