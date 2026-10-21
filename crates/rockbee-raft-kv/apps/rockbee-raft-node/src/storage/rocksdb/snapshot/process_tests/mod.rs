//! One storage-adapter child dies after image+directory sync, before activation.
//! No CLI, network quorum, power-loss or other snapshot phase is covered here.
#![allow(clippy::unwrap_used, clippy::panic)]

use super::*;
use crate::{
    storage::{GroupStores, OpenMode, Storage, StorageOptions},
    types::{Command, Entry},
};
use bytes::Bytes;
use openraft::{
    RaftLogReader, Vote,
    storage::{RaftLogStorage, RaftStateMachine},
};
use rockbee_raft_protocol::{ClusterId, GroupId, NodeId, ShardConfig};
use serde::Serialize;
use std::{
    process::{Child, Command as ProcessCommand, Stdio},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

mod child;
mod evidence;
mod wire;

const CHILD_FILTER: &str =
    "storage::rocksdb::snapshot::process_tests::child::snapshot_install_child";
const MARKER: &str = "ROCKBEE_PRIVATE_SNAPSHOT_CUT_CONFIG";

fn message(error: impl std::fmt::Display) -> String {
    error.to_string()
}
fn require(condition: bool, error: &str) -> Result<(), String> {
    if condition {
        Ok(())
    } else {
        Err(error.to_owned())
    }
}
fn identity() -> StorageIdentity {
    StorageIdentity {
        cluster_id: ClusterId::new([117; 16]).unwrap(),
        node_id: NodeId::new(1).unwrap(),
        shard_config: ShardConfig::new(2, 71).unwrap(),
    }
}

struct OwnedChild {
    child: Child,
    reaped: bool,
}
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if !self.reaped {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

#[derive(Serialize)]
struct KillReceipt {
    nonce: [u8; 16],
    pid: u32,
    signal: Option<i32>,
    status: String,
    phase_accepted: bool,
}

impl OwnedChild {
    fn spawn(config: &wire::Config) -> Result<Self, String> {
        wire::write(&config.path("config.bin"), config)?;
        let log = File::create(config.path("child.log")).map_err(message)?;
        let child = ProcessCommand::new(std::env::current_exe().map_err(message)?)
            .args([CHILD_FILTER, "--exact", "--test-threads=1", "--nocapture"])
            .env(MARKER, config.path("config.bin"))
            .stdin(Stdio::null())
            .stdout(Stdio::from(log.try_clone().map_err(message)?))
            .stderr(Stdio::from(log))
            .spawn()
            .map_err(message)?;
        Ok(Self {
            child,
            reaped: false,
        })
    }
    fn kill_and_wait(
        &mut self,
        config: &wire::Config,
        phase_accepted: bool,
    ) -> Result<KillReceipt, String> {
        if let Some(status) = self.child.try_wait().map_err(message)? {
            self.reaped = true;
            return Err(format!("child exited before the owned kill: {status}"));
        }
        let killed = self.child.kill();
        let waited = self.child.wait();
        self.reaped = waited.is_ok();
        killed.map_err(message)?;
        let status = waited.map_err(message)?;
        #[cfg(unix)]
        let signal = {
            use std::os::unix::process::ExitStatusExt;
            status.signal()
        };
        #[cfg(not(unix))]
        let signal = None;
        #[cfg(unix)]
        require(signal == Some(9), "owned child did not exit by SIGKILL")?;
        require(!status.success(), "owned child exited successfully")?;
        Ok(KillReceipt {
            nonce: config.nonce,
            pid: self.child.id(),
            signal,
            status: status.to_string(),
            phase_accepted,
        })
    }
}

async fn wait_receipt(
    config: &wire::Config,
    child: &mut OwnedChild,
    name: &str,
    sequence: u8,
) -> Result<wire::Envelope<wire::Held>, String> {
    loop {
        if let Some(receipt) = wire::read(&config.path(name))? {
            wire::validate_identity(&receipt, config, child.child.id(), sequence)?;
            return Ok(receipt);
        }
        require(
            child.child.try_wait().map_err(message)?.is_none(),
            "child exited before phase receipt",
        )?;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn observe(config: &wire::Config, child: &mut OwnedChild) -> Result<wire::Held, String> {
    let held = wait_receipt(config, child, "held.bin", 0).await?;
    evidence::validate_held(&held.body)?;
    let command = wire::Envelope {
        nonce: config.nonce,
        pid: child.child.id(),
        sequence: 1,
        body: wire::Command::ConfirmHeld,
    };
    wire::write(&config.path("command.bin"), &command)?;
    let confirmed = wait_receipt(config, child, "confirmed.bin", 1).await?;
    require(
        confirmed.body == held.body,
        "phase evidence changed before kill",
    )?;
    evidence::validate_held(&confirmed.body)?;
    // An unverified/wrong phase cannot authorize a recovery claim. Cleanup
    // still kills and reaps the held process before reporting any failure.
    wire::validate_phase(&confirmed, config, child.child.id(), 1)?;
    Ok(confirmed.body)
}

#[derive(Serialize)]
struct Reopened {
    nonce: [u8; 16],
    old: evidence::View,
    sibling: evidence::Sibling,
    published: Meta,
    orphans_removed: bool,
}

async fn inspect_offline(config: &wire::Config, held: &wire::Held) -> Result<(), String> {
    // This function is outside the disposable observer/timeout. Storage::open
    // and every native read finish before their owners are released.
    let storage = Storage::open(
        config.database("target"),
        identity(),
        StorageOptions::default(),
        OpenMode::OpenExisting,
    )
    .await
    .map_err(message)?;
    let mut target = storage.open_group(GroupId::new(1)).map_err(message)?;
    let mut sibling = storage.open_group(GroupId::new(2)).map_err(message)?;
    let group = Arc::clone(&target.state_machine.group);
    let old = evidence::view(&group).await?;
    require(
        old == held.old,
        "reopened old generation/state/membership changed",
    )?;
    evidence::validate_old(&old)?;
    let current = target
        .state_machine
        .get_current_snapshot()
        .await
        .map_err(message)?
        .ok_or("old snapshot missing on reopen")?;
    require(
        current.meta == held.old_meta,
        "reopened snapshot descriptor changed",
    )?;
    let descriptor = read_descriptor(&group)
        .map_err(message)?
        .ok_or("reopened descriptor missing")?;
    require(
        descriptor.image == held.old_image && descriptor.generation == held.old.generation,
        "reopened descriptor identity changed",
    )?;
    require(
        target.log_store.read_vote().await.map_err(message)? == Some(Vote::new(3, 1)),
        "target vote changed",
    )?;
    require(
        target.log_store.read_committed().await.map_err(message)? == Some(evidence::id(3, 3)),
        "target committed marker changed",
    )?;
    require(
        target
            .log_store
            .try_get_log_entries(..)
            .await
            .map_err(message)?
            == evidence::entries(false),
        "target log changed",
    )?;
    let sibling_view = evidence::sibling(&mut sibling).await?;
    require(sibling_view == held.sibling, "reopened sibling changed")?;
    require(
        !final_path(&group, held.imported.image)
            .try_exists()
            .map_err(message)?,
        "orphan finalized image survived recovery",
    )?;
    {
        let prefix = keys::data(group.id, held.imported.image, &[]);
        let mut rows = group
            .shared
            .db
            .raw_iterator_cf(group.cf("state").map_err(message)?);
        rows.seek(&prefix);
        require(
            !rows.valid() || rows.key().is_some_and(|key| !key.starts_with(&prefix)),
            "orphan generation rows survived recovery",
        )?;
        rows.status().map_err(message)?;
    }
    let receipt = Reopened {
        nonce: config.nonce,
        old,
        sibling: sibling_view,
        published: current.meta.clone(),
        orphans_removed: true,
    };
    drop(current);
    drop(group);
    drop(target);
    drop(sibling);
    drop(storage);
    wire::write(&config.path("reopened.bin"), &receipt)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn after_rename_process_death_reopens_only_old_generation() {
    // Signal observers exist before a child or any native store is started.
    #[cfg(unix)]
    let mut terminate =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
    #[cfg(unix)]
    let mut interrupt =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).unwrap();
    let signal = async {
        #[cfg(unix)]
        tokio::select! { _ = terminate.recv() => {}, _ = interrupt.recv() => {} }
        #[cfg(not(unix))]
        std::future::pending::<()>().await;
    };
    let root = tempfile::tempdir().unwrap();
    let report = tempfile::Builder::new()
        .prefix("rockbee-snapshot-process-")
        .tempdir()
        .unwrap()
        .keep();
    let config = wire::Config {
        nonce: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            .to_be_bytes(),
        root: root.path().to_owned(),
        files: report,
    };
    config.validate().unwrap();
    let mut child = OwnedChild::spawn(&config).expect("spawn exact owned storage child");
    let observed = tokio::select! {
        result = observe(&config, &mut child) => result,
        _ = signal => Err("parent observation interrupted".to_owned()),
        _ = tokio::time::sleep(Duration::from_secs(45)) => Err("parent observation deadline".to_owned()),
    };
    let killed = child.kill_and_wait(&config, observed.is_ok());
    if let Ok(receipt) = &killed {
        wire::write(&config.path("killed.bin"), receipt).unwrap();
    }
    eprintln!(
        "snapshot_process_cut phase={observed:?} killed={} report={}",
        killed.is_ok(),
        config.files.display()
    );
    killed.expect("exact child killed and reaped before assertions/reopen");
    let held = observed.expect("verified AfterRename receipt after owned child cleanup");
    inspect_offline(&config, &held)
        .await
        .expect("offline old-generation recovery");
    eprintln!(
        "snapshot_process_reopen old_exact=true sibling_exact=true inactive_generation_removed=true report={}",
        config.files.display()
    );
}
