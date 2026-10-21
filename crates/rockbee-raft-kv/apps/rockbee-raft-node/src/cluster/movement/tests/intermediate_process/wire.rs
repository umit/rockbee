//! Private, bounded, atomic fixture files; diagnostics are never phase proof.
use super::*;
use serde::de::DeserializeOwned;
const MAX_BYTES: u64 = 64 * 1024;
pub(super) const MAX_COMMANDS: u8 = 8;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Config {
    pub nonce: [u8; 16],
    pub incarnation: u8,
    pub directory: ClusterDirectory,
    pub endpoints: Vec<NodeEndpoint>,
    pub database: PathBuf,
    pub files: PathBuf,
    pub reopen: bool,
    pub request: MoveReplica,
}
impl Config {
    pub(super) fn validate(&self) {
        assert_ne!(self.nonce, [0; 16]);
        assert!(matches!(self.incarnation, 1 | 2));
        assert_eq!(self.reopen, self.incarnation == 2);
        self.directory.validate().unwrap();
        assert_eq!(self.directory.revision, 1);
        assert_eq!(self.endpoints.len(), 4);
        assert_eq!(&self.endpoints[..3], self.directory.nodes.as_slice());
        assert_eq!(self.request.data_group, GROUP);
        assert_eq!(self.request.remove_node.get(), 3);
        assert_eq!(self.request.add_node, self.endpoints[3]);
        assert_eq!(self.request.expected_directory_revision, 1);
        self.request.derive_intent(&self.directory).unwrap();
        assert!(self.database.is_absolute() && self.files.is_absolute());
        assert!(self.database.starts_with(self.files.parent().unwrap()));
    }
    pub(super) fn path(&self, name: &str) -> PathBuf {
        self.files.join(name)
    }
    pub(super) fn envelope<T>(&self, sequence: u8, pid: u32, body: T) -> Envelope<T> {
        Envelope {
            nonce: self.nonce,
            incarnation: self.incarnation,
            sequence,
            pid,
            node: NodeId::new(2).unwrap(),
            group: GROUP,
            job: self.request.job_id,
            body,
        }
    }
    pub(super) fn check<T>(&self, envelope: &Envelope<T>, sequence: u8, pid: u32) {
        assert_eq!(envelope.nonce, self.nonce);
        assert_eq!(envelope.incarnation, self.incarnation);
        assert_eq!(envelope.sequence, sequence);
        assert_eq!(envelope.pid, pid);
        assert_eq!(envelope.node.get(), 2);
        assert_eq!(envelope.group, GROUP);
        assert_eq!(envelope.job, self.request.job_id);
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Envelope<T> {
    pub nonce: [u8; 16],
    pub incarnation: u8,
    pub sequence: u8,
    pub pid: u32,
    pub node: NodeId,
    pub group: GroupId,
    pub job: MoveJobId,
    pub body: T,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) enum Command {
    Arm { baseline: LogId },
    ConfirmHeld,
    ObserveFinal { published: LogId, baseline: LogId },
    Stop,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct Held {
    pub applied: Option<LogId>,
    pub membership: StoredMembership,
    pub invocation: u64,
    pub runner: String,
    pub native_entered: bool,
    pub native_returned: bool,
    pub native_result: bool,
    pub runner_finished: bool,
    pub permits_available: usize,
    pub stopping: bool,
    // Child-local ordering only: entry preceded hook observation; confirmation
    // rechecks the same retained entry timestamp without exporting Instant.
    pub entry_before_hook: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) enum Receipt {
    Ready {
        endpoint: NodeEndpoint,
        reopen: bool,
        runner_count: usize,
    },
    Armed {
        source: Box<AppliedMembership>,
    },
    Held(Held),
    LocalTarget {
        applied: Option<LogId>,
        membership: StoredMembership,
    },
    Stopped,
}
/// Cleanup evidence is deliberately separate from phase evidence. A completed
/// phase test must not turn a forced child kill into a successful cleanup.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct CleanupReceipt {
    pub complete: bool,
    pub child: Option<ChildCleanup>,
    pub pending_node_startups_joined: bool,
    pub pending_gateway_startup_joined: bool,
    /// A constructor or JoinHandle error persists after its completed handle is
    /// removed from the fixture's owned startup slot.
    pub node_startup_failure: Option<String>,
    pub gateway_startup_failure: Option<String>,
}

/// The exact way the owned child finished during parent cleanup.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) enum ChildCleanup {
    /// The scenario's asserted SIGKILL had already reaped this child.
    ExpectedFaultReaped,
    /// Stop was acknowledged and the child then exited successfully.
    Graceful { sequence: u8, status: String },
    /// The child exited before a matching Stop receipt could be observed.
    UnexpectedExit { sequence: u8, status: String },
    /// Cleanup had to force-kill a child that did not stop in time.
    ForcedKill { sequence: u8, status: String },
}
pub(super) fn write<T: Serialize>(path: &Path, value: &T) {
    let bytes = postcard::to_stdvec(value).unwrap();
    assert!(u64::try_from(bytes.len()).unwrap() <= MAX_BYTES);
    let temporary = path.with_extension("partial");
    let mut file = std::fs::File::create(&temporary).unwrap();
    std::io::Write::write_all(&mut file, &bytes).unwrap();
    file.sync_all().unwrap();
    std::fs::rename(temporary, path).unwrap();
}
pub(super) fn read<T: DeserializeOwned>(path: &Path) -> Option<T> {
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return None,
        Err(error) => panic!("private receipt open failed: {error}"),
    };
    assert!(
        file.metadata().unwrap().len() <= MAX_BYTES,
        "oversized private fixture file"
    );
    let mut bytes = Vec::new();
    std::io::Read::read_to_end(&mut std::io::Read::take(file, MAX_BYTES + 1), &mut bytes).unwrap();
    assert!(u64::try_from(bytes.len()).unwrap() <= MAX_BYTES);
    let (value, remaining) = postcard::take_from_bytes(&bytes).unwrap();
    assert!(remaining.is_empty(), "trailing private fixture bytes");
    Some(value)
}
pub(super) fn emit(config: &Config, sequence: u8, kind: &str, body: Receipt) {
    write(
        &config.path(&format!("receipt-{sequence}-{kind}.bin")),
        &config.envelope(sequence, std::process::id(), body),
    );
}
