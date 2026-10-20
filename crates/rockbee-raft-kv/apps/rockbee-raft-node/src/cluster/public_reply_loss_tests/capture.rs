//! Bounded persisted bridge from observed wire events into the shared H1 model.

use super::{MAX_CAPTURE_BYTES, history};
use bytes::Bytes;
use history::{
    Attempt, EffectKnowledge, Execution, History, Notice, Operation, Outcome, Verdict, check,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    io::{Read, Write},
    path::Path,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct CapturedHistory {
    pub(super) key: Bytes,
    pub(super) value: Bytes,
    pub(super) put_invoked_at: u64,
    pub(super) committed_at: u64,
    pub(super) unknown_at: u64,
    pub(super) retired_get_invoked_at: u64,
    pub(super) retired_get_error_at: u64,
    pub(super) get_invoked_at: u64,
    pub(super) get_value_at: u64,
    pub(super) horizon: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct BackendCapturedHistory {
    pub(super) key: Bytes,
    pub(super) value: Bytes,
    pub(super) put_invoked_at: u64,
    pub(super) committed_at: u64,
    pub(super) unknown_at: u64,
    pub(super) get_invoked_at: u64,
    pub(super) get_value_at: u64,
    pub(super) horizon: u64,
}

pub(super) fn check_persisted_history(path: &Path, capture: &CapturedHistory) -> Verdict {
    let capture = persist_reload(path, capture);
    assert!(capture.key.len() <= rockbee_raft_protocol::MAX_KEY_BYTES);
    assert!(capture.value.len() <= rockbee_raft_protocol::MAX_VALUE_BYTES);
    check(&history(capture), 100_000).expect("captured history input must validate")
}

pub(super) fn check_persisted_backend_history(
    path: &Path,
    capture: &BackendCapturedHistory,
) -> Verdict {
    let capture = persist_reload(path, capture);
    assert!(capture.key.len() <= rockbee_raft_protocol::MAX_KEY_BYTES);
    assert!(capture.value.len() <= rockbee_raft_protocol::MAX_VALUE_BYTES);
    check(&backend_history(capture), 100_000).expect("captured backend history input must validate")
}

fn persist_reload<T>(path: &Path, capture: &T) -> T
where
    T: Serialize + DeserializeOwned,
{
    let bytes = postcard::to_stdvec(capture).expect("capture must serialize");
    assert!(
        bytes.len() <= MAX_CAPTURE_BYTES,
        "captured history must stay bounded"
    );
    let temporary = path.with_extension("partial");
    let mut output = std::fs::File::create(&temporary).expect("create bounded capture file");
    output
        .write_all(&bytes)
        .expect("write bounded capture file");
    output.sync_all().expect("sync bounded capture file");
    std::fs::rename(temporary, path).expect("publish bounded capture file");
    let input = std::fs::File::open(path).expect("open persisted capture file");
    assert!(input.metadata().expect("capture metadata").len() <= MAX_CAPTURE_BYTES as u64);
    let mut bytes = Vec::new();
    input
        .take(MAX_CAPTURE_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .expect("read capture");
    assert!(
        bytes.len() <= MAX_CAPTURE_BYTES,
        "persisted capture read must stay bounded"
    );
    let (capture, remaining) = postcard::take_from_bytes::<T>(&bytes).expect("decode capture");
    assert!(
        remaining.is_empty(),
        "captured history must have no trailing bytes"
    );
    capture
}

fn history(capture: CapturedHistory) -> History {
    History {
        initial: vec![(capture.key.to_vec(), None)],
        attempts: vec![
            Attempt {
                id: 1,
                key: capture.key.to_vec(),
                operation: Operation::Put(capture.value.to_vec()),
                invoked_at: capture.put_invoked_at,
                notice: Some(Notice {
                    at: capture.unknown_at,
                    outcome: Outcome::Unknown,
                }),
                execution: Some(Execution {
                    ended_by: capture.committed_at,
                    effect: EffectKnowledge::Applied,
                }),
            },
            Attempt {
                id: 2,
                key: capture.key.to_vec(),
                operation: Operation::Get,
                invoked_at: capture.retired_get_invoked_at,
                notice: Some(Notice {
                    at: capture.retired_get_error_at,
                    outcome: Outcome::ReadError,
                }),
                execution: None,
            },
            Attempt {
                id: 3,
                key: capture.key.to_vec(),
                operation: Operation::Get,
                invoked_at: capture.get_invoked_at,
                notice: Some(Notice {
                    at: capture.get_value_at,
                    outcome: Outcome::Value(Some(capture.value.to_vec())),
                }),
                execution: None,
            },
        ],
        horizon: capture.horizon,
        complete: true,
    }
}

fn backend_history(capture: BackendCapturedHistory) -> History {
    History {
        initial: vec![(capture.key.to_vec(), None)],
        attempts: vec![
            Attempt {
                id: 1,
                key: capture.key.to_vec(),
                operation: Operation::Put(capture.value.to_vec()),
                invoked_at: capture.put_invoked_at,
                notice: Some(Notice {
                    at: capture.unknown_at,
                    outcome: Outcome::Unknown,
                }),
                execution: Some(Execution {
                    ended_by: capture.committed_at,
                    effect: EffectKnowledge::Applied,
                }),
            },
            Attempt {
                id: 2,
                key: capture.key.to_vec(),
                operation: Operation::Get,
                invoked_at: capture.get_invoked_at,
                notice: Some(Notice {
                    at: capture.get_value_at,
                    outcome: Outcome::Value(Some(capture.value.to_vec())),
                }),
                execution: None,
            },
        ],
        horizon: capture.horizon,
        complete: true,
    }
}
