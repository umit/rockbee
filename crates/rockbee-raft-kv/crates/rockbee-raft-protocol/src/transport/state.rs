use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, MutexGuard},
};
use tokio::{
    sync::{Notify, Semaphore, oneshot, watch},
    time::Instant,
};

use super::{
    Delivery, LinkConfig, SubmissionStats, TransportFailure, TransportFailureKind,
    pending::{Admission, Completion},
};
use crate::RequestId;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Phase {
    Queued,
    Handed,
}

pub(super) struct Entry<Resp, Expected> {
    pub deadline: Instant,
    pub expected: Expected,
    pub phase: Phase,
    pub sender: oneshot::Sender<Completion<Resp>>,
    pub count: Arc<Admission>,
    pub stats: Option<SubmissionStats<Resp>>,
}

pub(super) struct State<Resp, Expected> {
    pub entries: BTreeMap<RequestId, Entry<Resp, Expected>>,
    pub deadlines: BTreeSet<(Instant, RequestId)>,
    pub next_id: Option<u64>,
    pub retired: Option<TransportFailureKind>,
}

pub(super) struct Shared<Resp, Expected> {
    state: Mutex<State<Resp, Expected>>,
    pub count: Arc<Semaphore>,
    pub request_bytes: Arc<Semaphore>,
    pub receive_bytes: Arc<Semaphore>,
    pub deadline_changed: Notify,
    pub stop: watch::Sender<bool>,
}

impl<Resp, Expected> Shared<Resp, Expected> {
    pub(super) fn new(config: LinkConfig) -> Self {
        Self {
            state: Mutex::new(State {
                entries: BTreeMap::new(),
                deadlines: BTreeSet::new(),
                next_id: Some(1),
                retired: None,
            }),
            count: Arc::new(Semaphore::new(config.max_outstanding)),
            request_bytes: Arc::new(Semaphore::new(config.request_bytes)),
            receive_bytes: Arc::new(Semaphore::new(config.receive_bytes)),
            deadline_changed: Notify::new(),
            stop: watch::channel(false).0,
        }
    }

    pub(super) fn lock(&self) -> MutexGuard<'_, State<Resp, Expected>> {
        self.state
            .lock()
            .expect("pending state lock is never held across user code or await")
    }

    pub(super) fn retire(&self, reason: TransportFailureKind) {
        let retired = {
            let mut state = self.lock();
            self.retire_locked(&mut state, reason)
        };
        if retired {
            log::debug!("raft transport retired connection generation kind={reason:?}");
        }
    }

    pub(super) fn retire_locked(
        &self,
        state: &mut State<Resp, Expected>,
        reason: TransportFailureKind,
    ) -> bool {
        if state.retired.is_some() {
            return false;
        }
        state.retired = Some(reason);
        state.deadlines.clear();
        for (_, entry) in std::mem::take(&mut state.entries) {
            fail(entry, reason);
        }
        self.stop.send_replace(true);
        self.deadline_changed.notify_one();
        true
    }

    pub(super) fn expire(&self, id: RequestId, now: Instant) {
        let expired = {
            let mut state = self.lock();
            let Some(entry) = state.entries.get(&id) else {
                return;
            };
            if entry.deadline > now {
                return;
            }
            if entry.phase == Phase::Handed {
                self.retire_locked(&mut state, TransportFailureKind::Deadline);
                "handed"
            } else if let Some(entry) = state.entries.remove(&id) {
                state.deadlines.remove(&(entry.deadline, id));
                fail(entry, TransportFailureKind::Deadline);
                "queued"
            } else {
                return;
            }
        };
        log::debug!(
            "raft transport request deadline expired request_id={} phase={expired}",
            id.get()
        );
    }

    pub(super) fn handoff(&self, id: RequestId) -> bool {
        let mut state = self.lock();
        let Some(entry) = state.entries.get_mut(&id) else {
            return false;
        };
        if entry.deadline <= Instant::now() {
            let entry = state.entries.remove(&id).expect("entry just inspected");
            state.deadlines.remove(&(entry.deadline, id));
            fail(entry, TransportFailureKind::Deadline);
            return false;
        }
        entry.phase = Phase::Handed;
        true
    }
}

fn fail<Resp, Expected>(entry: Entry<Resp, Expected>, reason: TransportFailureKind) {
    let delivery = match entry.phase {
        Phase::Queued => Delivery::NotSent,
        Phase::Handed => Delivery::MayHaveBeenSent,
    };
    let result = Err(TransportFailure { delivery, reason });
    if let Some(stats) = entry.stats {
        stats.finish(&result);
    }
    let _ = entry.sender.send(Completion {
        result,
        _count: entry.count,
        _receive: None,
    });
}
