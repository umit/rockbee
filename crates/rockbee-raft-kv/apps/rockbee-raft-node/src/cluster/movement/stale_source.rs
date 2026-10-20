//! One captured Source boundary and the exact stale add-learner receipt.
#![allow(clippy::unwrap_used)]
use super::*;
use std::net::SocketAddr;
use tokio::sync::watch;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct SourceIdentity {
    pub(super) node: NodeId,
    pub(super) group: GroupId,
    pub(super) job: MoveJobId,
    pub(super) learner: NodeId,
    pub(super) peer: SocketAddr,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct SourceReceipt {
    pub(super) identity: SourceIdentity,
    pub(super) observed: AppliedMembership,
}

#[derive(Clone, Default)]
struct PauseState {
    reached: Option<SourceReceipt>,
    released: bool,
    exited: bool,
}

pub(super) struct SourcePause {
    expected: SourceIdentity,
    state: watch::Sender<PauseState>,
}

impl SourcePause {
    pub(super) fn new(expected: SourceIdentity) -> Self {
        Self {
            expected,
            state: watch::channel(PauseState::default()).0,
        }
    }

    pub(super) async fn wait_reached(&self) -> SourceReceipt {
        let mut state = self.state.subscribe();
        state
            .wait_for(|state| state.reached.is_some())
            .await
            .unwrap();
        state.borrow().reached.clone().unwrap()
    }

    pub(super) fn held_receipt(&self) -> Option<SourceReceipt> {
        let state = self.state.borrow();
        (!state.exited).then(|| state.reached.clone()).flatten()
    }

    pub(super) fn release(&self) {
        self.state.send_modify(|state| state.released = true);
    }

    pub(super) async fn wait_exited(&self) {
        self.state
            .subscribe()
            .wait_for(|state| state.exited)
            .await
            .unwrap();
    }

    async fn hold(&self, observed: &AppliedMembership) {
        self.state.send_modify(|state| {
            state.reached = Some(SourceReceipt {
                identity: self.expected.clone(),
                observed: observed.clone(),
            });
        });
        self.state
            .subscribe()
            .wait_for(|state| state.released)
            .await
            .unwrap();
        self.state.send_modify(|state| state.exited = true);
    }
}

pub(super) async fn pause_source(
    service: &NodeService,
    intent: &MoveIntent,
    observed: &AppliedMembership,
) {
    let identity = SourceIdentity {
        node: service.identity.node_id,
        group: intent.data_group,
        job: intent.job_id,
        learner: intent.add_node.node_id,
        peer: intent.add_node.peer_address,
    };
    let pause = {
        let mut slot = service.movement.before_source_learner.lock().unwrap();
        if slot
            .as_ref()
            .is_some_and(|pause| pause.expected == identity)
        {
            slot.take()
        } else {
            None
        }
    };
    // Neither the one-shot slot nor storage/admission locks cross the await.
    if let Some(pause) = pause {
        pause.hold(observed).await;
    }
}

pub(super) type NativeLearnerError =
    openraft::error::RaftError<u64, openraft::error::ClientWriteError<u64, BasicNode>>;

#[derive(Clone, Debug)]
pub(super) struct NativeLearnerSuccess {
    pub(super) log_id: LogId,
    pub(super) membership: Option<openraft::Membership<u64, BasicNode>>,
}

#[derive(Clone, Debug)]
pub(super) struct NativeLearnerCall {
    pub(super) identity: SourceIdentity,
    pub(super) blocking: bool,
    pub(super) entered_at: Instant,
    pub(super) returned_at: Option<Instant>,
    pub(super) result: Option<Result<NativeLearnerSuccess, NativeLearnerError>>,
}

pub(super) struct NativeLearnerObservation {
    expected: SourceIdentity,
    call: Mutex<Option<NativeLearnerCall>>,
}

impl NativeLearnerObservation {
    pub(super) fn new(expected: SourceIdentity) -> Self {
        Self {
            expected,
            call: Mutex::new(None),
        }
    }

    pub(super) fn snapshot(&self) -> Option<NativeLearnerCall> {
        self.call.lock().unwrap().clone()
    }

    pub(super) fn enter(
        self: &Arc<Self>,
        identity: SourceIdentity,
        blocking: bool,
    ) -> Option<Arc<Self>> {
        if identity != self.expected || blocking {
            return None;
        }
        let mut call = self.call.lock().unwrap();
        // Keep the first matching invocation permanently; subsequent calls
        // cannot replace its identity, entry boundary, or actual result.
        if call.is_some() {
            return None;
        }
        *call = Some(NativeLearnerCall {
            identity,
            blocking,
            entered_at: Instant::now(),
            returned_at: None,
            result: None,
        });
        Some(Arc::clone(self))
    }

    pub(super) fn finish(
        &self,
        result: &Result<
            openraft::raft::ClientWriteResponse<crate::types::TypeConfig>,
            NativeLearnerError,
        >,
    ) {
        let returned_at = Instant::now();
        let mut call = self.call.lock().unwrap();
        let call = call
            .as_mut()
            .expect("only the original entered invocation can finish");
        assert!(
            call.result.is_none(),
            "native invocation records one actual result"
        );
        call.returned_at = Some(returned_at);
        call.result = Some(match result {
            Ok(response) => Ok(NativeLearnerSuccess {
                log_id: response.log_id,
                membership: response.membership.clone(),
            }),
            Err(error) => Err(error.clone()),
        });
    }
}
