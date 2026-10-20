//! One exact native call receipt; observation never schedules or cancels work.
#![allow(clippy::unwrap_used)]
use super::*;

type NativeResult = Result<
    openraft::raft::ClientWriteResponse<crate::types::TypeConfig>,
    openraft::error::RaftError<u64, openraft::error::ClientWriteError<u64, BasicNode>>,
>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct NativeMembershipIdentity {
    pub(super) node: NodeId,
    pub(super) group: GroupId,
    pub(super) job: MoveJobId,
    pub(super) target: BTreeSet<u64>,
}

#[derive(Clone, Debug)]
pub(super) struct NativeMembershipCall {
    pub(super) identity: NativeMembershipIdentity,
    pub(super) invocation: u64,
    pub(super) entered_at: Instant,
    pub(super) returned_at: Option<Instant>,
    // ClientWriteResponse does not implement Clone. Preserve the full actual
    // Debug value and its Ok/Err arm without consuming or replacing the result.
    pub(super) result: Option<Result<String, String>>,
}

pub(super) struct NativeMembershipObservation {
    expected: NativeMembershipIdentity,
    call: Mutex<Option<NativeMembershipCall>>,
}

impl NativeMembershipObservation {
    pub(super) fn new(expected: NativeMembershipIdentity) -> Self {
        Self {
            expected,
            call: Mutex::new(None),
        }
    }

    pub(super) fn snapshot(&self) -> Option<NativeMembershipCall> {
        self.call.lock().unwrap().clone()
    }

    pub(super) fn enter(self: &Arc<Self>, identity: NativeMembershipIdentity) -> Option<Arc<Self>> {
        if identity != self.expected {
            return None;
        }
        let mut call = self.call.lock().unwrap();
        if call.is_some() {
            return None;
        }
        static NEXT_INVOCATION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let invocation = NEXT_INVOCATION
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .expect("native observation invocation IDs exhausted");
        *call = Some(NativeMembershipCall {
            identity,
            invocation,
            entered_at: Instant::now(),
            returned_at: None,
            result: None,
        });
        Some(Arc::clone(self))
    }

    pub(super) fn finish(&self, result: &NativeResult) {
        let returned_at = Instant::now();
        let mut call = self.call.lock().unwrap();
        let call = call
            .as_mut()
            .expect("only an entered invocation can finish");
        assert!(
            call.result.is_none(),
            "native invocation records one result"
        );
        call.returned_at = Some(returned_at);
        call.result = Some(match result {
            Ok(response) => Ok(format!("{response:?}")),
            Err(error) => Err(format!("{error:?}")),
        });
    }
}
