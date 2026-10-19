use rockbee_raft_protocol::{
    Failure, FailureReason, KvResponse, MutationOutcome,
    transport::{Delivery, PendingResponse, TransportFailureKind},
};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

#[must_use = "dropping a mutation handle discards observation without cancelling the mutation"]
pub struct MutationHandle {
    pub(super) pending: PendingResponse<KvResponse>,
}

impl Future for MutationHandle {
    type Output = MutationOutcome;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.pending).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(KvResponse::Mutation(outcome))) => {
                log::trace!(
                    "raft client mutation completed outcome={}",
                    mutation_outcome_name(&outcome)
                );
                Poll::Ready(outcome)
            }
            Poll::Ready(Ok(_)) => {
                log::debug!("raft client mutation protocol mismatch");
                Poll::Ready(MutationOutcome::Unknown(failure(
                    TransportFailureKind::Protocol,
                )))
            }
            Poll::Ready(Err(error)) => {
                log::debug!(
                    "raft client mutation transport failure delivery={:?} kind={:?}",
                    error.delivery,
                    error.reason
                );
                let diagnostic = failure(error.reason);
                Poll::Ready(match error.delivery {
                    Delivery::NotSent => MutationOutcome::NotSent(diagnostic),
                    Delivery::MayHaveBeenSent => MutationOutcome::Unknown(diagnostic),
                })
            }
        }
    }
}

fn mutation_outcome_name(outcome: &MutationOutcome) -> &'static str {
    match outcome {
        MutationOutcome::Committed => "committed",
        MutationOutcome::NotSent(_) => "not_sent",
        MutationOutcome::Rejected(_) => "rejected",
        MutationOutcome::Unknown(_) => "unknown",
    }
}

fn failure(reason: TransportFailureKind) -> Failure {
    let (reason, message) = match reason {
        TransportFailureKind::Deadline => (
            FailureReason::Deadline,
            "request deadline expired; connection retired if handed",
        ),
        TransportFailureKind::Disconnected => {
            (FailureReason::Disconnected, "connection disconnected")
        }
        TransportFailureKind::Io => (FailureReason::Disconnected, "connection I/O failed"),
        TransportFailureKind::Protocol => (
            FailureReason::Protocol,
            "invalid response; connection retired",
        ),
        TransportFailureKind::Closed => (FailureReason::Disconnected, "connection closed"),
        TransportFailureKind::IdExhausted => (
            FailureReason::Unavailable,
            "request identifiers exhausted; connection retired",
        ),
    };
    Failure {
        reason,
        message: message.to_owned(),
    }
}
