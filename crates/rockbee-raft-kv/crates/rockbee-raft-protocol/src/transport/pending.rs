use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::sync::{OwnedSemaphorePermit, oneshot};

use super::{Delivery, TransportFailure, TransportFailureKind};

// Shared by queue, pending entry and completion. Metadata/count stay charged
// until the last actual owner ends, including unpolled terminal results.
pub(super) struct Admission {
    pub _count: OwnedSemaphorePermit,
    pub _metadata: OwnedSemaphorePermit,
}

pub(super) struct Completion<Resp> {
    pub result: Result<Resp, TransportFailure>,
    pub _count: Arc<Admission>,
    pub _receive: Option<OwnedSemaphorePermit>,
}

/// Eager work's observer. Dropping this future does not cancel submission.
///
/// Awaiting yields `Result<Resp, TransportFailure>`: `Ok` carries a validated
/// matching response; `Err` carries explicit transport delivery certainty.
/// The caller takes ownership of returned response bytes when polling succeeds.
#[must_use = "dropping a handle discards observation but does not cancel accepted work"]
pub struct PendingResponse<Resp> {
    receiver: oneshot::Receiver<Completion<Resp>>,
}

impl<Resp> PendingResponse<Resp> {
    pub(super) fn new(receiver: oneshot::Receiver<Completion<Resp>>) -> Self {
        Self { receiver }
    }
}

impl<Resp> Future for PendingResponse<Resp> {
    type Output = Result<Resp, TransportFailure>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.receiver).poll(cx) {
            Poll::Ready(Ok(completion)) => Poll::Ready(completion.result),
            Poll::Ready(Err(_)) => Poll::Ready(Err(TransportFailure {
                delivery: Delivery::MayHaveBeenSent,
                reason: TransportFailureKind::Closed,
            })),
            Poll::Pending => Poll::Pending,
        }
    }
}
