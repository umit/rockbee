//! Fixed client counters, independent of connections and response observers.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use super::{Delivery, TransportFailure};
use crate::{KvResponse, MutationOutcome};

/// Mutation call totals and currently unfinished admitted operations.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MutationStats {
    /// Calls reaching eager transport submission.
    pub attempted: u64,
    /// Calls accepted into the connection generation.
    pub admitted: u64,
    /// Calls rejected locally before admission.
    pub admission_failed: u64,
    /// Admitted calls without a terminal transport result.
    pub pending: u64,
    /// Received durable consensus acknowledgements.
    pub committed: u64,
    /// Received refusals proving no consensus submission.
    pub rejected: u64,
    /// Accepted calls proven not handed to the transport.
    pub not_sent: u64,
    /// Calls whose mutation effect may have occurred.
    pub unknown: u64,
}

/// Read call totals; an empty stored value is a hit.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReadStats {
    /// Calls reaching transport submission.
    pub attempted: u64,
    /// Calls accepted into the connection generation.
    pub admitted: u64,
    /// Calls rejected locally before admission.
    pub admission_failed: u64,
    /// Admitted calls without a terminal transport result.
    pub pending: u64,
    /// Received present values, including empty values.
    pub hit: u64,
    /// Received missing-key results.
    pub miss: u64,
    /// Received typed read failures from the remote service.
    pub remote_error: u64,
    /// Accepted reads ending in a transport failure.
    pub transport_error: u64,
}

/// Concurrent observations may straddle updates; conservation holds at quiescence
/// while `overflowed` is false.
/// Pending describes logical completion, not retained transport byte/count permits.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ClientStatsSnapshot {
    /// PUT counters.
    pub put: MutationStats,
    /// GET counters.
    pub get: ReadStats,
    /// DELETE counters.
    pub delete: MutationStats,
    /// Sticky indication that a counter could not represent an update exactly.
    pub overflowed: bool,
}

#[derive(Debug, Default)]
struct CommonCounters {
    attempted: AtomicU64,
    admitted: AtomicU64,
    admission_failed: AtomicU64,
    pending: AtomicU64,
}

#[derive(Debug, Default)]
struct MutationCounters {
    common: CommonCounters,
    committed: AtomicU64,
    rejected: AtomicU64,
    not_sent: AtomicU64,
    unknown: AtomicU64,
}

#[derive(Debug, Default)]
struct ReadCounters {
    common: CommonCounters,
    hit: AtomicU64,
    miss: AtomicU64,
    remote_error: AtomicU64,
    transport_error: AtomicU64,
}

#[derive(Debug, Default)]
struct Counters {
    put: MutationCounters,
    get: ReadCounters,
    delete: MutationCounters,
    overflowed: AtomicBool,
}

#[derive(Clone, Copy)]
enum Operation {
    Put,
    Get,
    Delete,
}

impl Counters {
    fn common(&self, operation: Operation) -> &CommonCounters {
        match operation {
            Operation::Put => &self.put.common,
            Operation::Get => &self.get.common,
            Operation::Delete => &self.delete.common,
        }
    }

    fn increment(&self, counter: &AtomicU64) {
        if counter
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .is_err()
        {
            self.overflowed.store(true, Ordering::Relaxed);
        }
    }

    fn decrement(&self, counter: &AtomicU64) {
        if counter
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_sub(1)
            })
            .is_err()
        {
            self.overflowed.store(true, Ordering::Relaxed);
        }
    }
}

impl MutationCounters {
    fn snapshot(&self) -> MutationStats {
        MutationStats {
            attempted: self.common.attempted.load(Ordering::Relaxed),
            admitted: self.common.admitted.load(Ordering::Relaxed),
            admission_failed: self.common.admission_failed.load(Ordering::Relaxed),
            pending: self.common.pending.load(Ordering::Relaxed),
            committed: self.committed.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            not_sent: self.not_sent.load(Ordering::Relaxed),
            unknown: self.unknown.load(Ordering::Relaxed),
        }
    }
}

impl ReadCounters {
    fn snapshot(&self) -> ReadStats {
        ReadStats {
            attempted: self.common.attempted.load(Ordering::Relaxed),
            admitted: self.common.admitted.load(Ordering::Relaxed),
            admission_failed: self.common.admission_failed.load(Ordering::Relaxed),
            pending: self.common.pending.load(Ordering::Relaxed),
            hit: self.hit.load(Ordering::Relaxed),
            miss: self.miss.load(Ordering::Relaxed),
            remote_error: self.remote_error.load(Ordering::Relaxed),
            transport_error: self.transport_error.load(Ordering::Relaxed),
        }
    }
}

/// Clone-shared fixed statistics. This handle never owns a connection or worker.
#[derive(Clone, Debug, Default)]
pub struct ClientStatsHandle {
    counters: Arc<Counters>,
}

/// An opaque, single-submission KV counter selection. No user callbacks run.
/// Select the factory corresponding to the submitted KV operation.
/// The token owns only counters, never a request, connection or completion handle.
pub struct SubmissionStats<Resp> {
    counters: Arc<Counters>,
    operation: Operation,
    // Only the built-in KV classifier can populate this private function pointer.
    classify: fn(&Counters, Operation, &Result<Resp, TransportFailure>),
}

impl ClientStatsHandle {
    /// Reads fixed, saturating counters without retaining transport resources.
    /// Relaxed atomic reads are not a transaction over concurrent updates.
    #[must_use]
    pub fn snapshot(&self) -> ClientStatsSnapshot {
        ClientStatsSnapshot {
            put: self.counters.put.snapshot(),
            get: self.counters.get.snapshot(),
            delete: self.counters.delete.snapshot(),
            overflowed: self.counters.overflowed.load(Ordering::Relaxed),
        }
    }

    /// Selects PUT counters; merely making the token does not count a call.
    #[must_use]
    pub fn for_put(&self) -> SubmissionStats<KvResponse> {
        self.token(Operation::Put)
    }

    /// Selects GET counters; merely making the token does not count a call.
    #[must_use]
    pub fn for_get(&self) -> SubmissionStats<KvResponse> {
        self.token(Operation::Get)
    }

    /// Selects DELETE counters; merely making the token does not count a call.
    #[must_use]
    pub fn for_delete(&self) -> SubmissionStats<KvResponse> {
        self.token(Operation::Delete)
    }

    fn token(&self, operation: Operation) -> SubmissionStats<KvResponse> {
        SubmissionStats {
            counters: Arc::clone(&self.counters),
            operation,
            classify: classify_kv,
        }
    }
}

impl<Resp> SubmissionStats<Resp> {
    pub(super) fn attempted(&self) {
        self.counters
            .increment(&self.counters.common(self.operation).attempted);
    }

    pub(super) fn admission_failed(&self) {
        self.counters
            .increment(&self.counters.common(self.operation).admission_failed);
    }

    pub(super) fn admitted(&self) {
        let common = self.counters.common(self.operation);
        self.counters.increment(&common.admitted);
        self.counters.increment(&common.pending);
    }

    pub(super) fn finish(self, result: &Result<Resp, TransportFailure>) {
        // Fixed built-in enum classification and atomics only. Preserve the
        // existing state-lock completion ownership and close quiescence.
        (self.classify)(&self.counters, self.operation, result);
        self.counters
            .decrement(&self.counters.common(self.operation).pending);
    }
}

fn classify_kv(
    counters: &Counters,
    operation: Operation,
    result: &Result<KvResponse, TransportFailure>,
) {
    let mutation = match operation {
        Operation::Put => &counters.put,
        Operation::Delete => &counters.delete,
        Operation::Get => {
            let counter = match result {
                Ok(KvResponse::Value(Some(_))) => &counters.get.hit,
                Ok(KvResponse::Value(None)) => &counters.get.miss,
                Ok(KvResponse::ReadError(_)) => &counters.get.remote_error,
                Ok(KvResponse::Mutation(_)) | Err(_) => &counters.get.transport_error,
            };
            counters.increment(counter);
            return;
        }
    };
    let counter = match result {
        Ok(KvResponse::Mutation(MutationOutcome::Committed)) => &mutation.committed,
        Ok(KvResponse::Mutation(MutationOutcome::Rejected(_))) => &mutation.rejected,
        Ok(KvResponse::Mutation(MutationOutcome::NotSent(_))) => &mutation.not_sent,
        Err(TransportFailure {
            delivery: Delivery::NotSent,
            ..
        }) => &mutation.not_sent,
        Ok(KvResponse::Mutation(MutationOutcome::Unknown(_)))
        | Ok(KvResponse::Value(_) | KvResponse::ReadError(_))
        | Err(TransportFailure {
            delivery: Delivery::MayHaveBeenSent,
            ..
        }) => &mutation.unknown,
    };
    counters.increment(counter);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_saturate_and_report_overflow_without_wrapping() {
        let stats = ClientStatsHandle::default();
        stats
            .counters
            .put
            .common
            .attempted
            .store(u64::MAX - 1, Ordering::Relaxed);
        let token = stats.for_put();
        token.attempted();
        assert_eq!(stats.snapshot().put.attempted, u64::MAX);
        assert!(!stats.snapshot().overflowed);
        token.attempted();
        assert_eq!(stats.snapshot().put.attempted, u64::MAX);
        assert!(stats.snapshot().overflowed);
        stats.counters.increment(&stats.counters.get.hit);
        assert_eq!(stats.snapshot().get.hit, 1);
        assert!(stats.snapshot().overflowed);
    }

    #[test]
    fn pending_gauge_never_wraps_on_an_unrepresentable_update() {
        let stats = ClientStatsHandle::default();
        stats
            .counters
            .put
            .common
            .pending
            .store(u64::MAX, Ordering::Relaxed);
        stats.for_put().admitted();
        assert_eq!(stats.snapshot().put.pending, u64::MAX);
        assert!(stats.snapshot().overflowed);
        stats
            .counters
            .put
            .common
            .pending
            .store(0, Ordering::Relaxed);
        stats.counters.decrement(&stats.counters.put.common.pending);
        assert_eq!(stats.snapshot().put.pending, 0);
    }
}
