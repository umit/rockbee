use rockbee_protocol::{ServiceRole, StatusSnapshot};
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) struct RpcStats {
    role: ServiceRole,
    connections_current: AtomicU64,
    connections_accepted_total: AtomicU64,
    connections_rejected_total: AtomicU64,
    connections_failed_total: AtomicU64,
    requests_completed_total: AtomicU64,
    error_responses_total: AtomicU64,
}

impl RpcStats {
    pub(crate) fn new(role: ServiceRole) -> Self {
        Self {
            role,
            connections_current: AtomicU64::new(0),
            connections_accepted_total: AtomicU64::new(0),
            connections_rejected_total: AtomicU64::new(0),
            connections_failed_total: AtomicU64::new(0),
            requests_completed_total: AtomicU64::new(0),
            error_responses_total: AtomicU64::new(0),
        }
    }

    pub(crate) fn admitted_connection(&self) -> CurrentConnection<'_> {
        self.connections_current.fetch_add(1, Ordering::Relaxed);
        self.connections_accepted_total
            .fetch_add(1, Ordering::Relaxed);
        CurrentConnection { stats: self }
    }

    pub(crate) fn rejected_connection(&self) {
        self.connections_rejected_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn failed_connection(&self) {
        self.connections_failed_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn prepared_crud_response(&self, error: bool) {
        self.requests_completed_total
            .fetch_add(1, Ordering::Relaxed);
        if error {
            self.error_responses_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn snapshot(&self) -> StatusSnapshot {
        StatusSnapshot {
            role: self.role,
            connections_current: self.connections_current.load(Ordering::Relaxed),
            connections_accepted_total: self.connections_accepted_total.load(Ordering::Relaxed),
            connections_rejected_total: self.connections_rejected_total.load(Ordering::Relaxed),
            connections_failed_total: self.connections_failed_total.load(Ordering::Relaxed),
            requests_completed_total: self.requests_completed_total.load(Ordering::Relaxed),
            error_responses_total: self.error_responses_total.load(Ordering::Relaxed),
        }
    }
}

pub(crate) struct CurrentConnection<'a> {
    stats: &'a RpcStats,
}

impl Drop for CurrentConnection<'_> {
    fn drop(&mut self) {
        self.stats
            .connections_current
            .fetch_sub(1, Ordering::Relaxed);
    }
}
