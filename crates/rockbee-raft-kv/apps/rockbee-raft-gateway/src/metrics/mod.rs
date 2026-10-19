//! Small in-process counters. Exporters belong to deployment integration.

use std::sync::atomic::{AtomicU64, Ordering};

/// Monotonic gateway counters safe to read without a lock.
#[derive(Default)]
pub struct GatewayMetrics {
    accepted: AtomicU64,
    rejected: AtomicU64,
    backend_failures: AtomicU64,
}

impl GatewayMetrics {
    pub(crate) fn accepted(&self) {
        self.accepted.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn rejected(&self) {
        self.rejected.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn backend_failure(&self) {
        self.backend_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns a consistent-enough operational snapshot; counters need no
    /// global lock because they are diagnostics, not correctness state.
    pub fn snapshot(&self) -> GatewayMetricsSnapshot {
        GatewayMetricsSnapshot {
            accepted: self.accepted.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            backend_failures: self.backend_failures.load(Ordering::Relaxed),
        }
    }
}

/// Copyable metrics view suitable for logs or a future exporter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GatewayMetricsSnapshot {
    /// Public operations handed to a backend link.
    pub accepted: u64,
    /// Requests rejected before a backend submission.
    pub rejected: u64,
    /// Backend transport or response failures.
    pub backend_failures: u64,
}
