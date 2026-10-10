use rockbee_protocol::INTERNAL_MAX_IN_FLIGHT;
use rockbee_protocol::Result;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

/// Incoming TCP limits, independently applied by each server or gateway.
///
/// Read time covers one whole frame, including idle time and partial bytes.
/// Write time covers the complete response and flush. Neither is a handler
/// execution deadline or a strict process-wide byte/memory budget. Request
/// admission is shared across connections and held until the response leaves
/// the queue for the connection's single bounded egress writer.
#[derive(Clone, Copy, Debug)]
pub struct RpcConfig {
    max_connections: usize,
    global_max_in_flight: usize,
    read_timeout: Duration,
    write_timeout: Duration,
}

impl RpcConfig {
    pub fn new(
        max_connections: usize,
        read_timeout: Duration,
        write_timeout: Duration,
    ) -> Result<Self> {
        if max_connections == 0 || max_connections > Semaphore::MAX_PERMITS {
            return Err("RPC connection limit must be positive and fit semaphore capacity".into());
        }
        let now = Instant::now();
        if [read_timeout, write_timeout]
            .iter()
            .any(|duration| duration.is_zero() || now.checked_add(*duration).is_none())
        {
            return Err("RPC timeouts must be positive and representable".into());
        }
        Ok(Self {
            max_connections,
            global_max_in_flight: INTERNAL_MAX_IN_FLIGHT,
            read_timeout,
            write_timeout,
        })
    }

    pub fn max_connections(self) -> usize {
        self.max_connections
    }
    /// Queued/executing requests across connections, including queued responses.
    /// Each connection can additionally own one response being written.
    pub fn global_max_in_flight(self) -> usize {
        self.global_max_in_flight
    }

    pub fn with_global_max_in_flight(mut self, limit: usize) -> Result<Self> {
        if limit == 0 || limit > Semaphore::MAX_PERMITS {
            return Err(
                "RPC process-wide request limit must be positive and fit semaphore capacity".into(),
            );
        }
        self.global_max_in_flight = limit;
        Ok(self)
    }
    pub fn read_timeout(self) -> Duration {
        self.read_timeout
    }
    pub fn write_timeout(self) -> Duration {
        self.write_timeout
    }
}

impl Default for RpcConfig {
    fn default() -> Self {
        Self {
            max_connections: 256,
            global_max_in_flight: INTERNAL_MAX_IN_FLIGHT,
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(5),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_public_limits_are_rejected_before_tokio_can_panic() {
        let second = Duration::from_secs(1);
        for count in [0, Semaphore::MAX_PERMITS + 1] {
            assert!(RpcConfig::new(count, second, second).is_err());
        }
        for budget in [Duration::ZERO, Duration::MAX] {
            assert!(RpcConfig::new(1, budget, second).is_err());
            assert!(RpcConfig::new(1, second, budget).is_err());
        }
    }
}
