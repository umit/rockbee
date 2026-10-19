use std::time::Duration;

use thiserror::Error;

use crate::{MAX_FRAME_PAYLOAD, WireRequest};

/// Process-wide limits shared by every connection accepted by one server.
///
/// These are admission budgets, not a promise about total process RSS. A
/// handler can own application data outside this server; it must use its own
/// domain limits as well.
#[derive(Clone, Copy, Debug)]
pub struct ServerConfig {
    /// Maximum simultaneously accepted TCP connections.
    pub max_connections: usize,
    /// Maximum accepted requests, including work after a client disconnects.
    pub max_outstanding: usize,
    /// Retained request-frame and request-body budget across all connections.
    pub request_bytes: usize,
    /// Reserved response budget across all connections.
    pub response_bytes: usize,
    /// Maximum idle, partial-header, partial-payload, or output wait.
    pub io_timeout: Duration,
    /// Maximum buffered completed responses on one connection.
    pub connection_response_queue: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_connections: 64,
            max_outstanding: 128,
            request_bytes: 64 * 1024 * 1024,
            response_bytes: 64 * 1024 * 1024,
            io_timeout: Duration::from_secs(60),
            connection_response_queue: 16,
        }
    }
}

impl ServerConfig {
    /// Checks process-global scalar limits without opening a listener.
    ///
    /// A node can call this during startup before it opens RocksDB or starts
    /// Raft groups. [`crate::transport::server::Server::start`] additionally
    /// checks message-family response decoding headroom.
    pub fn validate(self) -> Result<(), ServerConfigError> {
        if self.max_connections == 0
            || self.max_connections > tokio::sync::Semaphore::MAX_PERMITS
            || self.max_outstanding == 0
            || self.max_outstanding > tokio::sync::Semaphore::MAX_PERMITS
            || self.connection_response_queue == 0
            || self.connection_response_queue > tokio::sync::Semaphore::MAX_PERMITS
        {
            return Err(ServerConfigError::Count);
        }
        if self.io_timeout.is_zero() {
            return Err(ServerConfigError::Timeout);
        }
        // Decoding can temporarily retain both the wire frame and the typed
        // Bytes/String body, so account for two maximum payloads.
        let per_request = MAX_FRAME_PAYLOAD
            .checked_add(4)
            .and_then(|bytes| bytes.checked_mul(2))
            .ok_or(ServerConfigError::Bytes)?;
        // Encoding a response likewise overlaps the handler's typed value and
        // its output frame before the writer takes ownership.
        for bytes in [self.request_bytes, self.response_bytes] {
            if bytes < per_request
                || bytes > u32::MAX as usize
                || bytes > tokio::sync::Semaphore::MAX_PERMITS
            {
                return Err(ServerConfigError::Bytes);
            }
        }
        Ok(())
    }

    pub(crate) fn validate_for<Request, Response>(self) -> Result<(), ServerConfigError>
    where
        Request: WireRequest<Response>,
    {
        self.validate()?;
        let response_minimum = (MAX_FRAME_PAYLOAD + 4)
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(Request::RESPONSE_DECODE_HEADROOM))
            .ok_or(ServerConfigError::Bytes)?;
        if self.response_bytes < response_minimum {
            return Err(ServerConfigError::Bytes);
        }
        Ok(())
    }
}

/// Invalid server limits are rejected before the accept loop begins.
#[derive(Debug, Error)]
pub enum ServerConfigError {
    /// A connection, operation, or per-connection queue count is unsupported.
    #[error("server count limits must be nonzero and supported by semaphore counters")]
    Count,
    /// Request/response budgets cannot hold one legal frame.
    #[error("server byte limits must hold one maximum protocol frame")]
    Bytes,
    /// Socket progress must have a positive bound.
    #[error("server I/O timeout must be positive")]
    Timeout,
}
