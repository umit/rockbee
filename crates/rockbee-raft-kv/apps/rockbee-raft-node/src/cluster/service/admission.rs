//! Native KV ownership within the existing process-wide budgets.

use rockbee_raft_protocol::{
    MAX_FRAME_PAYLOAD, WireRequest,
    backend::{BackendRequest, BackendResponse},
    transport::server::ServerConfig,
};

// Match server admission's maximum retained frame/body and response charges.
const REQUEST_CHARGE: usize = (MAX_FRAME_PAYLOAD + 4) * 2;
const RESPONSE_CHARGE: usize =
    REQUEST_CHARGE + <BackendRequest as WireRequest<BackendResponse>>::RESPONSE_DECODE_HEADROOM;

/// Lets one group use the existing server capacity without dividing it by the
/// number of hosted groups. The server still enforces its budgets globally.
///
/// This semaphore also seals a group after a fatal Raft error. It does not
/// reserve capacity for sibling groups: a stalled group can occupy the shared
/// server budget until its accepted proposals finish.
pub(in crate::cluster) fn mutation_capacity(server: ServerConfig) -> usize {
    server
        .max_outstanding
        .min(server.request_bytes / REQUEST_CHARGE)
        .min(server.response_bytes / RESPONSE_CHARGE)
        .max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_group_can_use_the_process_effective_capacity() {
        let server = ServerConfig::default();
        assert_eq!(mutation_capacity(server), 15);
    }

    #[test]
    fn operation_and_each_byte_budget_can_bound_native_ownership() {
        for server in [
            ServerConfig {
                max_outstanding: 6,
                ..ServerConfig::default()
            },
            ServerConfig {
                request_bytes: REQUEST_CHARGE * 6,
                ..ServerConfig::default()
            },
            ServerConfig {
                response_bytes: RESPONSE_CHARGE * 6,
                ..ServerConfig::default()
            },
        ] {
            assert_eq!(mutation_capacity(server), 6);
        }
    }

    #[test]
    fn single_slot_remains_usable_without_claiming_isolation() {
        let server = ServerConfig {
            max_outstanding: 1,
            request_bytes: REQUEST_CHARGE,
            response_bytes: RESPONSE_CHARGE,
            ..ServerConfig::default()
        };
        assert!(server.validate().is_ok());
        assert_eq!(mutation_capacity(server), 1);
    }
}
