//! Per-group native KV ownership under the existing process-wide budgets.

use rockbee_raft_protocol::{
    MAX_FRAME_PAYLOAD, WireRequest,
    backend::{BackendRequest, BackendResponse},
    transport::server::ServerConfig,
};

// Match server admission's maximum retained frame/body and response charges.
const REQUEST_CHARGE: usize = (MAX_FRAME_PAYLOAD + 4) * 2;
const RESPONSE_CHARGE: usize =
    REQUEST_CHARGE + <BackendRequest as WireRequest<BackendResponse>>::RESPONSE_DECODE_HEADROOM;

/// Limits retained KV proposals without changing any configured server budget.
/// Include the hosted metadata group in the divisor when it is present. Keep
/// room for other work even on a node hosting just one data group.
///
/// This is a bound on retained native mutations, not a fairness guarantee under
/// an incoming flood or a simultaneous reservation for every hosted group.
/// Preserve existing legal one-slot configurations with capacity one: those
/// configurations cannot provide isolation headroom and remain best effort.
pub(super) fn mutation_capacity(server: ServerConfig, hosted_groups: usize) -> usize {
    let slots = server
        .max_outstanding
        .min(server.request_bytes / REQUEST_CHARGE)
        .min(server.response_bytes / RESPONSE_CHARGE);
    (slots / hosted_groups.max(2)).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_share_fifteen_effective_slots_without_changing_limits() {
        let server = ServerConfig::default();
        assert_eq!(mutation_capacity(server, 3), 5);
        assert_eq!(mutation_capacity(server, 2), 7);
        assert_eq!(mutation_capacity(server, 1), 7);
        assert_eq!(mutation_capacity(server, 32), 1);
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
            assert_eq!(mutation_capacity(server, 3), 2);
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
        assert_eq!(mutation_capacity(server, 3), 1);
    }
}
