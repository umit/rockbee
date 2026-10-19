//! Mapping between backend delivery certainty and public mutation outcomes.

use rockbee_raft_protocol::{
    Failure, FailureReason, MutationOutcome,
    transport::{Delivery, TransportFailure},
};

/// Converts a backend connection failure without pretending that a Raft write
/// was or was not committed. Only a proven pre-handoff failure is rejected.
pub(crate) fn mutation_transport_failure(error: TransportFailure) -> MutationOutcome {
    let failure = Failure {
        reason: match error.reason {
            rockbee_raft_protocol::transport::TransportFailureKind::Deadline => {
                FailureReason::Deadline
            }
            rockbee_raft_protocol::transport::TransportFailureKind::Protocol => {
                FailureReason::Protocol
            }
            _ => FailureReason::Unavailable,
        },
        message: "backend connection did not return a final mutation result".to_owned(),
    };
    match error.delivery {
        Delivery::NotSent => MutationOutcome::Rejected(failure),
        Delivery::MayHaveBeenSent => MutationOutcome::Unknown(failure),
    }
}
