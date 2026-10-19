//! Opaque byte operations, response certainty and generic transport body contract.

mod request;
mod response;
mod wire_request;

pub use request::KvRequest;
pub use response::{KvResponse, MutationOutcome};
pub use wire_request::{KvExpectedResponse, WireRequest};
