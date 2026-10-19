//! Engine-independent byte-key/value protocol for Rockbee Multi-Raft.
//!
//! Request IDs correlate responses; they do not deduplicate retried mutations.
//! Call [`RequestEnvelope::validate`] after decoding a request and
//! [`ResponseEnvelope::validate_for`] after decoding its response. Framing checks
//! bytes and resource limits separately from operation-specific validation.
//!
//! Modules group related values and functions. Private ID wrapper fields prevent
//! substituting node IDs for request IDs without inheritance or runtime casts.
//! Constructors return `Result` when caller input can be invalid.
//!
//! For Java readers: modules group related public types; structs carry fields,
//! enums represent a fixed set of cases, and traits describe shared behavior.
//! `Result<T, E>` carries either success (`Ok`) or a typed failure (`Err`); `?`
//! returns the failure to the caller. `Option<Bytes>` distinguishes a missing key
//! (`None`) from an existing empty value (`Some(Bytes::new())`). These are
//! explicit states, not exceptions or nullable byte arrays.
//!
//! [`WireRequest`] is the transport's body interface: each message family
//! validates its input, reports retained ownership and supplies a small response
//! compatibility token. Default generic envelopes keep public KV callers simple
//! while allowing internal message families to share the same TCP machinery.
//!
//! ```
//! use bytes::Bytes;
//! use rockbee_raft_protocol::{
//!     KvRequest, RequestEnvelope, RequestId, decode_frame, encode_frame,
//! };
//!
//! let request = RequestEnvelope::new(
//!     RequestId::new(7)?,
//!     1_000,
//!     KvRequest::Get { key: Bytes::from_static(b"key") },
//! )?;
//! let frame = encode_frame(&request)?;
//! let received: RequestEnvelope = decode_frame(&frame)?;
//! received.validate()?; // Validate semantics before dispatching the payload.
//! assert_eq!(received, request);
//! # Ok::<(), rockbee_raft_protocol::ProtocolError>(())
//! ```

#![warn(missing_docs)]

pub mod backend;
pub mod codec;
pub mod control;
mod error;
pub mod identity;
pub mod kv;
mod limits;
pub mod message;
pub mod routing;
pub mod transport;
mod validation;

pub use codec::{decode_frame, encode_frame, encoded_payload_size};
pub use error::ProtocolError;
pub use identity::{ClusterId, GroupId, NodeId, RequestId};
pub use kv::{KvExpectedResponse, KvRequest, KvResponse, MutationOutcome, WireRequest};
pub use limits::{MAX_FRAME_PAYLOAD, MAX_KEY_BYTES, MAX_MESSAGE_BYTES, MAX_VALUE_BYTES, VERSION};
pub use message::{Failure, FailureReason, ReadFailure, RequestEnvelope, ResponseEnvelope};
pub use routing::{HASH_VERSION, ShardConfig};
