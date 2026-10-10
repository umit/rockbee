//! Shared Protobuf messages. Public Client and internal Upstream own separate APIs.
use prost::Message;
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::io::{self, Read, Write};

mod mutation;
pub mod transport;
pub use mutation::{MutationError, MutationOutcome, MutationResult};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub const MAX_FRAME: usize = 1024 * 1024;
pub const MAX_KEY_LEN: usize = 65_534;
pub const PROTOCOL_VERSION: u32 = 1;
pub const INTERNAL_MAX_IN_FLIGHT: usize = 128;

/// Generated from proto/rockbee.proto. Normal builds do not run protoc.
pub mod wire {
    include!("generated/rockbee.v1.rs");
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase", deny_unknown_fields)]
pub enum Request {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    PutWithTtl {
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_millis: u64,
    },
    Get {
        key: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
    Status,
}

impl Request {
    pub fn is_mutation(&self) -> bool {
        matches!(
            self,
            Self::Put { .. } | Self::PutWithTtl { .. } | Self::Delete { .. }
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceRole {
    Unspecified,
    Writer,
    Reader,
    Gateway,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusSnapshot {
    pub role: ServiceRole,
    pub connections_current: u64,
    pub connections_accepted_total: u64,
    pub connections_rejected_total: u64,
    pub connections_failed_total: u64,
    pub requests_completed_total: u64,
    pub error_responses_total: u64,
}

// Serde is for CLI/control output. TCP always uses Protobuf.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum Response {
    Ok,
    Value {
        value: Option<Vec<u8>>,
    },
    Error {
        message: String,
        /// Meaningful only for mutations. A hop-local NotSent is encoded as
        /// Rejected: a remote caller has already sent its request to this hop.
        #[serde(default)]
        mutation_outcome: MutationOutcome,
    },
    Status {
        snapshot: StatusSnapshot,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedResponse {
    Mutation,
    Value,
    Status,
}

/// Validates a nonempty opaque key before admission, encoding, or storage access.
pub fn validate_key_bytes(key: &[u8]) -> Result<()> {
    if key.is_empty() {
        return Err("key must not be empty".into());
    }
    if key.len() > MAX_KEY_LEN {
        return Err("key exceeds 65,534 bytes".into());
    }
    Ok(())
}

/// Validate public request bytes before callers clone them into an owned wire message.
pub fn validate_request_bytes(key: &[u8], value: Option<&[u8]>) -> Result<()> {
    validate_key_bytes(key)?;
    if let Some(value) = value {
        validate_value_bytes(value)?;
    }
    let encoded = request_envelope_len(u64::MAX, key, value)?;
    if encoded > MAX_FRAME - 4 {
        return Err("request bytes exceed 1 MiB frame limit".into());
    }
    Ok(())
}

/// Validate TTL PUT bytes before callers clone them into an owned wire message.
pub fn validate_put_with_ttl_bytes(key: &[u8], value: &[u8], ttl_millis: u64) -> Result<()> {
    validate_key_bytes(key)?;
    validate_value_bytes(value)?;
    let encoded = put_with_ttl_envelope_len(u64::MAX, key, value, ttl_millis)?;
    if encoded > MAX_FRAME - 4 {
        return Err("request bytes exceed 1 MiB frame limit".into());
    }
    Ok(())
}

/// A stored value must be nonempty and fit a GET response at the largest correlation ID.
/// This borrows the value so writers can reject it before cloning or storing it.
pub fn validate_value_bytes(value: &[u8]) -> Result<()> {
    if value.is_empty() {
        return Err("value must not be empty".into());
    }
    let encoded = envelope_len(u64::MAX, 2, optional_bytes_field_len(Some(value))?)?;
    if encoded > MAX_FRAME - 4 {
        return Err("value exceeds 1 MiB GET response frame limit".into());
    }
    Ok(())
}

fn checked_add(left: usize, right: usize) -> Result<usize> {
    left.checked_add(right)
        .ok_or_else(|| "encoded frame size overflow".into())
}

fn varint_len(mut value: usize) -> usize {
    let mut len = 1;
    while value >= 128 {
        value >>= 7;
        len += 1;
    }
    len
}

fn varint_len_u64(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 128 {
        value >>= 7;
        len += 1;
    }
    len
}

fn bytes_field_len(value: &[u8]) -> Result<usize> {
    if value.is_empty() {
        return Ok(0);
    }
    checked_add(1 + varint_len(value.len()), value.len())
}

fn optional_bytes_field_len(value: Option<&[u8]>) -> Result<usize> {
    match value {
        None => Ok(0),
        Some(value) => checked_add(1 + varint_len(value.len()), value.len()),
    }
}

fn envelope_len(request_id: u64, oneof_tag_len: usize, message_len: usize) -> Result<usize> {
    let id_len = if request_id == 0 {
        0
    } else {
        1 + varint_len_u64(request_id)
    };
    let envelope = checked_add(2 + id_len + oneof_tag_len, varint_len(message_len))?;
    checked_add(envelope, message_len)
}

fn request_envelope_len(request_id: u64, key: &[u8], value: Option<&[u8]>) -> Result<usize> {
    let key_len = bytes_field_len(key)?;
    let message_len = match value {
        Some(value) => checked_add(key_len, bytes_field_len(value)?)?,
        None => key_len,
    };
    envelope_len(request_id, 1, message_len)
}

fn put_with_ttl_envelope_len(
    request_id: u64,
    key: &[u8],
    value: &[u8],
    ttl_millis: u64,
) -> Result<usize> {
    let message_len = checked_add(bytes_field_len(key)?, bytes_field_len(value)?)?;
    let message_len = if ttl_millis == 0 {
        message_len
    } else {
        checked_add(message_len, 1 + varint_len_u64(ttl_millis))?
    };
    envelope_len(request_id, 1, message_len)
}

fn response_envelope_len(request_id: u64, response: &Response) -> Result<usize> {
    let message_len = match response {
        Response::Ok => 0,
        Response::Value { value } => optional_bytes_field_len(value.as_deref())?,
        Response::Error {
            message,
            mutation_outcome,
        } => checked_add(
            bytes_field_len(message.as_bytes())?,
            if *mutation_outcome == MutationOutcome::Unknown {
                0
            } else {
                2
            },
        )?,
        Response::Status { snapshot } => status_snapshot_len(snapshot)?,
    };
    envelope_len(request_id, 2, message_len)
}

fn status_snapshot_len(snapshot: &StatusSnapshot) -> Result<usize> {
    let role = match snapshot.role {
        ServiceRole::Unspecified => 0,
        ServiceRole::Writer | ServiceRole::Reader | ServiceRole::Gateway => 2,
    };
    [
        snapshot.connections_current,
        snapshot.connections_accepted_total,
        snapshot.connections_rejected_total,
        snapshot.connections_failed_total,
        snapshot.requests_completed_total,
        snapshot.error_responses_total,
    ]
    .into_iter()
    .try_fold(role, |length, value| {
        checked_add(
            length,
            if value == 0 {
                0
            } else {
                1 + varint_len_u64(value)
            },
        )
    })
}

fn validate_exact_len(encoded: usize) -> Result<()> {
    if encoded > MAX_FRAME - 4 {
        Err("frame exceeds 1 MiB".into())
    } else {
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct RequestEnvelope<R = Request> {
    pub request_id: u64,
    pub request: R,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ResponseEnvelope {
    pub request_id: u64,
    pub response: Response,
}

fn decode(frame: &[u8]) -> Result<wire::Envelope> {
    if frame.len() > MAX_FRAME - 4 {
        return Err("frame exceeds 1 MiB".into());
    }
    let envelope = wire::Envelope::decode(frame)?;
    if envelope.version != PROTOCOL_VERSION {
        return Err("unsupported protocol version".into());
    }
    Ok(envelope)
}

impl RequestEnvelope {
    pub fn decode(frame: &[u8]) -> Result<Self> {
        use wire::envelope::Message::*;
        let envelope = decode(frame)?;
        let request = match envelope.message {
            Some(Put(v)) => Request::Put {
                key: v.key,
                value: v.value,
            },
            Some(PutWithTtl(v)) => Request::PutWithTtl {
                key: v.key,
                value: v.value,
                ttl_millis: v.ttl_millis,
            },
            Some(Get(v)) => Request::Get { key: v.key },
            Some(Delete(v)) => Request::Delete { key: v.key },
            Some(Status(_)) => Request::Status,
            _ => return Err("expected a PUT, GET, DELETE or STATUS request".into()),
        };
        std::result::Result::Ok(Self {
            request_id: envelope.request_id,
            request,
        })
    }
}

impl ResponseEnvelope {
    pub fn decode(frame: &[u8]) -> Result<Self> {
        use wire::envelope::Message::*;
        let envelope = decode(frame)?;
        let response = match envelope.message {
            Some(Ok(_)) => Response::Ok,
            Some(Value(v)) => Response::Value { value: v.value },
            Some(Error(v)) => Response::Error {
                message: v.message,
                mutation_outcome: if v.mutation_outcome == wire::MutationOutcome::Rejected as i32 {
                    MutationOutcome::Rejected
                } else {
                    MutationOutcome::Unknown
                },
            },
            Some(StatusSnapshot(v)) => Response::Status {
                snapshot: status_snapshot_from_wire(v),
            },
            _ => return Err("expected an OK, VALUE, ERROR or STATUS response".into()),
        };
        std::result::Result::Ok(Self {
            request_id: envelope.request_id,
            response,
        })
    }

    /// An ID is checked by the owning client; response kinds must match its call.
    pub fn validate(self, expected: ExpectedResponse) -> Result<Response> {
        match self.response {
            response @ Response::Error { .. } => Ok(response),
            response @ Response::Value { .. } if expected == ExpectedResponse::Value => {
                Ok(response)
            }
            Response::Ok if expected == ExpectedResponse::Mutation => Ok(Response::Ok),
            response @ Response::Status { .. } if expected == ExpectedResponse::Status => {
                Ok(response)
            }
            _ => Err("response does not match request".into()),
        }
    }
}

pub trait WireMessage {
    fn validate(&self) -> Result<()>;
    fn to_wire(&self) -> wire::Envelope;
}

impl WireMessage for Request {
    fn validate(&self) -> Result<()> {
        match self {
            Request::Put { key, value } => {
                validate_key_bytes(key)?;
                validate_value_bytes(value)?;
                validate_exact_len(request_envelope_len(0, key, Some(value))?)
            }
            Request::PutWithTtl {
                key,
                value,
                ttl_millis,
            } => {
                validate_put_with_ttl_bytes(key, value, *ttl_millis)?;
                validate_exact_len(put_with_ttl_envelope_len(0, key, value, *ttl_millis)?)
            }
            Request::Get { key } | Request::Delete { key } => {
                validate_key_bytes(key)?;
                validate_exact_len(request_envelope_len(0, key, None)?)
            }
            Request::Status => validate_exact_len(envelope_len(0, 1, 0)?),
        }
    }

    fn to_wire(&self) -> wire::Envelope {
        RequestEnvelope {
            request_id: 0,
            request: self,
        }
        .to_wire()
    }
}
impl<R: Borrow<Request>> WireMessage for RequestEnvelope<R> {
    fn validate(&self) -> Result<()> {
        match self.request.borrow() {
            Request::Put { key, value } => {
                validate_key_bytes(key)?;
                validate_value_bytes(value)?;
                validate_exact_len(request_envelope_len(self.request_id, key, Some(value))?)
            }
            Request::PutWithTtl {
                key,
                value,
                ttl_millis,
            } => {
                validate_put_with_ttl_bytes(key, value, *ttl_millis)?;
                validate_exact_len(put_with_ttl_envelope_len(
                    self.request_id,
                    key,
                    value,
                    *ttl_millis,
                )?)
            }
            Request::Get { key } | Request::Delete { key } => {
                validate_key_bytes(key)?;
                validate_exact_len(request_envelope_len(self.request_id, key, None)?)
            }
            Request::Status => validate_exact_len(envelope_len(self.request_id, 1, 0)?),
        }
    }

    fn to_wire(&self) -> wire::Envelope {
        use wire::envelope::Message::*;
        let message = match self.request.borrow() {
            Request::Put { key, value } => Put(wire::Put {
                key: key.clone(),
                value: value.clone(),
            }),
            Request::PutWithTtl {
                key,
                value,
                ttl_millis,
            } => PutWithTtl(wire::PutWithTtl {
                key: key.clone(),
                value: value.clone(),
                ttl_millis: *ttl_millis,
            }),
            Request::Get { key } => Get(wire::Get { key: key.clone() }),
            Request::Delete { key } => Delete(wire::Delete { key: key.clone() }),
            Request::Status => Status(wire::Status {}),
        };
        wire::Envelope {
            version: PROTOCOL_VERSION,
            request_id: self.request_id,
            message: Some(message),
        }
    }
}
impl WireMessage for Response {
    fn validate(&self) -> Result<()> {
        validate_exact_len(response_envelope_len(0, self)?)
    }

    fn to_wire(&self) -> wire::Envelope {
        response_wire(0, self)
    }
}
impl WireMessage for ResponseEnvelope {
    fn validate(&self) -> Result<()> {
        validate_exact_len(response_envelope_len(self.request_id, &self.response)?)
    }

    fn to_wire(&self) -> wire::Envelope {
        response_wire(self.request_id, &self.response)
    }
}
fn response_wire(request_id: u64, response: &Response) -> wire::Envelope {
    use wire::envelope::Message::*;
    let message = match response {
        Response::Ok => Ok(wire::Ok {}),
        Response::Value { value } => Value(wire::Value {
            value: value.clone(),
        }),
        Response::Error {
            message,
            mutation_outcome,
        } => Error(wire::Error {
            message: message.clone(),
            mutation_outcome: match mutation_outcome {
                MutationOutcome::NotSent | MutationOutcome::Rejected => {
                    wire::MutationOutcome::Rejected as i32
                }
                MutationOutcome::Unknown => wire::MutationOutcome::Unknown as i32,
            },
        }),
        Response::Status { snapshot } => StatusSnapshot(status_snapshot_wire(snapshot)),
    };
    wire::Envelope {
        version: PROTOCOL_VERSION,
        request_id,
        message: Some(message),
    }
}

fn service_role_wire(role: ServiceRole) -> i32 {
    match role {
        ServiceRole::Unspecified => wire::ServiceRole::Unspecified as i32,
        ServiceRole::Writer => wire::ServiceRole::Writer as i32,
        ServiceRole::Reader => wire::ServiceRole::Reader as i32,
        ServiceRole::Gateway => wire::ServiceRole::Gateway as i32,
    }
}

fn status_snapshot_wire(snapshot: &StatusSnapshot) -> wire::StatusSnapshot {
    wire::StatusSnapshot {
        role: service_role_wire(snapshot.role),
        connections_current: snapshot.connections_current,
        connections_accepted_total: snapshot.connections_accepted_total,
        connections_rejected_total: snapshot.connections_rejected_total,
        connections_failed_total: snapshot.connections_failed_total,
        requests_completed_total: snapshot.requests_completed_total,
        error_responses_total: snapshot.error_responses_total,
    }
}

fn status_snapshot_from_wire(snapshot: wire::StatusSnapshot) -> StatusSnapshot {
    let role = match wire::ServiceRole::try_from(snapshot.role).ok() {
        Some(wire::ServiceRole::Writer) => ServiceRole::Writer,
        Some(wire::ServiceRole::Reader) => ServiceRole::Reader,
        Some(wire::ServiceRole::Gateway) => ServiceRole::Gateway,
        _ => ServiceRole::Unspecified,
    };
    StatusSnapshot {
        role,
        connections_current: snapshot.connections_current,
        connections_accepted_total: snapshot.connections_accepted_total,
        connections_rejected_total: snapshot.connections_rejected_total,
        connections_failed_total: snapshot.connections_failed_total,
        requests_completed_total: snapshot.requests_completed_total,
        error_responses_total: snapshot.error_responses_total,
    }
}

/// Encode a complete bounded frame, validating size before allocating output.
pub fn encode_frame(value: &impl WireMessage) -> Result<Vec<u8>> {
    value.validate()?;
    let message = value.to_wire();
    let len = message.encoded_len();
    if len > MAX_FRAME - 4 {
        return Err("frame exceeds 1 MiB".into());
    }
    let mut data = Vec::with_capacity(len + 4);
    data.extend_from_slice(&(len as u32).to_be_bytes());
    message.encode(&mut data)?;
    Ok(data)
}

/// Read one bounded payload. Only EOF before a new header is a clean disconnect.
pub fn read_frame(reader: &mut impl Read) -> io::Result<Option<Vec<u8>>> {
    let mut header = [0; 4];
    loop {
        match reader.read(&mut header[..1]) {
            Ok(0) => return Ok(None),
            Ok(_) => break,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    reader.read_exact(&mut header[1..])?;
    let len = u32::from_be_bytes(header) as usize;
    if len > MAX_FRAME - 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame exceeds 1 MiB",
        ));
    }
    let mut payload = vec![0; len];
    reader.read_exact(&mut payload)?;
    Ok(Some(payload))
}

pub fn write_frame(writer: &mut impl Write, value: &impl WireMessage) -> Result<()> {
    writer.write_all(&encode_frame(value)?)?;
    writer.flush()?;
    Ok(())
}

#[cfg(test)]
mod exact_size_tests {
    use super::*;

    #[test]
    fn response_preflight_matches_prost_for_optional_empty_and_varint_boundaries() {
        for (request_id, response) in [
            (0, Response::Value { value: None }),
            (
                0,
                Response::Value {
                    value: Some(Vec::new()),
                },
            ),
            (
                u64::MAX,
                Response::Value {
                    value: Some(vec![0; 128]),
                },
            ),
        ] {
            assert_eq!(
                response_envelope_len(request_id, &response).unwrap(),
                response_wire(request_id, &response).encoded_len()
            );
        }
    }
}
