//! Strict length-prefixed postcard for trusted protocol message types.
use serde::{Serialize, de::DeserializeOwned};

use crate::{MAX_FRAME_PAYLOAD, ProtocolError};

const FRAME_HEADER_BYTES: usize = 4;

/// Encodes a four-byte big-endian payload length followed by postcard bytes.
///
/// A bounded counting pass rejects overflow before allocating an exact-size
/// output buffer; the second pass writes into that fixed slice. Serializers
/// must be trusted code. Changes in size between passes cannot grow the buffer.
///
/// # Errors
/// Returns an error for an empty/oversized payload or serialization failure.
pub fn encode_frame<T: Serialize>(value: &T) -> Result<Vec<u8>, ProtocolError> {
    encode_frame_with_payload_limit(value, MAX_FRAME_PAYLOAD)
}

pub(crate) fn encode_frame_with_payload_limit<T: Serialize>(
    value: &T,
    limit: usize,
) -> Result<Vec<u8>, ProtocolError> {
    let size = encoded_payload_size(value)?;
    if size > limit {
        return Err(ProtocolError::InvalidFrameLength(size));
    }
    if size == 0 {
        return Err(ProtocolError::InvalidFrameLength(0));
    }
    let mut frame = vec![0; size + FRAME_HEADER_BYTES];
    let payload_len = postcard::to_slice(value, &mut frame[FRAME_HEADER_BYTES..])?.len();
    if payload_len == 0 {
        return Err(ProtocolError::InvalidFrameLength(0));
    }
    let length =
        u32::try_from(payload_len).map_err(|_| ProtocolError::InvalidFrameLength(payload_len))?;
    frame[..FRAME_HEADER_BYTES].copy_from_slice(&length.to_be_bytes());
    frame.truncate(payload_len + FRAME_HEADER_BYTES);
    Ok(frame)
}

/// Counts serialized bytes without allocating a frame.
///
/// # Errors
/// Returns a codec error when serialization exceeds the frame limit.
pub fn encoded_payload_size<T: Serialize>(value: &T) -> Result<usize, ProtocolError> {
    Ok(postcard::serialize_with_flavor(value, BoundedSize(0))?)
}

/// Decodes exactly one complete frame, rejecting trailing postcard bytes.
///
/// Supported untrusted wire data is limited to this crate's protocol types:
/// their variable-size fields are `Bytes`/`String`, whose declared lengths
/// postcard checks against available input before allocation. `T` and custom
/// serde implementations must be trusted application code. Arbitrary collection
/// schemas (including maps and zero-sized element sequences) require their own
/// count/depth limits; this generic helper does not sandbox serde visitors.
/// Envelope semantics remain separate: call `validate()` or `validate_for()`.
///
/// # Errors
/// Returns an error for a truncated/oversized/empty frame, trailing input,
/// invalid postcard, or an invalid typed identifier/configuration.
pub fn decode_frame<T: DeserializeOwned>(frame: &[u8]) -> Result<T, ProtocolError> {
    let header: [u8; FRAME_HEADER_BYTES] = frame
        .get(..FRAME_HEADER_BYTES)
        .ok_or(ProtocolError::FrameLengthMismatch)?
        .try_into()
        .map_err(|_| ProtocolError::FrameLengthMismatch)?;
    let length = usize::try_from(u32::from_be_bytes(header))
        .map_err(|_| ProtocolError::FrameLengthMismatch)?;
    if length == 0 || length > MAX_FRAME_PAYLOAD {
        return Err(ProtocolError::InvalidFrameLength(length));
    }
    if frame.len() - FRAME_HEADER_BYTES != length {
        return Err(ProtocolError::FrameLengthMismatch);
    }
    let (value, remaining) = postcard::take_from_bytes(&frame[FRAME_HEADER_BYTES..])?;
    if !remaining.is_empty() {
        return Err(ProtocolError::TrailingPayload);
    }
    Ok(value)
}

// postcard's unbounded Size flavor adds unchecked sizes and never stops an
// oversized stream. Keep the count within the frame bound on every write.
struct BoundedSize(usize);
impl postcard::ser_flavors::Flavor for BoundedSize {
    type Output = usize;
    fn try_push(&mut self, byte: u8) -> Result<(), postcard::Error> {
        self.try_extend(&[byte])
    }
    fn try_extend(&mut self, bytes: &[u8]) -> Result<(), postcard::Error> {
        if bytes.len() > MAX_FRAME_PAYLOAD - self.0 {
            return Err(postcard::Error::SerializeBufferFull);
        }
        self.0 += bytes.len();
        Ok(())
    }
    fn finalize(self) -> Result<Self::Output, postcard::Error> {
        Ok(self.0)
    }
}
