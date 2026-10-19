//! Reserve count and retained bytes before frame allocation or enqueueing.
use super::{AdmitError, config, pending::Admission, state::Shared};
use crate::{RequestEnvelope, RequestId, VERSION, WireRequest, encoded_payload_size};
use std::sync::Arc;
use tokio::sync::OwnedSemaphorePermit;

pub(super) struct RequestReservation {
    pub lifetime: Arc<Admission>,
    pub request_bytes: OwnedSemaphorePermit,
    pub frame_payload_limit: usize,
}

pub(super) fn reserve_request<Req: WireRequest<Resp>, Resp>(
    shared: &Shared<Resp, Req::Expected>,
    request: &Req,
) -> Result<RequestReservation, AdmitError> {
    let count = Arc::clone(&shared.count)
        .try_acquire_owned()
        .map_err(|_| AdmitError::CountLimit)?;
    // Count-only serialization allocates no transport frame. Use the largest
    // ID/budget so later stamping cannot exceed this reservation.
    let envelope = RequestEnvelope {
        version: VERSION,
        request_id: RequestId::new(u64::MAX).expect("nonzero maximum ID"),
        budget_ms: 60_000,
        request,
    };
    let metadata = config::ownership_bytes::<Req, Resp>().ok_or(AdmitError::ByteLimit)?;
    let frame_payload_limit =
        encoded_payload_size(&envelope).map_err(AdmitError::InvalidRequest)?;
    let total = request
        .retained_bytes()
        .map_err(AdmitError::InvalidRequest)?
        .checked_add(frame_payload_limit)
        .and_then(|n| n.checked_add(metadata + 4))
        .ok_or(AdmitError::ByteLimit)?;
    let amount = u32::try_from(total).map_err(|_| AdmitError::ByteLimit)?;
    let mut request_bytes = Arc::clone(&shared.request_bytes)
        .try_acquire_many_owned(amount)
        .map_err(|_| AdmitError::ByteLimit)?;
    let metadata = request_bytes
        .split(metadata)
        .expect("metadata is part of total reservation");
    Ok(RequestReservation {
        lifetime: Arc::new(Admission {
            _count: count,
            _metadata: metadata,
        }),
        request_bytes,
        frame_payload_limit,
    })
}
