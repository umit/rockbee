use std::sync::Arc;

use serde::de::DeserializeOwned;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::{OwnedSemaphorePermit, mpsc},
    time::{Instant, sleep_until},
};

use super::{
    Queued, TransportFailureKind,
    config::OWNERSHIP_HEADROOM,
    pending::Completion,
    state::{Phase, Shared},
};
use crate::{
    MAX_FRAME_PAYLOAD, RequestEnvelope, ResponseEnvelope, VERSION, WireRequest,
    codec::encode_frame_with_payload_limit, decode_frame,
};

pub(super) async fn write_requests<Req, Resp, W>(
    mut writer: W,
    mut queue: mpsc::Receiver<Queued<Req>>,
    shared: Arc<Shared<Resp, Req::Expected>>,
) where
    Req: WireRequest<Resp>,
    Resp: Send + 'static,
    W: AsyncWrite + Unpin,
{
    let mut stop = shared.stop.subscribe();
    loop {
        if *stop.borrow() {
            break;
        }
        // `select!` polls competing futures. `biased` gives retirement priority
        // when both are ready, so a retired generation starts no queued work.
        // Cancelling queue.recv is safe: it does not consume a queued item.
        let item = tokio::select! {
            biased;
            _ = stop.changed() => break,
            item = queue.recv() => match item { Some(item) => item, None => break },
        };
        // Expired queue storage stays charged until physically dropped here.
        if !shared.lock().entries.contains_key(&item.id) {
            log::trace!(
                "raft transport discarded retired queued request request_id={}",
                item.id.get()
            );
            continue;
        }
        let Some(remaining) = item.deadline.checked_duration_since(Instant::now()) else {
            log::debug!(
                "raft transport queued request deadline elapsed request_id={}",
                item.id.get()
            );
            shared.expire(item.id, Instant::now());
            continue;
        };
        let budget_ms = u64::try_from(remaining.as_millis())
            .unwrap_or(u64::MAX)
            .clamp(1, 60_000);
        let frame = match encode_request(&item, budget_ms) {
            Ok(frame) => frame,
            Err(_) => {
                log::debug!(
                    "raft transport request encoding failed request_id={}",
                    item.id.get()
                );
                shared.retire(TransportFailureKind::Protocol);
                break;
            }
        };
        // This locked transition is expiry's sole competitor and immediately
        // precedes the first write poll. Encoding spent the same deadline.
        if !shared.handoff(item.id) {
            log::trace!(
                "raft transport request was not handed off request_id={}",
                item.id.get()
            );
            continue;
        }
        log::trace!(
            "raft transport request handed off request_id={} frame_bytes={}",
            item.id.get(),
            frame.len()
        );
        // Cancelling write_all can leave a partial frame. That branch is only
        // retirement: we drop this socket and never reuse its frame boundary.
        // `biased` prevents a ready socket write from beating known retirement.
        let written = tokio::select! {
            biased;
            _ = stop.changed() => break,
            result = writer.write_all(&frame) => result,
        };
        if written.is_err() {
            log::debug!(
                "raft transport request write failed request_id={}",
                item.id.get()
            );
            shared.retire(TransportFailureKind::Io);
            break;
        }
        log::trace!(
            "raft transport request write completed request_id={} frame_bytes={}",
            item.id.get(),
            frame.len()
        );
        // Frame/body byte reservations drop together at iteration end.
    }
    // Drops socket half and entire queue. Never append another frame after an
    // interrupted partial write. The pending map owns only expected tokens.
}

fn encode_request<Req: serde::Serialize>(
    item: &Queued<Req>,
    budget_ms: u64,
) -> Result<Vec<u8>, crate::ProtocolError> {
    let envelope = RequestEnvelope {
        version: VERSION,
        request_id: item.id,
        budget_ms,
        request: &item.request,
    };
    // Enforce admission's measured capacity even for a changing serializer.
    encode_frame_with_payload_limit(&envelope, item.frame_payload_limit)
}

pub(super) async fn read_responses<Req, Resp, R>(
    mut reader: R,
    shared: Arc<Shared<Resp, Req::Expected>>,
) where
    Req: WireRequest<Resp>,
    Resp: DeserializeOwned + Send + 'static,
    R: AsyncRead + Unpin,
{
    let mut stop = shared.stop.subscribe();
    loop {
        if *stop.borrow() {
            break;
        }
        // Cancelling receive_frame may discard a partial header/body. It is
        // safe here because retirement discards the whole connection; all local
        // payload/receive permits are then dropped. No replacement stream shares
        // this reader or pending map. Prefer cancellation when both are ready.
        let received = tokio::select! {
            biased;
            _ = stop.changed() => break,
            frame = receive_frame::<Resp, _, _>(&mut reader, &shared, Req::RESPONSE_DECODE_HEADROOM) => frame,
        };
        let (frame, permit) = match received {
            Ok(frame) => frame,
            Err(reason) => {
                log::debug!("raft transport response read failed kind={reason:?}");
                shared.retire(reason);
                break;
            }
        };
        let frame_bytes = frame.len();
        let response: ResponseEnvelope<Resp> = match decode_frame(&frame) {
            Ok(response) => response,
            Err(_) => {
                log::debug!("raft transport response decode failed frame_bytes={frame_bytes}");
                shared.retire(TransportFailureKind::Protocol);
                break;
            }
        };
        let request_id = response.request_id.get();
        drop(frame);
        if !deliver_response::<Req, Resp>(&shared, response, permit) {
            log::debug!(
                "raft transport response did not match pending request request_id={request_id} frame_bytes={frame_bytes}"
            );
            break;
        }
        log::trace!(
            "raft transport response matched request_id={request_id} frame_bytes={frame_bytes}"
        );
    }
}

async fn receive_frame<Resp, Expected, R>(
    reader: &mut R,
    shared: &Shared<Resp, Expected>,
    response_decode_headroom: usize,
) -> Result<(Vec<u8>, OwnedSemaphorePermit), TransportFailureKind>
where
    R: AsyncRead + Unpin,
{
    let mut header = [0; 4];
    reader.read_exact(&mut header).await.map_err(io_failure)?;
    let length = u32::from_be_bytes(header) as usize;
    if length == 0 || length > MAX_FRAME_PAYLOAD {
        return Err(TransportFailureKind::Protocol);
    }
    // One frame and one decoded payload coexist. Concrete response decoders
    // must obey WireRequest's bounded schema contract before using this Link.
    let reserved = length
        .checked_mul(2)
        .and_then(|n| n.checked_add(OWNERSHIP_HEADROOM + 4 + size_of::<Resp>()))
        .and_then(|n| n.checked_add(response_decode_headroom))
        .and_then(|n| u32::try_from(n).ok())
        .ok_or(TransportFailureKind::Protocol)?;
    let permit = Arc::clone(&shared.receive_bytes)
        .acquire_many_owned(reserved)
        .await
        .map_err(|_| TransportFailureKind::Closed)?;
    let mut frame = vec![0; length + 4];
    frame[..4].copy_from_slice(&header);
    reader
        .read_exact(&mut frame[4..])
        .await
        .map_err(io_failure)?;
    Ok((frame, permit))
}

fn io_failure(error: std::io::Error) -> TransportFailureKind {
    if error.kind() == std::io::ErrorKind::UnexpectedEof {
        TransportFailureKind::Disconnected
    } else {
        TransportFailureKind::Io
    }
}

fn deliver_response<Req: WireRequest<Resp>, Resp>(
    shared: &Shared<Resp, Req::Expected>,
    response: ResponseEnvelope<Resp>,
    permit: OwnedSemaphorePermit,
) -> bool {
    let expected = {
        let mut state = shared.lock();
        let Some(entry) = state.entries.get(&response.request_id) else {
            shared.retire_locked(&mut state, TransportFailureKind::Protocol);
            return false;
        };
        if response.version != VERSION || entry.phase != Phase::Handed {
            shared.retire_locked(&mut state, TransportFailureKind::Protocol);
            return false;
        }
        entry.expected
    };
    // The message-family implementation runs outside the map lock. Recheck
    // retirement and the absolute deadline afterwards before delivery can win.
    let valid = Req::validate_response(expected, &response.response).is_ok();
    let mut state = shared.lock();
    if state.retired.is_some() {
        return false;
    }
    let Some(entry) = state.entries.get(&response.request_id) else {
        return false;
    };
    if !valid {
        shared.retire_locked(&mut state, TransportFailureKind::Protocol);
        return false;
    }
    if entry.deadline <= Instant::now() {
        shared.retire_locked(&mut state, TransportFailureKind::Deadline);
        return false;
    }
    let entry = state
        .entries
        .remove(&response.request_id)
        .expect("matched entry");
    state
        .deadlines
        .remove(&(entry.deadline, response.request_id));
    let result = Ok(response.response);
    if let Some(stats) = entry.stats {
        stats.finish(&result);
    }
    let _ = entry.sender.send(Completion {
        result,
        _count: entry.count,
        _receive: Some(permit),
    });
    drop(state);
    shared.deadline_changed.notify_one();
    true
}

pub(super) async fn coordinate_deadlines<Resp, Expected>(shared: Arc<Shared<Resp, Expected>>)
where
    Resp: Send + 'static,
    Expected: Send + 'static,
{
    let mut stop = shared.stop.subscribe();
    loop {
        if *stop.borrow() {
            break;
        }
        let next = shared.lock().deadlines.first().copied();
        // Wake on the earliest deadline or a changed deadline set. Prefer stop
        // and rescheduling notifications; expire() still rechecks the live entry
        // under the mutex, so an old timer cannot retire completed work.
        match next {
            Some((deadline, id)) => tokio::select! {
                biased;
                _ = stop.changed() => break,
                _ = shared.deadline_changed.notified() => {},
                _ = sleep_until(deadline) => shared.expire(id, Instant::now()),
            },
            None => {
                tokio::select! { biased; _ = stop.changed() => break, _ = shared.deadline_changed.notified() => {} }
            }
        }
    }
}
