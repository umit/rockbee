//! Test-only public relay that drops proven committed replies.

use super::Events;
use rockbee_raft_protocol::{
    KvRequest, KvResponse, MAX_FRAME_PAYLOAD, MutationOutcome, RequestEnvelope, RequestId,
    ResponseEnvelope, decode_frame,
};
use std::{io, net::SocketAddr, sync::Arc};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    task::JoinHandle,
};

#[derive(Clone, Debug)]
pub(super) struct MatchedPut {
    pub(super) request_id: RequestId,
    pub(super) key: Vec<u8>,
    pub(super) value: Vec<u8>,
    pub(super) committed_at: u64,
}

#[derive(Clone, Debug, Default)]
pub(super) struct ProxyReceipt {
    pub(super) accepted_connections: usize,
    pub(super) forwarded_put_frames: usize,
    pub(super) suppressed_response_bytes: usize,
    pub(super) extra_original_frames: usize,
    pub(super) original_eof_observed: bool,
    pub(super) committed_at: Option<u64>,
    /// The two-flight relay has exactly two bounded correlation slots.
    pub(super) matched_puts: [Option<MatchedPut>; 2],
}

#[derive(Clone, Copy)]
enum Mode {
    One,
    Two,
}

pub(super) struct ReplyLossProxy {
    pub(super) address: SocketAddr,
    shutdown: watch::Sender<bool>,
    task: Option<JoinHandle<Result<ProxyReceipt, String>>>,
}

impl ReplyLossProxy {
    pub(super) async fn start(gateway: SocketAddr, events: Arc<Events>) -> Result<Self, io::Error> {
        Self::start_with_mode(gateway, events, Mode::One).await
    }

    pub(super) async fn start_two(
        gateway: SocketAddr,
        events: Arc<Events>,
    ) -> Result<Self, io::Error> {
        Self::start_with_mode(gateway, events, Mode::Two).await
    }

    async fn start_with_mode(
        gateway: SocketAddr,
        events: Arc<Events>,
        mode: Mode,
    ) -> Result<Self, io::Error> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (shutdown, stopped) = watch::channel(false);
        let task = tokio::spawn(async move {
            match mode {
                Mode::One => run_one(listener, gateway, events, stopped).await,
                Mode::Two => run_two(listener, gateway, events, stopped).await,
            }
        });
        Ok(Self {
            address,
            shutdown,
            task: Some(task),
        })
    }

    pub(super) async fn close(&mut self) -> Result<ProxyReceipt, String> {
        let _ = self.shutdown.send(true);
        let result = self
            .task
            .as_mut()
            .expect("owned reply-loss proxy task")
            .await
            .map_err(|error| format!("reply-loss proxy task failed: {error}"))?;
        self.task.take();
        result
    }
}

async fn run_one(
    listener: TcpListener,
    gateway: SocketAddr,
    events: Arc<Events>,
    mut stopped: watch::Receiver<bool>,
) -> Result<ProxyReceipt, String> {
    let mut receipt = ProxyReceipt::default();
    loop {
        let Some((client, _)) = interruptible(&mut stopped, listener.accept()).await? else {
            return Ok(receipt);
        };
        receipt.accepted_connections += 1;
        if receipt.accepted_connections > 1 {
            return Err("retired client opened an unexpected replacement connection".into());
        }
        let (mut client_read, mut client_write) = client.into_split();
        let Some(request_frame) = interruptible(&mut stopped, read_frame(&mut client_read)).await?
        else {
            return Ok(receipt);
        };
        let request = decode_put(&request_frame, "one")?;
        let Some(mut backend) = interruptible(&mut stopped, TcpStream::connect(gateway)).await?
        else {
            return Ok(receipt);
        };
        let Some(()) = interruptible(&mut stopped, backend.write_all(&request_frame)).await? else {
            return Ok(receipt);
        };
        let Some(()) = interruptible(&mut stopped, backend.flush()).await? else {
            return Ok(receipt);
        };
        receipt.forwarded_put_frames += 1;
        let Some(response_frame) = interruptible(&mut stopped, read_frame(&mut backend)).await?
        else {
            return Ok(receipt);
        };
        let response: ResponseEnvelope =
            decode_frame(&response_frame).map_err(|error| error.to_string())?;
        response
            .validate_for(&request)
            .map_err(|error| error.to_string())?;
        if !matches!(
            &response.response,
            KvResponse::Mutation(MutationOutcome::Committed)
        ) {
            return Err(format!(
                "reply-loss proxy requires actual Committed response, got {:?}",
                response.response
            ));
        }
        receipt.committed_at = Some(events.next());
        receipt.suppressed_response_bytes = response_frame.len();
        client_write
            .shutdown()
            .await
            .map_err(|error| error.to_string())?;
        let Some(extra) =
            interruptible(&mut stopped, read_next_frame_or_eof(&mut client_read)).await?
        else {
            return Ok(receipt);
        };
        if extra.is_some() {
            receipt.extra_original_frames += 1;
            return Err(format!(
                "original client sent {} extra request after its committed reply was suppressed",
                receipt.extra_original_frames
            ));
        }
        receipt.original_eof_observed = true;
    }
}

async fn run_two(
    listener: TcpListener,
    gateway: SocketAddr,
    events: Arc<Events>,
    mut stopped: watch::Receiver<bool>,
) -> Result<ProxyReceipt, String> {
    let mut receipt = ProxyReceipt::default();
    loop {
        let Some((client, _)) = interruptible(&mut stopped, listener.accept()).await? else {
            return Ok(receipt);
        };
        receipt.accepted_connections += 1;
        if receipt.accepted_connections > 1 {
            return Err("retired client opened an unexpected replacement connection".into());
        }
        let (mut client_read, mut client_write) = client.into_split();
        let Some(first_frame) = interruptible(&mut stopped, read_frame(&mut client_read)).await?
        else {
            return Ok(receipt);
        };
        let first = decode_put(&first_frame, "first")?;
        let Some(second_frame) = interruptible(&mut stopped, read_frame(&mut client_read)).await?
        else {
            return Ok(receipt);
        };
        let second = decode_put(&second_frame, "second")?;
        validate_distinct_puts(&first, &second)?;
        let Some(mut backend) = interruptible(&mut stopped, TcpStream::connect(gateway)).await?
        else {
            return Ok(receipt);
        };
        for frame in [&first_frame, &second_frame] {
            let Some(()) = interruptible(&mut stopped, backend.write_all(frame)).await? else {
                return Ok(receipt);
            };
        }
        let Some(()) = interruptible(&mut stopped, backend.flush()).await? else {
            return Ok(receipt);
        };
        receipt.forwarded_put_frames = 2;
        for _ in 0..2 {
            let Some(response_frame) =
                interruptible(&mut stopped, read_frame(&mut backend)).await?
            else {
                return Ok(receipt);
            };
            let response: ResponseEnvelope =
                decode_frame(&response_frame).map_err(|error| error.to_string())?;
            let index = if response.request_id == first.request_id {
                0
            } else if response.request_id == second.request_id {
                1
            } else {
                return Err(format!(
                    "two-flight reply-loss proxy received unknown response request id {}",
                    response.request_id.get()
                ));
            };
            if receipt.matched_puts[index].is_some() {
                return Err(format!(
                    "two-flight reply-loss proxy received duplicate response request id {}",
                    response.request_id.get()
                ));
            }
            let request = if index == 0 { &first } else { &second };
            response
                .validate_for(request)
                .map_err(|error| error.to_string())?;
            if !matches!(
                &response.response,
                KvResponse::Mutation(MutationOutcome::Committed)
            ) {
                return Err(format!(
                    "two-flight reply-loss proxy requires actual Committed response, got {:?}",
                    response.response
                ));
            }
            let (key, value) = put_parts(request)?;
            let committed_at = events.next();
            receipt.committed_at.get_or_insert(committed_at);
            receipt.matched_puts[index] = Some(MatchedPut {
                request_id: request.request_id,
                key: key.to_vec(),
                value: value.to_vec(),
                committed_at,
            });
            receipt.suppressed_response_bytes += response_frame.len();
        }
        client_write
            .shutdown()
            .await
            .map_err(|error| error.to_string())?;
        let Some(extra) =
            interruptible(&mut stopped, read_next_frame_or_eof(&mut client_read)).await?
        else {
            return Ok(receipt);
        };
        if extra.is_some() {
            receipt.extra_original_frames += 1;
            return Err("original client sent extra full or partial request bytes after two committed replies were suppressed".into());
        }
        receipt.original_eof_observed = true;
    }
}

async fn interruptible<T, F>(
    stopped: &mut watch::Receiver<bool>,
    operation: F,
) -> Result<Option<T>, String>
where
    F: Future<Output = io::Result<T>>,
{
    tokio::select! {
        biased;
        result = operation => result.map(Some).map_err(|error| error.to_string()),
        changed = stopped.changed() => {
            changed.map_err(|_| "reply-loss proxy shutdown sender disappeared".to_owned())?;
            if *stopped.borrow() { Ok(None) } else { Err("reply-loss proxy received an invalid shutdown transition".into()) }
        }
    }
}

fn decode_put(frame: &[u8], position: &str) -> Result<RequestEnvelope, String> {
    let request: RequestEnvelope = decode_frame(frame).map_err(|error| error.to_string())?;
    request.validate().map_err(|error| error.to_string())?;
    if !matches!(&request.request, KvRequest::Put { .. }) {
        return Err(format!(
            "reply-loss proxy expected {position} public PUT frame"
        ));
    }
    Ok(request)
}

fn validate_distinct_puts(first: &RequestEnvelope, second: &RequestEnvelope) -> Result<(), String> {
    if first.request_id == second.request_id {
        return Err("two-flight reply-loss proxy requires distinct nonzero request ids".into());
    }
    let (first_key, first_value) = put_parts(first)?;
    let (second_key, second_value) = put_parts(second)?;
    if first_key == second_key || first_value == second_value {
        return Err("two-flight reply-loss proxy requires distinct key and value targets".into());
    }
    Ok(())
}

fn put_parts(request: &RequestEnvelope) -> Result<(&[u8], &[u8]), String> {
    match &request.request {
        KvRequest::Put { key, value } => Ok((key, value)),
        _ => Err("reply-loss proxy expected public PUT frame".into()),
    }
}

async fn read_frame(reader: &mut (impl AsyncRead + Unpin)) -> io::Result<Vec<u8>> {
    let mut header = [0_u8; 4];
    reader.read_exact(&mut header).await?;
    let length = usize::try_from(u32::from_be_bytes(header))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "frame length conversion"))?;
    if length == 0 || length > MAX_FRAME_PAYLOAD {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid frame length",
        ));
    }
    let mut frame = vec![0; length + header.len()];
    frame[..header.len()].copy_from_slice(&header);
    reader.read_exact(&mut frame[header.len()..]).await?;
    Ok(frame)
}

async fn read_next_frame_or_eof(
    reader: &mut (impl AsyncRead + Unpin),
) -> io::Result<Option<Vec<u8>>> {
    let mut header = [0_u8; 4];
    let first = reader.read(&mut header).await?;
    if first == 0 {
        return Ok(None);
    }
    reader.read_exact(&mut header[first..]).await?;
    let length = usize::try_from(u32::from_be_bytes(header))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "frame length conversion"))?;
    if length == 0 || length > MAX_FRAME_PAYLOAD {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid extra frame length",
        ));
    }
    let mut frame = vec![0; length + header.len()];
    frame[..header.len()].copy_from_slice(&header);
    reader.read_exact(&mut frame[header.len()..]).await?;
    Ok(Some(frame))
}
