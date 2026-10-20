//! A one-request public-hop proxy that drops only a proven committed reply.

use super::Events;
use rockbee_raft_protocol::{
    KvRequest, KvResponse, MAX_FRAME_PAYLOAD, MutationOutcome, RequestEnvelope, ResponseEnvelope,
    decode_frame,
};
use std::{io, net::SocketAddr, sync::Arc};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    task::JoinHandle,
};

#[derive(Clone, Debug, Default)]
pub(super) struct ProxyReceipt {
    pub(super) accepted_connections: usize,
    pub(super) forwarded_put_frames: usize,
    pub(super) suppressed_response_bytes: usize,
    pub(super) extra_original_frames: usize,
    pub(super) original_eof_observed: bool,
    pub(super) committed_at: Option<u64>,
}

pub(super) struct ReplyLossProxy {
    pub(super) address: SocketAddr,
    shutdown: watch::Sender<bool>,
    task: Option<JoinHandle<Result<ProxyReceipt, String>>>,
}

impl ReplyLossProxy {
    pub(super) async fn start(gateway: SocketAddr, events: Arc<Events>) -> Result<Self, io::Error> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (shutdown, mut stopped) = watch::channel(false);
        let task = tokio::spawn(async move {
            let mut receipt = ProxyReceipt::default();
            loop {
                tokio::select! {
                    biased;
                    accepted = listener.accept() => {
                        let (client, _) = accepted.map_err(|error| error.to_string())?;
                        let (mut client_read, mut client_write) = client.into_split();
                        receipt.accepted_connections += 1;
                        if receipt.accepted_connections > 1 {
                            return Err("retired client opened an unexpected replacement connection".into());
                        }
                        let request_frame = tokio::select! {
                            changed = stopped.changed() => {
                                changed.map_err(|_| "reply-loss proxy shutdown sender disappeared".to_owned())?;
                                if *stopped.borrow() { return Ok(receipt); }
                                continue;
                            }
                            frame = read_frame(&mut client_read) => frame.map_err(|error| error.to_string())?,
                        };
                        let request: RequestEnvelope = decode_frame(&request_frame).map_err(|error| error.to_string())?;
                        request.validate().map_err(|error| error.to_string())?;
                        if !matches!(&request.request, KvRequest::Put { .. }) {
                            return Err("reply-loss proxy expected one public PUT frame".into());
                        }
                        let mut backend = tokio::select! {
                            changed = stopped.changed() => {
                                changed.map_err(|_| "reply-loss proxy shutdown sender disappeared".to_owned())?;
                                if *stopped.borrow() { return Ok(receipt); }
                                continue;
                            }
                            stream = TcpStream::connect(gateway) => stream.map_err(|error| error.to_string())?,
                        };
                        tokio::select! {
                            changed = stopped.changed() => {
                                changed.map_err(|_| "reply-loss proxy shutdown sender disappeared".to_owned())?;
                                if *stopped.borrow() { return Ok(receipt); }
                                continue;
                            }
                            written = backend.write_all(&request_frame) => written.map_err(|error| error.to_string())?,
                        };
                        tokio::select! {
                            changed = stopped.changed() => {
                                changed.map_err(|_| "reply-loss proxy shutdown sender disappeared".to_owned())?;
                                if *stopped.borrow() { return Ok(receipt); }
                                continue;
                            }
                            flushed = backend.flush() => flushed.map_err(|error| error.to_string())?,
                        };
                        receipt.forwarded_put_frames += 1;
                        let response_frame = tokio::select! {
                            changed = stopped.changed() => {
                                changed.map_err(|_| "reply-loss proxy shutdown sender disappeared".to_owned())?;
                                if *stopped.borrow() { return Ok(receipt); }
                                continue;
                            }
                            frame = read_frame(&mut backend) => frame.map_err(|error| error.to_string())?,
                        };
                        let response: ResponseEnvelope = decode_frame(&response_frame).map_err(|error| error.to_string())?;
                        response.validate_for(&request).map_err(|error| error.to_string())?;
                        if !matches!(&response.response, KvResponse::Mutation(MutationOutcome::Committed)) {
                            return Err(format!("reply-loss proxy requires actual Committed response, got {:?}", response.response));
                        }
                        receipt.committed_at = Some(events.next());
                        receipt.suppressed_response_bytes = response_frame.len();
                        // Send FIN without response bytes. Continue reading this
                        // generation to EOF so a queued replay cannot be hidden.
                        client_write.shutdown().await.map_err(|error| error.to_string())?;
                        let extra = tokio::select! {
                            biased;
                            frame = read_next_frame_or_eof(&mut client_read) => frame.map_err(|error| error.to_string())?,
                            changed = stopped.changed() => {
                                changed.map_err(|_| "reply-loss proxy shutdown sender disappeared".to_owned())?;
                                if *stopped.borrow() { return Ok(receipt); }
                                continue;
                            }
                        };
                        if extra.is_some() {
                            receipt.extra_original_frames += 1;
                            return Err(format!("original client sent {} extra request after its committed reply was suppressed", receipt.extra_original_frames));
                        }
                        receipt.original_eof_observed = true;
                        drop(backend);
                        drop(client_write);
                    }
                    changed = stopped.changed() => {
                        changed.map_err(|_| "reply-loss proxy shutdown sender disappeared".to_owned())?;
                        if *stopped.borrow() { return Ok(receipt); }
                    }
                }
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
