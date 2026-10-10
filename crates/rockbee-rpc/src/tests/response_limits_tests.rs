use super::*;
use rockbee_protocol::transport::{FrameReader, read_frame, write_frame};
use rockbee_protocol::{MAX_FRAME, RequestEnvelope, ResponseEnvelope};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;

const IO_BOUND: Duration = Duration::from_secs(2);
const BODY_BOUND: Duration = Duration::from_secs(8);

#[derive(Clone, Copy)]
enum Case {
    GetError,
    PutError,
    DeleteError,
    GetValue,
}

impl Case {
    const ALL: [Self; 4] = [
        Self::GetError,
        Self::PutError,
        Self::DeleteError,
        Self::GetValue,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::GetError => "get-error",
            Self::PutError => "put-error",
            Self::DeleteError => "delete-error",
            Self::GetValue => "get-value",
        }
    }

    fn request(self, key: Vec<u8>) -> Request {
        match self {
            Self::GetError | Self::GetValue => Request::Get { key },
            Self::PutError => Request::Put {
                key,
                value: b"small-put-value".to_vec(),
            },
            Self::DeleteError => Request::Delete { key },
        }
    }

    fn is_mutation(self) -> bool {
        matches!(self, Self::PutError | Self::DeleteError)
    }
}

async fn exchange(
    output: &mut tokio::net::tcp::OwnedWriteHalf,
    input: &mut FrameReader<tokio::net::tcp::OwnedReadHalf>,
    envelope: RequestEnvelope,
) -> Result<ResponseEnvelope> {
    timeout(IO_BOUND, write_frame(output, &envelope)).await??;
    let frame = timeout(IO_BOUND, read_frame(input))
        .await??
        .ok_or("peer closed before response")?;
    ResponseEnvelope::decode(&frame)
}

async fn stop_server(stop: oneshot::Sender<()>, server: &mut JoinHandle<Result<()>>) -> Result<()> {
    let _ = stop.send(());
    match timeout(IO_BOUND, &mut *server).await {
        Ok(joined) => Ok(joined??),
        Err(_) => {
            server.abort();
            let _ = server.await;
            Err("RPC server did not stop within its cleanup bound".into())
        }
    }
}

#[tokio::test]
async fn oversized_handler_responses_fall_back_and_keep_legacy_and_mux_connections_usable()
-> Result<()> {
    // A maximum backend Error at ID 1 grows beyond the limit with the public
    // connection's ten-byte ID. Check the boundary fixture independently.
    let backend_boundary = ResponseEnvelope {
        request_id: 1,
        response: Response::Error {
            mutation_outcome: rockbee_protocol::MutationOutcome::Unknown,
            message: "x".repeat(MAX_FRAME - 17),
        },
    };
    if rockbee_protocol::encode_frame(&backend_boundary)?.len() != MAX_FRAME {
        return Err("backend boundary Error fixture does not fill the frame".into());
    }
    let rpc = RpcServer::bind("127.0.0.1:0").await?;
    let address = rpc.local_addr()?;
    let calls = Arc::new(Mutex::new(HashMap::<Vec<u8>, usize>::new()));
    let (stop, stopped) = oneshot::channel();
    let mut server = tokio::spawn(rpc.run_until(
        {
            let calls = Arc::clone(&calls);
            move |request| {
                let calls = Arc::clone(&calls);
                async move {
                    let key = match request {
                        Request::Get { key }
                        | Request::Put { key, .. }
                        | Request::PutWithTtl { key, .. }
                        | Request::Delete { key } => key,
                        Request::Status => panic!("Status is handled before the handler"),
                    };
                    *calls
                        .lock()
                        .map_err(|_| std::io::Error::other("handler call ledger poisoned"))?
                        .entry(key.clone())
                        .or_default() += 1;

                    if key.ends_with(b"get-error")
                        || key.ends_with(b"put-error")
                        || key.ends_with(b"delete-error")
                    {
                        Ok(Response::Error {
                            mutation_outcome: rockbee_protocol::MutationOutcome::Unknown,
                            message: "x".repeat(if key == b"mux-oversized-get-error" {
                                MAX_FRAME - 17
                            } else {
                                MAX_FRAME
                            }),
                        })
                    } else if key.ends_with(b"get-value") {
                        Ok(Response::Value {
                            // A value this size cannot fit in a framed VALUE envelope.
                            value: Some(vec![0; MAX_FRAME]),
                        })
                    } else {
                        Ok(Response::Value { value: Some(key) })
                    }
                }
            }
        },
        async {
            stopped.await?;
            Ok(())
        },
    ));

    let body = timeout(BODY_BOUND, async {
        for (mode, first_id) in [("legacy", 0_u64), ("mux", u64::MAX - 7)] {
            let stream = TcpStream::connect(address).await?;
            let (input, mut output) = stream.into_split();
            let mut input = FrameReader::new(input);

            for (index, case) in Case::ALL.into_iter().enumerate() {
                let key = format!("{mode}-oversized-{}", case.name()).into_bytes();
                let failed_id = if first_id == 0 {
                    0
                } else {
                    first_id + 2 * index as u64
                };
                let failed = exchange(
                    &mut output,
                    &mut input,
                    RequestEnvelope {
                        request_id: failed_id,
                        request: case.request(key.clone()),
                    },
                )
                .await?;
                if failed.request_id != failed_id {
                    return Err(format!(
                        "{mode} {} fallback ID was {}, expected {failed_id}",
                        case.name(),
                        failed.request_id
                    )
                    .into());
                }
                let Response::Error { message, .. } = failed.response else {
                    return Err(
                        format!("{mode} {} did not receive fallback error", case.name()).into(),
                    );
                };
                if message.len() > 256 || !message.contains("frame") {
                    return Err(format!("{mode} {} fallback: {message}", case.name()).into());
                }
                if message.contains("mutation outcome is unknown") != case.is_mutation() {
                    return Err(format!("{mode} {} classification: {message}", case.name()).into());
                }

                let mut recovery_key = format!("{mode}-recovery-{index}").into_bytes();
                recovery_key.extend_from_slice(&[0, 255]);
                let recovery_id = if first_id == 0 { 0 } else { failed_id + 1 };
                let recovered = exchange(
                    &mut output,
                    &mut input,
                    RequestEnvelope {
                        request_id: recovery_id,
                        request: Request::Get {
                            key: recovery_key.clone(),
                        },
                    },
                )
                .await?;
                if recovered.request_id != recovery_id {
                    return Err(format!(
                        "{mode} recovery {index} ID was {}, expected {recovery_id}",
                        recovered.request_id
                    )
                    .into());
                }
                if recovered.response
                    != (Response::Value {
                        value: Some(recovery_key),
                    })
                {
                    return Err(format!("{mode} recovery {index} was not exact").into());
                }
            }
        }

        let calls = calls
            .lock()
            .map_err(|_| std::io::Error::other("handler call ledger poisoned"))?;
        if calls.len() != 16 {
            return Err(
                format!("handler saw {} distinct requests, expected 16", calls.len()).into(),
            );
        }
        for (key, count) in calls.iter() {
            if *count != 1 {
                return Err(format!("handler called {count} times for {key:?}").into());
            }
        }
        Result::<()>::Ok(())
    })
    .await;
    let cleanup = stop_server(stop, &mut server).await;
    body??;
    cleanup
}
