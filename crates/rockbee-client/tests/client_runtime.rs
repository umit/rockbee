use rockbee_client::{Result, RockBeeClient};
use rockbee_protocol::transport::{FrameReader, read_frame, write_frame};
use rockbee_protocol::{Request, RequestEnvelope, Response, ResponseEnvelope};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::{Handle, Runtime};
use tokio::task::JoinHandle;
use tokio::time::timeout;

const IO_BOUND: Duration = Duration::from_secs(2);
const PEER_BOUND: Duration = Duration::from_secs(5);
const NO_EXTRA_CONNECTION_BOUND: Duration = Duration::from_millis(250);
const INITIAL_KEY: &[u8] = b"initial-eof";
const PUT_KEY: &[u8] = b"put-after-foreign-runtime";
const PUT_VALUE: &[u8] = b"durable\0\xff";
const FINAL_GET_KEY: &[u8] = b"get-after-foreign-runtime";
const FINAL_VALUE: &[u8] = b"exact-after-foreign-runtime\0\xff";

#[derive(Clone, Copy)]
enum Trigger {
    Get,
    Delete,
}

impl Trigger {
    fn request(self) -> Request {
        match self {
            Self::Get => Request::Get {
                key: b"foreign-runtime-get".to_vec(),
            },
            Self::Delete => Request::Delete {
                key: b"foreign-runtime-delete".to_vec(),
            },
        }
    }

    fn response(self) -> Response {
        match self {
            Self::Get => Response::Value {
                value: Some(b"foreign-runtime-get-value".to_vec()),
            },
            Self::Delete => Response::Ok,
        }
    }

    async fn call(self, client: &RockBeeClient) -> Result<()> {
        let caller_id = Handle::current().id();
        match self {
            Self::Get => {
                let value = client.get(b"foreign-runtime-get").await?;
                if value != Some(b"foreign-runtime-get-value".to_vec()) {
                    return Err(format!("foreign runtime GET returned {value:?}").into());
                }
            }
            Self::Delete => client.delete(b"foreign-runtime-delete").await?,
        }
        if Handle::current().id() != caller_id {
            return Err("foreign runtime caller context changed across the operation".into());
        }
        Ok(())
    }
}

fn other(message: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    std::io::Error::other(message.into()).into()
}

async fn read_request(
    input: &mut FrameReader<tokio::net::tcp::OwnedReadHalf>,
    stage: &'static str,
) -> Result<RequestEnvelope> {
    let frame = timeout(IO_BOUND, read_frame(input))
        .await
        .map_err(|_| other(format!("{stage} read timed out")))??
        .ok_or_else(|| other(format!("{stage} peer closed before its request")))?;
    RequestEnvelope::decode(&frame)
}

async fn write_response(
    output: &mut tokio::net::tcp::OwnedWriteHalf,
    envelope: ResponseEnvelope,
    stage: &'static str,
) -> Result<()> {
    timeout(IO_BOUND, write_frame(output, &envelope))
        .await
        .map_err(|_| other(format!("{stage} write timed out")))??;
    Ok(())
}

async fn accept(listener: &TcpListener, stage: &'static str) -> Result<TcpStream> {
    timeout(IO_BOUND, listener.accept())
        .await
        .map_err(|_| other(format!("{stage} accept timed out")))?
        .map(|(stream, _)| stream)
        .map_err(Into::into)
}

async fn peer(listener: TcpListener, trigger: Trigger) -> Result<()> {
    let initial = accept(&listener, "initial").await?;
    let (initial_input, initial_output) = initial.into_split();
    let mut initial_input = FrameReader::new(initial_input);
    let initial_request = read_request(&mut initial_input, "initial GET").await?;
    if initial_request.request_id != 1
        || initial_request.request
            != (Request::Get {
                key: INITIAL_KEY.to_vec(),
            })
    {
        return Err(format!("unexpected initial request: {initial_request:?}").into());
    }
    drop((initial_input, initial_output));

    let recovered = accept(&listener, "foreign-runtime recovery").await?;
    let (recovered_input, mut recovered_output) = recovered.into_split();
    let mut recovered_input = FrameReader::new(recovered_input);
    let trigger_request = read_request(&mut recovered_input, "foreign-runtime trigger").await?;
    if trigger_request.request_id != 1 || trigger_request.request != trigger.request() {
        return Err(format!("unexpected foreign-runtime trigger: {trigger_request:?}").into());
    }
    write_response(
        &mut recovered_output,
        ResponseEnvelope {
            request_id: trigger_request.request_id,
            response: trigger.response(),
        },
        "foreign-runtime trigger",
    )
    .await?;

    let put = read_request(&mut recovered_input, "retained-runtime PUT").await?;
    if put.request_id != 2
        || put.request
            != (Request::Put {
                key: PUT_KEY.to_vec(),
                value: PUT_VALUE.to_vec(),
            })
    {
        return Err(format!("unexpected retained-runtime PUT: {put:?}").into());
    }
    write_response(
        &mut recovered_output,
        ResponseEnvelope {
            request_id: put.request_id,
            response: Response::Ok,
        },
        "retained-runtime PUT",
    )
    .await?;

    let get = read_request(&mut recovered_input, "retained-runtime GET").await?;
    if get.request_id != 3
        || get.request
            != (Request::Get {
                key: FINAL_GET_KEY.to_vec(),
            })
    {
        return Err(format!("unexpected retained-runtime GET: {get:?}").into());
    }
    write_response(
        &mut recovered_output,
        ResponseEnvelope {
            request_id: get.request_id,
            response: Response::Value {
                value: Some(FINAL_VALUE.to_vec()),
            },
        },
        "retained-runtime GET",
    )
    .await?;

    match timeout(IO_BOUND, read_frame(&mut recovered_input))
        .await
        .map_err(|_| other("second socket did not reach EOF after all Clients dropped"))??
    {
        None => {}
        Some(_) => return Err("second socket carried an unexpected extra request".into()),
    }
    drop(recovered_output);
    match timeout(NO_EXTRA_CONNECTION_BOUND, listener.accept()).await {
        Err(_) => Ok(()),
        Ok(Ok(_)) => Err("client opened an unexpected third connection".into()),
        Ok(Err(error)) => Err(error.into()),
    }
}

fn stop_peer(runtime: &Runtime, peer: &mut JoinHandle<Result<()>>) -> Result<()> {
    match runtime.block_on(async { timeout(PEER_BOUND, &mut *peer).await }) {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => Err(error.into()),
        Err(_) => {
            peer.abort();
            let _ = runtime.block_on(&mut *peer);
            Err("whole peer transcript timed out".into())
        }
    }
}

fn run_foreign_runtime_reconnect(trigger: Trigger) -> Result<()> {
    let runtime_a = Runtime::new()?;
    let listener = runtime_a.block_on(TcpListener::bind("127.0.0.1:0"))?;
    let address = listener.local_addr()?.to_string();
    let mut peer = runtime_a.spawn(async move {
        timeout(PEER_BOUND, peer(listener, trigger))
            .await
            .map_err(|_| other("whole peer transcript timed out"))?
    });

    let body = (|| -> Result<()> {
        let client = runtime_a.block_on(RockBeeClient::connect(&address))?;
        let initial_error = runtime_a.block_on(async {
            match timeout(IO_BOUND, client.get(INITIAL_KEY))
                .await
                .map_err(|_| other("initial EOF GET timed out"))?
            {
                Err(error) => Ok(error.to_string()),
                Ok(value) => Err(other(format!(
                    "initial EOF GET unexpectedly returned {value:?}"
                ))),
            }
        })?;
        if !initial_error.contains("upstream EOF") {
            return Err(format!("initial GET did not fail from peer EOF: {initial_error}").into());
        }

        let client_b = client.clone();
        let runtime_b = Runtime::new()?;
        runtime_b.block_on(async {
            timeout(IO_BOUND, trigger.call(&client_b))
                .await
                .map_err(|_| other("foreign runtime trigger timed out"))?
        })?;
        drop(runtime_b);
        drop(client_b);

        runtime_a.block_on(async {
            timeout(IO_BOUND, client.put(PUT_KEY, PUT_VALUE, None))
                .await
                .map_err(|_| other("retained runtime PUT timed out"))?
                .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { error.into() })
        })?;
        let final_value = runtime_a.block_on(async {
            timeout(IO_BOUND, client.get(FINAL_GET_KEY))
                .await
                .map_err(|_| other("retained runtime GET timed out"))?
        })?;
        if final_value != Some(FINAL_VALUE.to_vec()) {
            return Err(format!("retained runtime GET returned {final_value:?}").into());
        }
        drop(client);
        Ok(())
    })();

    let peer_result = stop_peer(&runtime_a, &mut peer);
    drop(runtime_a);
    match (body, peer_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(body), Ok(())) => Err(body),
        (Ok(()), Err(peer)) => Err(peer),
        (Err(body), Err(peer)) => Err(other(format!("{body}; peer failed: {peer}"))),
    }
}

#[test]
fn reconnect_triggered_by_foreign_runtime_get_keeps_the_connection_on_connect_runtime() -> Result<()>
{
    run_foreign_runtime_reconnect(Trigger::Get)
}

#[test]
fn reconnect_triggered_by_foreign_runtime_delete_keeps_the_connection_on_connect_runtime()
-> Result<()> {
    run_foreign_runtime_reconnect(Trigger::Delete)
}
