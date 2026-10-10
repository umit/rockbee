use rockbee_node::protocol::{
    MAX_FRAME, MutationOutcome, PROTOCOL_VERSION, Request, RequestEnvelope, Response,
    ResponseEnvelope, WireMessage,
};
use rockbee_node::storage::Role;
use rockbee_node::{Result, Server, Store};
use slatedb::object_store::{ObjectStore, local::LocalFileSystem};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpStream, tcp};
use tokio::task::JoinHandle;
use tokio::time::timeout;

struct RawPut {
    request_id: u64,
    key: Vec<u8>,
    value: Vec<u8>,
}

impl WireMessage for RawPut {
    fn validate(&self) -> Result<()> {
        Ok(())
    }

    fn to_wire(&self) -> rockbee_node::protocol::wire::Envelope {
        rockbee_node::protocol::wire::Envelope {
            version: PROTOCOL_VERSION,
            request_id: self.request_id,
            message: Some(rockbee_node::protocol::wire::envelope::Message::Put(
                rockbee_node::protocol::wire::Put {
                    key: self.key.clone(),
                    value: self.value.clone(),
                },
            )),
        }
    }
}

async fn start_writer(
    store: Store,
) -> Result<(
    String,
    tokio::sync::oneshot::Sender<()>,
    JoinHandle<Result<()>>,
)> {
    let server = Server::bind("127.0.0.1:0", store).await?;
    let address = server.local_addr()?.to_string();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(server.run_until(async move {
        stopped.await?;
        Ok(())
    }));
    Ok((address, stop, task))
}

async fn stop_writer(
    stop: tokio::sync::oneshot::Sender<()>,
    mut task: JoinHandle<Result<()>>,
) -> Result<()> {
    let stop_result: Result<()> = stop
        .send(())
        .map_err(|_| "writer shutdown receiver dropped".into());
    let join_result = match timeout(Duration::from_secs(5), &mut task).await {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => Err(error.into()),
        Err(_) => {
            task.abort();
            let _ = timeout(Duration::from_secs(5), task).await;
            Err("writer did not stop within five seconds".into())
        }
    };
    match (stop_result, join_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(stop), Ok(())) => Err(stop),
        (Ok(()), Err(join)) => Err(join),
        (Err(stop), Err(join)) => Err(format!("writer shutdown failed: {stop}; {join}").into()),
    }
}

async fn exchange(
    output: &mut tcp::OwnedWriteHalf,
    input: &mut rockbee_node::transport::FrameReader<tcp::OwnedReadHalf>,
    request: &impl WireMessage,
) -> Result<ResponseEnvelope> {
    timeout(
        Duration::from_secs(2),
        rockbee_node::transport::write_frame(output, request),
    )
    .await
    .map_err(|_| "request write timed out")??;
    let frame = timeout(
        Duration::from_secs(2),
        rockbee_node::transport::read_frame(input),
    )
    .await
    .map_err(|_| "response read timed out")??
    .ok_or("server closed before responding")?;
    ResponseEnvelope::decode(&frame)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_rejects_value_that_cannot_fit_a_future_get_response() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let objects: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(directory.path())?);
    let store = timeout(
        Duration::from_secs(5),
        Store::open(Role::Writer, "put-value-boundary", objects.clone()),
    )
    .await
    .map_err(|_| "writer store open timed out")??;
    timeout(Duration::from_secs(5), store.put(b"k", b"original"))
        .await
        .map_err(|_| "writer seed PUT timed out")??;
    let (address, stop, task) = start_writer(store).await?;

    let body = async {
        let stream = timeout(Duration::from_secs(2), TcpStream::connect(&address))
            .await
            .map_err(|_| "writer connect timed out")??;
        let (input, mut output) = stream.into_split();
        let mut input = rockbee_node::transport::FrameReader::new(input);

        let oversized = RawPut {
            request_id: 7,
            key: b"k".to_vec(),
            value: vec![b'x'; MAX_FRAME - 25],
        };
        let response = exchange(&mut output, &mut input, &oversized).await?;
        if response.request_id != oversized.request_id {
            return Err("oversized PUT response ID did not correlate".into());
        }
        if !matches!(
            response.response,
            Response::Error {
                mutation_outcome: MutationOutcome::Rejected,
                ..
            }
        ) {
            return Err(
                "server acknowledged a value that cannot fit a maximum-ID GET response".into(),
            );
        }

        let get_original = RequestEnvelope {
            request_id: 8,
            request: Request::Get { key: b"k".to_vec() },
        };
        let response = exchange(&mut output, &mut input, &get_original).await?;
        if response.request_id != get_original.request_id
            || response.response
                != (Response::Value {
                    value: Some(b"original".to_vec()),
                })
        {
            return Err("rejected PUT changed the acknowledged value".into());
        }

        let valid = RawPut {
            request_id: 9,
            key: b"k".to_vec(),
            value: vec![b'y'; MAX_FRAME - 26],
        };
        let response = exchange(&mut output, &mut input, &valid).await?;
        if response.request_id != valid.request_id || response.response != Response::Ok {
            return Err("server did not acknowledge the largest round-trippable value".into());
        }

        let get = RequestEnvelope {
            request_id: u64::MAX,
            request: Request::Get { key: b"k".to_vec() },
        };
        let response = exchange(&mut output, &mut input, &get).await?;
        if response.request_id != u64::MAX
            || response.response
                != (Response::Value {
                    value: Some(valid.value),
                })
        {
            return Err("maximum-ID GET did not return the exact accepted value".into());
        }
        Ok(())
    }
    .await;

    let cleanup = stop_writer(stop, task).await;
    match (body, cleanup) {
        (Err(body), Ok(())) => Err(body),
        (Ok(()), Err(cleanup)) => Err(cleanup),
        (Err(body), Err(cleanup)) => {
            Err(format!("test body failed: {body}; cleanup failed: {cleanup}").into())
        }
        (Ok(()), Ok(())) => Ok(()),
    }
}
