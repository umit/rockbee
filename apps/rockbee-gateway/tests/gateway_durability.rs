use rockbee_client::RockBeeClient;
use rockbee_gateway::Gateway;
use rockbee_test_support::protocol::{Request, RequestEnvelope, Response, ResponseEnvelope};
use rockbee_test_support::storage::Role;
use rockbee_test_support::transport::{FrameReader, read_frame, write_frame};
use rockbee_test_support::{Result, Server, Store};
use slatedb::object_store::{ObjectStore, local::LocalFileSystem};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, timeout, timeout_at};

const DROP_INITIAL_ACK: bool = true;
const WRITE_BOUND: Duration = Duration::from_secs(6);
const RELAY_IDLE_BOUND: Duration = Duration::from_secs(30);
const CONVERGENCE_BOUND: Duration = Duration::from_secs(30);

#[derive(Clone)]
struct Expected {
    request: Request,
    drop_ack: bool,
}

async fn start_server(
    server: Server,
) -> Result<(
    String,
    tokio::sync::oneshot::Sender<()>,
    JoinHandle<Result<()>>,
)> {
    let address = server.local_addr()?.to_string();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(server.run_until(async move {
        stopped.await?;
        Ok(())
    }));
    Ok((address, stop, task))
}

async fn start_gateway(
    gateway: Gateway,
) -> Result<(
    String,
    tokio::sync::oneshot::Sender<()>,
    JoinHandle<Result<()>>,
)> {
    let address = gateway.local_addr()?.to_string();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(gateway.run_until(async move {
        stopped.await?;
        Ok(())
    }));
    Ok((address, stop, task))
}

async fn stop_owned(
    name: &str,
    stop: tokio::sync::oneshot::Sender<()>,
    mut task: JoinHandle<Result<()>>,
) -> Result<()> {
    let sent: Result<()> = stop
        .send(())
        .map_err(|_| format!("{name} stop receiver dropped").into());
    let joined = match timeout(Duration::from_secs(5), &mut task).await {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => Err(error.into()),
        Err(_) => {
            task.abort();
            let _ = task.await;
            Err(format!("{name} did not stop within five seconds").into())
        }
    };
    match (sent, joined) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(sent), Ok(())) => Err(sent),
        (Ok(()), Err(joined)) => Err(joined),
        (Err(sent), Err(joined)) => Err(format!("{name} shutdown failed: {sent}; {joined}").into()),
    }
}

async fn next_request(
    input: &mut FrameReader<tokio::net::tcp::OwnedReadHalf>,
) -> Result<RequestEnvelope> {
    let frame = timeout(RELAY_IDLE_BOUND, read_frame(input))
        .await??
        .ok_or("relay peer closed before expected request")?;
    RequestEnvelope::decode(&frame)
}

async fn relay_connection(
    downstream: TcpStream,
    writer: &str,
    expected: &[Expected],
) -> Result<()> {
    let (input, mut output) = downstream.into_split();
    let mut input = FrameReader::new(input);
    let upstream = timeout(WRITE_BOUND, TcpStream::connect(writer)).await??;
    let (upstream_input, mut upstream_output) = upstream.into_split();
    let mut upstream_input = FrameReader::new(upstream_input);
    for step in expected {
        let request = next_request(&mut input).await?;
        if request.request_id == 0 || request.request != step.request {
            return Err(format!("relay request mismatch: {request:?}").into());
        }
        timeout(WRITE_BOUND, write_frame(&mut upstream_output, &request)).await??;
        let frame = timeout(WRITE_BOUND, read_frame(&mut upstream_input))
            .await??
            .ok_or("real writer closed before ACK")?;
        let response = ResponseEnvelope::decode(&frame)?;
        if response.request_id != request.request_id || response.response != Response::Ok {
            return Err(format!("real writer did not return matching OK: {response:?}").into());
        }
        if step.drop_ack {
            return Ok(());
        }
        timeout(WRITE_BOUND, write_frame(&mut output, &response)).await??;
    }
    match read_frame(&mut input).await? {
        None => Ok(()),
        Some(frame) => {
            Err(format!("unexpected writer-link frame after four mutations: {frame:?}").into())
        }
    }
}

async fn fault_relay(listener: TcpListener, writer: String, drop_initial_ack: bool) -> Result<()> {
    let plan = [
        vec![Expected {
            request: Request::Put {
                key: b"lost-key".to_vec(),
                value: b"original\0bytes".to_vec(),
            },
            drop_ack: drop_initial_ack,
        }],
        vec![
            Expected {
                request: Request::Put {
                    key: b"marker-key".to_vec(),
                    value: b"marker".to_vec(),
                },
                drop_ack: false,
            },
            Expected {
                request: Request::Delete {
                    key: b"lost-key".to_vec(),
                },
                drop_ack: true,
            },
        ],
        vec![Expected {
            request: Request::Put {
                key: b"new-key".to_vec(),
                value: b"distinct\xffvalue".to_vec(),
            },
            drop_ack: false,
        }],
    ];
    for expected in &plan {
        let (stream, _) = timeout(RELAY_IDLE_BOUND, listener.accept()).await??;
        relay_connection(stream, &writer, expected).await?;
    }
    Ok(())
}

async fn wait_for(client: &RockBeeClient, key: &[u8], expected: Option<&[u8]>) -> Result<()> {
    let deadline = Instant::now() + CONVERGENCE_BOUND;
    let mut last = None;
    while Instant::now() < deadline {
        match timeout_at(deadline, client.get(key)).await {
            Ok(Ok(value)) if value.as_deref() == expected => return Ok(()),
            Ok(Ok(value)) => last = Some(format!("stale value {value:?}")),
            Ok(Err(error)) => last = Some(format!("temporary reader error: {error}")),
            Err(_) => break,
        }
        sleep(Duration::from_millis(50)).await;
    }
    Err(format!(
        "reader did not converge for {key:?}: {}",
        last.unwrap_or_else(|| "deadline elapsed".into())
    )
    .into())
}

fn require_unknown(result: rockbee_client::MutationResult, mutation: &str) -> Result<()> {
    match result {
        Ok(()) => Err(format!("{mutation} unexpectedly returned a durable ACK").into()),
        Err(error) if error.outcome() == rockbee_client::MutationOutcome::Unknown => Ok(()),
        Err(error) => Err(format!("{mutation} had wrong error: {error}").into()),
    }
}

async fn finish_relay(mut relay_task: JoinHandle<Result<()>>) -> Result<()> {
    match timeout(Duration::from_secs(6), &mut relay_task).await {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => Err(error.into()),
        Err(_) => {
            relay_task.abort();
            let _ = relay_task.await;
            Err("relay did not finish within six seconds".into())
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_gateway_writer_acks_are_unknown_without_replaying_durable_mutations() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let objects: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(directory.path())?);
    let prefix = "gateway-lost-ack";
    let writer_store = Store::open(Role::Writer, prefix, objects.clone()).await?;
    let reader_store = Store::open(Role::Reader, prefix, objects).await?;
    let writer = Server::bind("127.0.0.1:0", writer_store).await?;
    let reader = Server::bind("127.0.0.1:0", reader_store).await?;
    let (writer_address, writer_stop, writer_task) = start_server(writer).await?;
    let (reader_address, reader_stop, reader_task) = start_server(reader).await?;
    let relay_listener = TcpListener::bind("127.0.0.1:0").await?;
    let relay_address = relay_listener.local_addr()?.to_string();
    let relay_task = tokio::spawn(fault_relay(
        relay_listener,
        writer_address,
        DROP_INITIAL_ACK,
    ));
    let gateway = Gateway::bind("127.0.0.1:0", relay_address, vec![reader_address]).await?;
    let (gateway_address, gateway_stop, gateway_task) = start_gateway(gateway).await?;

    let body: Result<()> = async {
        let client = RockBeeClient::connect(&gateway_address).await?;
        require_unknown(
            client.put(b"lost-key", b"original\0bytes", None).await,
            "initial PUT",
        )?;
        wait_for(&client, b"lost-key", Some(b"original\0bytes")).await?;
        timeout(WRITE_BOUND, client.put(b"marker-key", b"marker", None)).await??;
        require_unknown(
            timeout(WRITE_BOUND, client.delete(b"lost-key")).await?,
            "DELETE",
        )?;
        wait_for(&client, b"lost-key", None).await?;
        timeout(
            WRITE_BOUND,
            client.put(b"new-key", b"distinct\xffvalue", None),
        )
        .await??;
        wait_for(&client, b"new-key", Some(b"distinct\xffvalue")).await?;
        Ok(())
    }
    .await;

    let gateway_closed = stop_owned("gateway", gateway_stop, gateway_task).await;
    let relay_closed = finish_relay(relay_task).await;
    let reader_closed = stop_owned("reader", reader_stop, reader_task).await;
    let writer_closed = stop_owned("writer", writer_stop, writer_task).await;
    match (body, gateway_closed, relay_closed, reader_closed, writer_closed) {
        (Ok(()), Ok(()), Ok(()), Ok(()), Ok(())) => Ok(()),
        (Err(body), Ok(()), Ok(()), Ok(()), Ok(())) => Err(body),
        (body, gateway, relay, reader, writer) => Err(format!(
            "body={body:?}; gateway_close={gateway:?}; relay_close={relay:?}; reader_close={reader:?}; writer_close={writer:?}"
        ).into()),
    }
}
