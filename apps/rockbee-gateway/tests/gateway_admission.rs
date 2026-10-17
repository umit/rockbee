//! Gateway-wide admission rejects overload before the writer-side relay sees a mutation.
use rockbee_client::RockBeeClient;
use rockbee_gateway::Gateway;
use rockbee_test_support::protocol::{Request, RequestEnvelope, Response, ResponseEnvelope};
use rockbee_test_support::storage::Role;
use rockbee_test_support::transport::{FrameReader, read_frame, write_frame};
use rockbee_test_support::{Result, Server, Store};
use slatedb::object_store::{ObjectStore, local::LocalFileSystem};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;

const OP_BOUND: Duration = Duration::from_secs(6);
const FAST_REJECTION_BOUND: Duration = Duration::from_secs(1);
const CLEANUP_BOUND: Duration = Duration::from_secs(6);
const REQUESTS: usize = 128;

fn run_server(server: Server, stopped: oneshot::Receiver<()>) -> JoinHandle<Result<()>> {
    tokio::spawn(server.run_until(async move {
        stopped.await?;
        Ok(())
    }))
}

fn run_gateway(gateway: Gateway, stopped: oneshot::Receiver<()>) -> JoinHandle<Result<()>> {
    tokio::spawn(gateway.run_until(async move {
        stopped.await?;
        Ok(())
    }))
}

async fn stop_owned(
    name: &str,
    stop: oneshot::Sender<()>,
    mut task: JoinHandle<Result<()>>,
) -> Result<()> {
    let sent: Result<()> = stop
        .send(())
        .map_err(|_| format!("{name} stop receiver dropped").into());
    let joined = match timeout(CLEANUP_BOUND, &mut task).await {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => Err(error.into()),
        Err(_) => {
            task.abort();
            let _ = task.await;
            Err(format!("{name} did not stop within {CLEANUP_BOUND:?}").into())
        }
    };
    match (sent, joined) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(sent), Ok(())) => Err(sent),
        (Ok(()), Err(joined)) => Err(joined),
        (Err(sent), Err(joined)) => Err(format!("{name} stop={sent}; join={joined}").into()),
    }
}

async fn read_request(
    input: &mut FrameReader<tokio::net::tcp::OwnedReadHalf>,
) -> Result<RequestEnvelope> {
    let frame = timeout(OP_BOUND, read_frame(input))
        .await??
        .ok_or("relay peer closed before expected request")?;
    RequestEnvelope::decode(&frame)
}

async fn read_response(
    input: &mut FrameReader<tokio::net::tcp::OwnedReadHalf>,
) -> Result<ResponseEnvelope> {
    let frame = timeout(OP_BOUND, read_frame(input))
        .await??
        .ok_or("writer closed before durable ACK")?;
    ResponseEnvelope::decode(&frame)
}

async fn writer_relay(
    listener: TcpListener,
    writer_address: String,
    expected: BTreeMap<Vec<u8>, Vec<u8>>,
    full: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
    recovery_key: Vec<u8>,
    recovery_value: Vec<u8>,
) -> Result<()> {
    let (gateway_stream, _) = timeout(OP_BOUND, listener.accept()).await??;
    let (gateway_input, mut gateway_output) = gateway_stream.into_split();
    let mut gateway_input = FrameReader::new(gateway_input);
    let writer_stream = timeout(OP_BOUND, TcpStream::connect(writer_address)).await??;
    let (writer_input, mut writer_output) = writer_stream.into_split();
    let mut writer_input = FrameReader::new(writer_input);
    let mut request_ids = BTreeSet::new();
    let mut seen_keys = BTreeSet::new();
    let mut previous_id = 0;

    for _ in 0..REQUESTS {
        let request = read_request(&mut gateway_input).await?;
        let Request::Put { key, value } = &request.request else {
            return Err(format!("relay expected a held PUT, got {request:?}").into());
        };
        if request.request_id == 0 || request.request_id <= previous_id {
            return Err(format!(
                "relay saw a non-increasing held request ID {} after {previous_id}",
                request.request_id
            )
            .into());
        }
        previous_id = request.request_id;
        if expected.get(key) != Some(value) || !seen_keys.insert(key.clone()) {
            return Err(format!("relay saw unexpected or duplicate held PUT {request:?}").into());
        }
        request_ids.insert(request.request_id);
        write_frame(&mut writer_output, &request).await?;
    }
    if seen_keys.len() != expected.len() || request_ids.len() != REQUESTS {
        return Err("relay did not see all distinct held PUT keys and IDs".into());
    }

    let mut acknowledgements = Vec::with_capacity(REQUESTS);
    let mut acknowledged_ids = BTreeSet::new();
    for _ in 0..REQUESTS {
        let response = read_response(&mut writer_input).await?;
        if response.response != Response::Ok
            || !request_ids.contains(&response.request_id)
            || !acknowledged_ids.insert(response.request_id)
        {
            return Err(format!("writer did not durably ACK one held PUT: {response:?}").into());
        }
        acknowledgements.push(response);
    }
    if acknowledged_ids != request_ids {
        return Err("writer did not durably ACK every held PUT".into());
    }
    full.send(())
        .map_err(|_| "test stopped waiting for relay full condition")?;
    timeout(OP_BOUND, release)
        .await
        .map_err(|_| "test did not release held durable ACKs")??;

    for response in acknowledgements {
        write_frame(&mut gateway_output, &response).await?;
    }

    let recovery = read_request(&mut gateway_input).await?;
    if recovery.request_id == 0 || recovery.request_id <= previous_id {
        return Err("recovery request had invalid or non-increasing ID".into());
    }
    if recovery.request
        != (Request::Put {
            key: recovery_key,
            value: recovery_value,
        })
    {
        return Err(format!(
            "relay saw a rejected mutation or wrong recovery request: {recovery:?}"
        )
        .into());
    }
    write_frame(&mut writer_output, &recovery).await?;
    let response = read_response(&mut writer_input).await?;
    if response.request_id != recovery.request_id || response.response != Response::Ok {
        return Err(format!("writer did not ACK recovery PUT: {response:?}").into());
    }
    write_frame(&mut gateway_output, &response).await?;
    let terminal = timeout(OP_BOUND, read_frame(&mut gateway_input)).await??;
    if terminal.is_some() {
        return Err("relay saw a duplicate or extra writer mutation after recovery".into());
    }
    Ok(())
}

async fn finish_relay(mut task: JoinHandle<Result<()>>) -> Result<()> {
    match timeout(CLEANUP_BOUND, &mut task).await {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => Err(error.into()),
        Err(_) => {
            task.abort();
            let _ = task.await;
            Err("writer relay did not finish".into())
        }
    }
}

async fn direct_exact(client: &RockBeeClient, key: &[u8], expected: Option<&[u8]>) -> Result<()> {
    let observed = timeout(OP_BOUND, client.get(key)).await??;
    if observed.as_deref() != expected {
        return Err(format!("direct writer state mismatch for {key:?}: {observed:?}").into());
    }
    Ok(())
}

async fn reopen_exact(
    objects: Arc<dyn ObjectStore>,
    prefix: &str,
    expected: &[(Vec<u8>, Option<Vec<u8>>)],
) -> Result<()> {
    let reader = timeout(CLEANUP_BOUND, Store::open(Role::Reader, prefix, objects))
        .await
        .map_err(|_| "fresh reader did not open within cleanup bound")??;
    let body: Result<()> = async {
        for (key, value) in expected {
            let observed = timeout(OP_BOUND, reader.get(key)).await??;
            if observed.as_deref() != value.as_deref() {
                return Err(
                    format!("fresh reader state mismatch for {key:?}: {observed:?}").into(),
                );
            }
        }
        Ok(())
    }
    .await;
    let closed = timeout(CLEANUP_BOUND, reader.close())
        .await
        .map_err(|_| "fresh reader did not close within cleanup bound")?;
    match (body, closed) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(body), Ok(())) => Err(body),
        (Ok(()), Err(close)) => Err(close),
        (Err(body), Err(close)) => Err(format!("fresh reader body={body}; close={close}").into()),
    }
}

/// A saturated gateway peer must not execute rejected mutations or poison another RockBeeClient.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overload_peer_never_executes_rejected_mutations_or_poisons_another_client() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let objects: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(directory.path())?);
    let prefix = "gateway-admission";
    // Bind every TCP endpoint before opening SlateDB, whose Store::open starts engine work.
    let relay_listener = TcpListener::bind("127.0.0.1:0").await?;
    let relay_address = relay_listener.local_addr()?.to_string();
    // This test exercises mutations only; Gateway still needs a syntactically valid reader
    // endpoint, but no GET is routed to this listener.
    let unused_reader = TcpListener::bind("127.0.0.1:0").await?;
    let unused_reader_address = unused_reader.local_addr()?.to_string();
    let gateway = Gateway::bind("127.0.0.1:0", relay_address, vec![unused_reader_address]).await?;
    let gateway_address = gateway.local_addr()?.to_string();

    let writer = Server::bind(
        "127.0.0.1:0",
        Store::open(Role::Writer, prefix, Arc::clone(&objects)).await?,
    )
    .await?;
    let writer_address = writer.local_addr()?.to_string();
    let protected_key = b"protected\0key".to_vec();
    let protected_value = b"protected\xffvalue".to_vec();
    let rejected_key = b"rejected-put\0key".to_vec();
    let recovery_key = b"recovery-put\xffkey".to_vec();
    let recovery_value = b"recovery-value\0".to_vec();
    let mut expected = BTreeMap::new();
    for index in 0..REQUESTS {
        expected.insert(
            format!("held-{index:03}").into_bytes(),
            vec![index as u8, 0, 0xff, (index >> 1) as u8],
        );
    }

    // Every fallible setup step completed above. From here, all owned tasks have a
    // stop path below even if the test body reports an assertion failure.
    let (writer_stop, writer_stopped) = oneshot::channel();
    let writer_task = run_server(writer, writer_stopped);
    let (full, full_wait) = oneshot::channel();
    let (release, release_wait) = oneshot::channel();
    let relay_task = tokio::spawn(writer_relay(
        relay_listener,
        writer_address.clone(),
        expected.clone(),
        full,
        release_wait,
        recovery_key.clone(),
        recovery_value.clone(),
    ));
    let (gateway_stop, gateway_stopped) = oneshot::channel();
    let gateway_task = run_gateway(gateway, gateway_stopped);

    let body: Result<()> = async {
        let writer_client = RockBeeClient::connect(&writer_address).await?;
        timeout(
            OP_BOUND,
            writer_client.put(&protected_key, &protected_value, None),
        )
        .await??;
        let first = RockBeeClient::connect(&gateway_address).await?;
        let mut held = Vec::with_capacity(REQUESTS);
        for (key, value) in &expected {
            held.push(first.put(key, value, None));
        }
        timeout(OP_BOUND, full_wait)
            .await
            .map_err(|_| "relay did not collect 128 real durable ACKs")??;

        let second = RockBeeClient::connect(&gateway_address).await?;
        let put_error = timeout(
            FAST_REJECTION_BOUND,
            second.put(&rejected_key, b"must-not-forward", None),
        )
        .await
        .map_err(|_| "overflow PUT did not fail quickly")?;
        if put_error.is_ok() {
            return Err("overflow PUT unexpectedly returned durable ACK".into());
        }
        let delete_error = timeout(FAST_REJECTION_BOUND, second.delete(&protected_key))
            .await
            .map_err(|_| "overflow DELETE did not fail quickly")?;
        if delete_error.is_ok() {
            return Err("overflow DELETE unexpectedly returned durable ACK".into());
        }

        release
            .send(())
            .map_err(|_| "relay release receiver dropped")?;
        timeout(OP_BOUND, async {
            for handle in held {
                handle.await?;
            }
            Result::<()>::Ok(())
        })
        .await
        .map_err(|_| "held PUT acknowledgements did not drain")??;

        for (key, value) in &expected {
            direct_exact(&writer_client, key, Some(value)).await?;
        }
        direct_exact(&writer_client, &rejected_key, None).await?;
        direct_exact(&writer_client, &protected_key, Some(&protected_value)).await?;

        timeout(OP_BOUND, second.put(&recovery_key, &recovery_value, None)).await??;
        direct_exact(&writer_client, &recovery_key, Some(&recovery_value)).await?;
        Ok(())
    }
    .await;

    let gateway_closed = stop_owned("gateway", gateway_stop, gateway_task).await;
    let relay_closed = finish_relay(relay_task).await;
    let writer_closed = stop_owned("writer", writer_stop, writer_task).await;
    drop(unused_reader);
    let reopened = if body.is_ok()
        && gateway_closed.is_ok()
        && relay_closed.is_ok()
        && writer_closed.is_ok()
    {
        let mut state: Vec<_> = expected
            .into_iter()
            .map(|(key, value)| (key, Some(value)))
            .collect();
        state.push((rejected_key, None));
        state.push((protected_key, Some(protected_value)));
        state.push((recovery_key, Some(recovery_value)));
        reopen_exact(objects, prefix, &state).await
    } else {
        Ok(())
    };
    match (body, gateway_closed, relay_closed, writer_closed, reopened) {
        (Ok(()), Ok(()), Ok(()), Ok(()), Ok(())) => Ok(()),
        (body, gateway, relay, writer, reopened) => Err(format!(
            "body={body:?}; gateway_close={gateway:?}; relay_close={relay:?}; writer_close={writer:?}; reopen={reopened:?}"
        )
        .into()),
    }
}
