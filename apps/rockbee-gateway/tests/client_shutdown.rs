//! Public client shutdown drains eager PUTs before disconnecting the gateway link.

use rockbee_client::{Client, MutationOutcome};
use rockbee_gateway::Gateway;
use rockbee_node::storage::Role;
use rockbee_node::{Result, Server, Store};
use slatedb::object_store::{ObjectStore, local::LocalFileSystem};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;

const OP_BOUND: Duration = Duration::from_secs(6);
const CLEANUP_BOUND: Duration = Duration::from_secs(6);
const PUTS: usize = 16;

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

async fn fresh_reader_observes(
    objects: Arc<dyn ObjectStore>,
    prefix: &str,
    expected: &[(Vec<u8>, Vec<u8>)],
) -> Result<()> {
    let reader = timeout(CLEANUP_BOUND, Store::open(Role::Reader, prefix, objects))
        .await
        .map_err(|_| "fresh reader did not open within cleanup bound")??;
    let body: Result<()> = async {
        for (key, value) in expected {
            let observed = timeout(OP_BOUND, reader.get(key)).await??;
            if observed.as_deref() != Some(value.as_slice()) {
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
        (Err(body), Err(close)) => Err(format!("reader body={body}; close={close}").into()),
    }
}

/// Shutdown admits no new mutation, but durably finishes every eager PUT it accepted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_drains_eager_puts_and_rejects_later_puts() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let objects: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(directory.path())?);
    let prefix = "client-shutdown";

    // Reserve the unused reader address before opening SlateDB, whose writer starts engine work.
    let unused_reader = TcpListener::bind("127.0.0.1:0").await?;
    let unused_reader_address = unused_reader.local_addr()?.to_string();
    let writer = Server::bind(
        "127.0.0.1:0",
        Store::open(Role::Writer, prefix, Arc::clone(&objects)).await?,
    )
    .await?;
    let writer_address = writer.local_addr()?.to_string();
    let gateway = Gateway::bind("127.0.0.1:0", writer_address, vec![unused_reader_address]).await?;
    let gateway_address = gateway.local_addr()?.to_string();

    let expected: Vec<_> = (0..PUTS)
        .map(|index| {
            (
                vec![0, index as u8, 0xff, b'k'],
                vec![0xff, index as u8, 0, b'v'],
            )
        })
        .collect();

    let (writer_stop, writer_stopped) = oneshot::channel();
    let writer_task = run_server(writer, writer_stopped);
    let (gateway_stop, gateway_stopped) = oneshot::channel();
    let gateway_task = run_gateway(gateway, gateway_stopped);

    let body: Result<()> = async {
        let client = Client::connect(&gateway_address).await?;
        let mut handles = Vec::with_capacity(PUTS - 1);
        for (index, (key, value)) in expected.iter().enumerate() {
            let handle = client.put(key, value, None);
            if index == 0 {
                // A caller may drop an eager handle; shutdown must still drain its PUT.
                drop(handle);
            } else {
                handles.push(handle);
            }
        }

        timeout(OP_BOUND, client.shutdown())
            .await
            .map_err(|_| "client shutdown did not finish within operation bound")?;

        timeout(OP_BOUND, async {
            for handle in handles {
                handle.await?;
            }
            Result::<()>::Ok(())
        })
        .await
        .map_err(|_| "accepted eager PUTs did not drain during shutdown")??;

        let rejected = timeout(
            OP_BOUND,
            client.put(b"after\0shutdown", b"must-not-send", None),
        )
        .await
        .map_err(|_| "post-shutdown PUT did not finish")?;
        match rejected {
            Err(error) if error.outcome() == MutationOutcome::NotSent => Ok(()),
            Ok(()) => Err("post-shutdown PUT unexpectedly received a durable ACK".into()),
            Err(error) => Err(format!("post-shutdown PUT had wrong outcome: {error}").into()),
        }
    }
    .await;

    let gateway_closed = stop_owned("gateway", gateway_stop, gateway_task).await;
    let writer_closed = stop_owned("writer", writer_stop, writer_task).await;
    drop(unused_reader);
    let reopened = if body.is_ok() && gateway_closed.is_ok() && writer_closed.is_ok() {
        fresh_reader_observes(objects, prefix, &expected).await
    } else {
        Ok(())
    };
    match (body, gateway_closed, writer_closed, reopened) {
        (Ok(()), Ok(()), Ok(()), Ok(())) => Ok(()),
        (body, gateway, writer, reopened) => Err(format!(
            "body={body:?}; gateway_close={gateway:?}; writer_close={writer:?}; reopen={reopened:?}"
        )
        .into()),
    }
}
