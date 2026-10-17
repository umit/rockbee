//! Public mutation-error classification at the real Store -> Server -> RockBeeClient boundary.
//!
//! SlateDB writer fencing is the deterministic engine error here. A separate
//! controlled diagnostic establishes why an engine mutation error can follow a
//! durable WAL write; this regression protects the public classification without
//! recreating that object-store fault adapter.

use rockbee_client::{MutationError, MutationOutcome, RockBeeClient};
use rockbee_test_support::protocol::{MAX_FRAME, MAX_KEY_LEN};
use rockbee_test_support::storage::Role;
use rockbee_test_support::{Result, Server, Store};
use slatedb::object_store::{ObjectStore, memory::InMemory};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::timeout;

const OP_BOUND: Duration = Duration::from_secs(6);
const CLOSE_BOUND: Duration = Duration::from_secs(6);
const FENCED: &str = "detected newer DB client";

struct OwnedServer {
    name: &'static str,
    stop: tokio::sync::oneshot::Sender<()>,
    task: JoinHandle<Result<()>>,
    // A fenced writer's Store::close can correctly report its terminal engine error.
    allow_fenced_close: bool,
}

async fn start_server(
    name: &'static str,
    store: Store,
    allow_fenced_close: bool,
) -> Result<(String, OwnedServer)> {
    let server = timeout(OP_BOUND, Server::bind("127.0.0.1:0", store))
        .await
        .map_err(|_| format!("{name} bind exceeded operation bound"))??;
    let address = server.local_addr()?.to_string();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(server.run_until(async move {
        stopped.await.map_err(|_| "server stop sender dropped")?;
        Ok(())
    }));
    Ok((
        address,
        OwnedServer {
            name,
            stop,
            task,
            allow_fenced_close,
        },
    ))
}

async fn stop_server(mut server: OwnedServer) -> Result<()> {
    let stop = server
        .stop
        .send(())
        .map_err(|_| format!("{} stop receiver dropped", server.name));
    let joined = match timeout(CLOSE_BOUND, &mut server.task).await {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => Err(error.into()),
        Err(_) => {
            server.task.abort();
            let _ = server.task.await;
            Err(format!("{} did not stop within cleanup bound", server.name).into())
        }
    };
    let joined = match joined {
        Err(error) if server.allow_fenced_close && error.to_string().contains(FENCED) => Ok(()),
        other => other,
    };
    match (stop, joined) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(stop), Ok(())) => Err(stop.into()),
        (Ok(()), Err(joined)) => Err(joined),
        (Err(stop), Err(joined)) => {
            Err(format!("{} cleanup failed: {stop}; {joined}", server.name).into())
        }
    }
}

async fn connect(address: &str) -> Result<RockBeeClient> {
    timeout(OP_BOUND, RockBeeClient::connect(address))
        .await
        .map_err(|_| "client connect exceeded operation bound")?
}

async fn put(client: &RockBeeClient, key: &[u8], value: &[u8]) -> Result<()> {
    timeout(OP_BOUND, client.put(key, value, None))
        .await
        .map_err(|_| "PUT exceeded operation bound")?
        .map_err(Into::into)
}

async fn delete(client: &RockBeeClient, key: &[u8]) -> Result<()> {
    timeout(OP_BOUND, client.delete(key))
        .await
        .map_err(|_| "DELETE exceeded operation bound")?
        .map_err(Into::into)
}

async fn get(client: &RockBeeClient, key: &[u8]) -> Result<Option<Vec<u8>>> {
    timeout(OP_BOUND, client.get(key))
        .await
        .map_err(|_| "GET exceeded operation bound")?
}

fn check_error<T>(
    result: Result<T>,
    needle: &str,
    expected: Option<MutationOutcome>,
    label: &str,
) -> Result<()> {
    let error = match result {
        Ok(_) => return Err(format!("{label} unexpectedly succeeded").into()),
        Err(error) => error,
    };
    let message = error.to_string();
    if !message.contains(needle) {
        return Err(format!("{label} missing {needle:?}: {message}").into());
    }
    let actual = error
        .downcast_ref::<MutationError>()
        .map(MutationError::outcome);
    if actual != expected {
        return Err(
            format!("{label} expected {expected:?}, received {actual:?}: {message}").into(),
        );
    }
    Ok(())
}

async fn expect_value(
    client: &RockBeeClient,
    key: &[u8],
    expected: &[u8],
    label: &str,
) -> Result<()> {
    let actual = get(client, key).await?;
    if actual.as_deref() == Some(expected) {
        Ok(())
    } else {
        Err(format!("{label} expected {expected:?}, got {actual:?}").into())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engine_mutation_errors_are_conservative_while_preflight_rejections_are_definite()
-> Result<()> {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let prefix = "storage-error-contract";
    let protected_key = b"protected\0key";
    let protected_value = b"new-writer\0durable-value";

    let mut old_writer = None;
    let mut new_writer = None;
    let mut reader = None;

    let body: Result<()> = async {
        let old_store = timeout(OP_BOUND, Store::open(Role::Writer, prefix, objects.clone()))
            .await
            .map_err(|_| "old writer open exceeded operation bound")??;
        let (old_address, old_server) = start_server("old writer", old_store, true).await?;
        old_writer = Some(old_server);
        let old_client = connect(&old_address).await?;

        let new_store = timeout(OP_BOUND, Store::open(Role::Writer, prefix, objects.clone()))
            .await
            .map_err(|_| "new writer open exceeded operation bound")??;
        // Retain preflight results until the Store has an owned shutdown path.
        let oversized_key = vec![0; MAX_KEY_LEN + 1];
        let oversized_value = vec![0; MAX_FRAME - 4];
        let key_put = timeout(OP_BOUND, new_store.put(&oversized_key, b"never-sent"))
            .await
            .unwrap_or_else(|_| Err("Store key preflight timed out".into()));
        let key_delete = timeout(OP_BOUND, new_store.delete(&oversized_key))
            .await
            .unwrap_or_else(|_| Err("Store delete preflight timed out".into()));
        let value_put = timeout(OP_BOUND, new_store.put(b"never-sent", &oversized_value))
            .await
            .unwrap_or_else(|_| Err("Store value preflight timed out".into()));

        // Opening B claims a newer writer epoch. Its ACK establishes the protected
        // state before A is asked to mutate; no manifest-poll sleep is needed.
        let (new_address, new_server) = start_server("new writer", new_store, false).await?;
        new_writer = Some(new_server);
        check_error(
            key_put,
            "key exceeds",
            Some(MutationOutcome::Rejected),
            "Store oversized PUT key",
        )?;
        check_error(
            key_delete,
            "key exceeds",
            Some(MutationOutcome::Rejected),
            "Store oversized DELETE key",
        )?;
        check_error(
            value_put,
            "value exceeds",
            Some(MutationOutcome::Rejected),
            "Store oversized value",
        )?;
        let new_client = connect(&new_address).await?;
        put(&new_client, protected_key, protected_value).await?;

        check_error(
            put(&old_client, protected_key, b"stale").await,
            FENCED,
            Some(MutationOutcome::Unknown),
            "fenced PUT",
        )?;
        // GET reaches the same terminal engine but is not a mutation.
        check_error(
            get(&old_client, protected_key).await,
            FENCED,
            None,
            "fenced GET",
        )?;
        check_error(
            delete(&old_client, protected_key).await,
            FENCED,
            Some(MutationOutcome::Unknown),
            "fenced DELETE",
        )?;
        expect_value(
            &new_client,
            protected_key,
            protected_value,
            "new writer after fenced mutations",
        )
        .await?;

        let reader_store = timeout(OP_BOUND, Store::open(Role::Reader, prefix, objects.clone()))
            .await
            .map_err(|_| "reader open exceeded operation bound")??;
        let (reader_address, reader_server) = start_server("reader", reader_store, false).await?;
        reader = Some(reader_server);
        let reader_client = connect(&reader_address).await?;
        check_error(
            put(&reader_client, protected_key, b"reader-must-not-write").await,
            "read-only server",
            Some(MutationOutcome::Rejected),
            "read-only PUT",
        )?;
        check_error(
            delete(&reader_client, protected_key).await,
            "read-only server",
            Some(MutationOutcome::Rejected),
            "read-only DELETE",
        )?;
        expect_value(
            &new_client,
            protected_key,
            protected_value,
            "new writer after definite rejections",
        )
        .await
    }
    .await;

    let mut cleanup_failures = Vec::new();
    for server in [reader.take(), new_writer.take(), old_writer.take()]
        .into_iter()
        .flatten()
    {
        if let Err(error) = stop_server(server).await {
            cleanup_failures.push(error.to_string());
        }
    }
    match (body, cleanup_failures.is_empty()) {
        (Ok(()), true) => Ok(()),
        (Err(body), true) => Err(body),
        (Ok(()), false) => Err(format!("cleanup failed: {}", cleanup_failures.join("; ")).into()),
        (Err(body), false) => Err(format!(
            "body failed: {body}; cleanup failed: {}",
            cleanup_failures.join("; ")
        )
        .into()),
    }
}
