use rockbee_client::RockBeeClient;
use rockbee_test_support::storage::Role;
use rockbee_test_support::{Result, Server, Store};
use slatedb::object_store::{
    ObjectStore,
    chunked::ChunkedStore,
    local::LocalFileSystem,
    throttle::{ThrottleConfig, ThrottledStore},
};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, timeout, timeout_at};

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
            let _ = task.await;
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

async fn fresh_reader_observes(
    objects: Arc<dyn ObjectStore>,
    prefix: &str,
    key: &[u8],
    expected: Option<&[u8]>,
    budget: Duration,
) -> Result<()> {
    let deadline = Instant::now() + budget;
    let reader = timeout_at(deadline, Store::open(Role::Reader, prefix, objects)).await??;
    let observed = async {
        loop {
            let result = timeout_at(deadline, reader.get(key))
                .await
                .map_err(|_| "fresh reader did not observe the delayed durable state")?;
            if result?.as_deref() == expected {
                return Ok(());
            }
            sleep(Duration::from_millis(50)).await;
        }
    }
    .await;
    let closed = match timeout(Duration::from_secs(5), reader.close()).await {
        Ok(result) => result,
        Err(_) => Err("fresh reader did not close within five seconds".into()),
    };
    match (observed, closed) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(observed), Ok(())) => Err(observed),
        (Ok(()), Err(closed)) => Err(closed),
        (Err(observed), Err(closed)) => Err(format!(
            "reader observation failed: {observed}; reader close failed: {closed}"
        )
        .into()),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_put_and_delete_ack_only_after_delayed_wal_persistence() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let plain: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(directory.path())?);
    let throttled = Arc::new(ThrottledStore::new(
        ChunkedStore::new(plain.clone(), 4096),
        ThrottleConfig::default(),
    ));
    let objects: Arc<dyn ObjectStore> = throttled.clone();
    let writer = Store::open(Role::Writer, "delayed-ack", objects).await?;
    let (address, stop, task) = start_writer(writer).await?;
    let body = async {
        let client = RockBeeClient::connect(&address).await?;
        throttled.config_mut(|config| config.wait_put_per_call = Duration::from_millis(500));

        let mut put = client.put(b"delayed", b"exact\0bytes", None);
        if timeout(Duration::from_millis(75), &mut put).await.is_ok() {
            return Err("PUT completed before delayed WAL persistence".into());
        }
        put.await?;
        fresh_reader_observes(
            plain.clone(),
            "delayed-ack",
            b"delayed",
            Some(b"exact\0bytes"),
            Duration::from_secs(5),
        )
        .await?;

        let mut delete = Box::pin(client.delete(b"delayed"));
        if timeout(Duration::from_millis(75), delete.as_mut())
            .await
            .is_ok()
        {
            return Err("DELETE completed before delayed WAL persistence".into());
        }
        delete.await?;
        fresh_reader_observes(
            plain.clone(),
            "delayed-ack",
            b"delayed",
            None,
            Duration::from_secs(5),
        )
        .await
    }
    .await;

    throttled.config_mut(|config| config.wait_put_per_call = Duration::ZERO);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_timeout_does_not_ack_before_delayed_wal_write_later_persists() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let plain: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(directory.path())?);
    let throttled = Arc::new(ThrottledStore::new(
        ChunkedStore::new(plain.clone(), 4096),
        ThrottleConfig::default(),
    ));
    let objects: Arc<dyn ObjectStore> = throttled.clone();
    let writer = Store::open(Role::Writer, "late-durable", objects).await?;
    let (address, stop, task) = start_writer(writer).await?;
    let body = async {
        let client = RockBeeClient::connect(&address).await?;
        throttled.config_mut(|config| config.wait_put_per_call = Duration::from_secs(6));
        let mut put = client.put(b"late", b"durable-after-timeout", None);
        let result = timeout(Duration::from_secs(7), &mut put)
            .await
            .map_err(|_| "public client did not finish within its request budget")?;
        let error = match result {
            Ok(()) => return Err("delayed WAL write unexpectedly acknowledged".into()),
            Err(error) => error,
        };
        if !error.to_string().contains("timed out") {
            return Err(format!("expected client timeout, got: {error}").into());
        }

        // This does not shorten an already-started throttled put; it avoids slowing
        // reader metadata and later graceful shutdown work.
        throttled.config_mut(|config| config.wait_put_per_call = Duration::ZERO);
        fresh_reader_observes(
            plain.clone(),
            "late-durable",
            b"late",
            Some(b"durable-after-timeout"),
            Duration::from_secs(12),
        )
        .await?;
        if !error.to_string().contains("mutation outcome is unknown") {
            return Err(format!(
                "PUT persisted after its timeout, but the error omitted the unknown outcome: {error}"
            )
            .into());
        }
        Ok(())
    }
    .await;

    throttled.config_mut(|config| config.wait_put_per_call = Duration::ZERO);
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
