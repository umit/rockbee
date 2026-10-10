use rockbee_client::Client;
use rockbee_gateway::Gateway;
use rockbee_node::storage::Role;
use rockbee_node::{Result, Server, Store};
use slatedb::object_store::{ObjectStore, local::LocalFileSystem};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, timeout};

async fn visible(client: &Client, key: &[u8], expected: Option<&[u8]>) -> Result<()> {
    timeout(Duration::from_secs(30), async {
        loop {
            if client.get(key).await?.as_deref() == expected {
                return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(());
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await?
}

async fn stop(
    stop: tokio::sync::oneshot::Sender<()>,
    mut task: JoinHandle<Result<()>>,
) -> Result<()> {
    let _ = stop.send(());
    match timeout(Duration::from_secs(5), &mut task).await {
        Ok(result) => result?,
        Err(error) => {
            task.abort();
            let _ = task.await;
            Err(error.into())
        }
    }
}

// Catches TTL being lost by Client encoding, gateway forwarding, storage writes,
// or a warm reader returning an expired newest version/its older predecessor.
#[tokio::test]
async fn ttl_flows_through_gateway_and_expires_without_another_write() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let objects: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let writer_store = Store::open(Role::Writer, "ttl-gateway", objects.clone()).await?;
    writer_store.put(b"key\xff", b"older-forever").await?;
    let reader_store = Store::open(Role::Reader, "ttl-gateway", objects).await?;
    let writer = Server::bind("127.0.0.1:0", writer_store).await?;
    let reader = Server::bind("127.0.0.1:0", reader_store).await?;
    let writer_addr = writer.local_addr()?.to_string();
    let reader_addr = reader.local_addr()?.to_string();
    let (writer_stop, writer_stopped) = tokio::sync::oneshot::channel();
    let writer_task = tokio::spawn(writer.run_until(async {
        writer_stopped.await?;
        Ok(())
    }));
    let (reader_stop, reader_stopped) = tokio::sync::oneshot::channel();
    let reader_task = tokio::spawn(reader.run_until(async {
        reader_stopped.await?;
        Ok(())
    }));
    let gateway = Gateway::bind("127.0.0.1:0", writer_addr, vec![reader_addr]).await?;
    let gateway_addr = gateway.local_addr()?.to_string();
    let (gateway_stop, gateway_stopped) = tokio::sync::oneshot::channel();
    let gateway_task = tokio::spawn(gateway.run_until(async {
        gateway_stopped.await?;
        Ok(())
    }));

    let body: Result<()> = async {
        let client = Client::connect(&gateway_addr).await?;
        visible(&client, b"key\xff", Some(b"older-forever")).await?;
        let started = Instant::now();
        client
            .put(b"key\xff", b"new\0bytes", Some(Duration::from_secs(15)))
            .await?;
        visible(&client, b"key\xff", Some(b"new\0bytes")).await?;
        // GET must hide the record on expiry even if no compaction or later PUT
        // has occurred. The timeout is a test bound, not a latency promise.
        tokio::time::sleep_until(started + Duration::from_secs(16)).await;
        assert_eq!(client.get(b"key\xff").await?, None);
        assert_eq!(client.get(b"key\xff").await?, None);
        client
            .put(b"key\xff", b"forever", Some(Duration::ZERO))
            .await?;
        visible(&client, b"key\xff", Some(b"forever")).await?;
        client.delete(b"key\xff").await?;
        visible(&client, b"key\xff", None).await?;
        Ok(())
    }
    .await;
    let gateway_closed = stop(gateway_stop, gateway_task).await;
    let reader_closed = stop(reader_stop, reader_task).await;
    let writer_closed = stop(writer_stop, writer_task).await;
    body?;
    gateway_closed?;
    reader_closed?;
    writer_closed
}
