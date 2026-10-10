use rockbee_client::{Client, ClientConfig, MutationOutcome, Result};
use rockbee_gateway::Gateway;
use rockbee_node::{Server, Store, storage::Role};
use slatedb::object_store::memory::InMemory;
use std::sync::Arc;
use tokio::{
    net::TcpSocket,
    sync::oneshot,
    time::{Duration, timeout},
};

#[tokio::test]
async fn received_but_unforwarded_gateway_mutation_is_rejected_not_client_not_sent() -> Result<()> {
    // Reserve a valid loopback address without accepting writer connections.
    // On macOS this can stall rather than fail immediately; give the client
    // more time than the gateway's five-second upstream deadline so it can
    // receive the gateway's rejection of the unforwarded mutation.
    let writer = TcpSocket::new_v4()?;
    writer.bind("127.0.0.1:0".parse()?)?;
    let writer_address = writer.local_addr()?.to_string();
    let reader = TcpSocket::new_v4()?;
    reader.bind("127.0.0.1:0".parse()?)?;
    let gateway = Gateway::bind(
        "127.0.0.1:0",
        writer_address,
        vec![reader.local_addr()?.to_string()],
    )
    .await?;
    let client = Client::connect_with_config(
        &gateway.local_addr()?.to_string(),
        ClientConfig::new(Duration::from_secs(10), 1, 1)?,
    )
    .await?;
    let (stop, stopped) = oneshot::channel();
    let task = tokio::spawn(gateway.run_until(async {
        stopped.await?;
        Ok(())
    }));
    let body: Result<()> = async {
        for result in [
            client.put(b"key", b"value", None).await,
            client.delete(b"key").await,
        ] {
            assert_eq!(result.unwrap_err().outcome(), MutationOutcome::Rejected);
        }
        Ok(())
    }
    .await;
    let _ = stop.send(());
    timeout(Duration::from_secs(3), task).await???;
    body
}

#[tokio::test]
async fn read_only_storage_rejection_survives_both_rpc_hops_without_changing_data() -> Result<()> {
    let objects = Arc::new(InMemory::new());
    let writer_store = Store::open(Role::Writer, "typed-errors", objects.clone()).await?;
    writer_store.put(b"key", b"original").await?;
    let reader_store = Store::open(Role::Reader, "typed-errors", objects).await?;
    let reader = Server::bind("127.0.0.1:0", reader_store).await?;
    let reader_address = reader.local_addr()?.to_string();
    let spare = TcpSocket::new_v4()?;
    spare.bind("127.0.0.1:0".parse()?)?;
    let gateway = Gateway::bind(
        "127.0.0.1:0",
        reader_address.clone(),
        vec![spare.local_addr()?.to_string()],
    )
    .await?;
    let client = Client::connect(&gateway.local_addr()?.to_string()).await?;
    let (stop_reader, stopped_reader) = oneshot::channel();
    let reader_task = tokio::spawn(reader.run_until(async {
        stopped_reader.await?;
        Ok(())
    }));
    let (stop_gateway, stopped_gateway) = oneshot::channel();
    let gateway_task = tokio::spawn(gateway.run_until(async {
        stopped_gateway.await?;
        Ok(())
    }));
    let body: Result<()> = async {
        for result in [
            client.put(b"key", b"bad", None).await,
            client.delete(b"key").await,
        ] {
            assert_eq!(result.unwrap_err().outcome(), MutationOutcome::Rejected);
        }
        assert_eq!(writer_store.get(b"key").await?, Some(b"original".to_vec()));
        let direct_reader = Client::connect(&reader_address).await?;
        assert_eq!(direct_reader.get(b"key").await?, Some(b"original".to_vec()));
        Ok(())
    }
    .await;
    let _ = stop_gateway.send(());
    let _ = stop_reader.send(());
    timeout(Duration::from_secs(3), gateway_task).await???;
    timeout(Duration::from_secs(3), reader_task).await???;
    writer_store.close().await?;
    body
}
