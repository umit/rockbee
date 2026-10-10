use rockbee_node::storage::Role;
use rockbee_node::{Result, Store};
use slatedb::object_store::{ObjectStore, local::LocalFileSystem};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Instant, sleep};

fn backend(directory: &std::path::Path) -> Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(LocalFileSystem::new_with_prefix(directory)?))
}

async fn wait_for(reader: &Store, key: &[u8], expected: Option<&[u8]>) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if reader.get(key).await?.as_deref() == expected {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("reader did not converge within 60 seconds".into());
        }
        sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn acknowledged_put_and_delete_survive_reopen() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let objects = backend(directory.path())?;
    let writer = Store::open(Role::Writer, "reopen", Arc::clone(&objects)).await?;
    writer.put(b"product:100", b"first").await?;
    writer.put(b"product:100", &[0x00, 0x80, 0xff]).await?;
    writer.put(b"\0", b"different key").await?;
    let largest = vec![b'k'; 65_534];
    writer.put(&largest, b"largest").await?;
    let oversized = vec![b'x'; 65_535];
    assert!(writer.put(&oversized, b"invalid").await.is_err());
    assert!(writer.get(&oversized).await.is_err());
    assert!(writer.delete(&oversized).await.is_err());
    writer.close().await?;
    let writer = Store::open(Role::Writer, "reopen", Arc::clone(&objects)).await?;
    assert_eq!(
        writer.get(b"product:100").await?.as_deref(),
        Some(&[0x00, 0x80, 0xff][..])
    );
    assert_eq!(
        writer.get(b"\0").await?.as_deref(),
        Some(&b"different key"[..])
    );
    assert_eq!(
        writer.get(&largest).await?.as_deref(),
        Some(&b"largest"[..])
    );
    writer.delete(b"product:100").await?;
    writer.delete(b"product:100").await?;
    writer.close().await?;
    let writer = Store::open(Role::Writer, "reopen", objects).await?;
    assert_eq!(writer.get(b"product:100").await?, None);
    writer.close().await
}

#[tokio::test]
async fn readers_reject_writes_without_mutating_shared_data() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let objects = backend(directory.path())?;
    let writer = Store::open(Role::Writer, "read-only", Arc::clone(&objects)).await?;
    writer.put(b"product:100", b"original").await?;
    let reader = Store::open(Role::Reader, "read-only", objects).await?;
    assert!(reader.put(b"product:100", b"changed").await.is_err());
    assert!(reader.delete(b"product:100").await.is_err());
    assert_eq!(
        writer.get(b"product:100").await?.as_deref(),
        Some(&b"original"[..])
    );
    reader.close().await?;
    writer.close().await
}

#[tokio::test]
async fn two_readers_follow_updates_delete_and_writer_restart() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let objects = backend(directory.path())?;
    let writer = Store::open(Role::Writer, "shared", Arc::clone(&objects)).await?;
    writer.put(b"product:100", b"v1").await?;
    let first = Store::open(Role::Reader, "shared", Arc::clone(&objects)).await?;
    let second = Store::open(Role::Reader, "shared", Arc::clone(&objects)).await?;
    wait_for(&first, b"product:100", Some(b"v1")).await?;
    wait_for(&second, b"product:100", Some(b"v1")).await?;
    writer.close().await?;
    let writer = Store::open(Role::Writer, "shared", Arc::clone(&objects)).await?;
    writer.put(b"product:100", b"v2").await?;
    wait_for(&first, b"product:100", Some(b"v2")).await?;
    wait_for(&second, b"product:100", Some(b"v2")).await?;
    first.close().await?;
    writer.delete(b"product:100").await?;
    wait_for(&second, b"product:100", None).await?;
    let restarted = Store::open(Role::Reader, "shared", objects).await?;
    wait_for(&restarted, b"product:100", None).await?;
    restarted.close().await?;
    second.close().await?;
    writer.close().await
}

#[tokio::test]
async fn stores_reject_empty_keys_and_put_values_without_changing_data() -> Result<()> {
    use rockbee_node::protocol::{MutationError, MutationOutcome};

    let directory = tempfile::tempdir()?;
    let objects = backend(directory.path())?;
    let writer = Store::open(Role::Writer, "nonempty-inputs", Arc::clone(&objects)).await?;
    writer.put(&[0], &[0, 255]).await?;
    let reader = Store::open(Role::Reader, "nonempty-inputs", objects).await?;
    let body: Result<()> = async {
        for result in [
            writer.put(b"", b"value").await,
            writer.put(&[0], b"").await,
            writer.put_with_ttl(b"", b"value", 1).await,
            writer.put_with_ttl(&[0], b"", 1).await,
            writer.delete(b"").await,
        ] {
            let error = result.expect_err("Store must reject empty mutation inputs");
            assert_eq!(
                error
                    .downcast_ref::<MutationError>()
                    .map(MutationError::outcome),
                Some(MutationOutcome::Rejected)
            );
        }
        assert!(writer.get(b"").await.is_err());
        assert!(reader.get(b"").await.is_err());
        assert_eq!(writer.get(&[0]).await?, Some(vec![0, 255]));
        wait_for(&reader, &[0], Some(&[0, 255])).await?;
        Ok(())
    }
    .await;
    let reader_close = reader.close().await;
    let writer_close = writer.close().await;
    body?;
    reader_close?;
    writer_close
}
