use rockbee_node::storage::Role;
use rockbee_node::{Result, Store};
use slatedb::Db;
use slatedb::config::{PutOptions, Ttl, WriteOptions};
use slatedb::object_store::{ObjectStore, local::LocalFileSystem};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;

fn backend(directory: &std::path::Path) -> Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(LocalFileSystem::new_with_prefix(directory)?))
}

#[tokio::test]
async fn latest_expired_value_is_hidden_before_native_compaction() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let writer = Store::open(Role::Writer, "ttl-expired", backend(directory.path())?).await?;

    writer.put_with_ttl(b"key", b"expired", 1).await?;
    sleep(Duration::from_millis(10)).await;

    assert_eq!(writer.get(b"key").await?, None);
    writer.close().await
}

#[tokio::test]
async fn native_expiry_metadata_can_remain_readable_before_compaction() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let db = Db::open("native-expiry", backend(directory.path())?).await?;
    let past = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| "test clock is before the Unix epoch")?
            .as_millis(),
    )? - 1;

    db.put_with_options(
        b"key",
        b"native-expired",
        &PutOptions {
            ttl: Ttl::ExpireAtMillis(past),
        },
        &WriteOptions::default(),
    )
    .await?
    .await_durable()
    .await?;

    assert_eq!(
        db.get_key_value(b"key").await?.map(|value| value.value),
        Some(b"native-expired".as_slice().into())
    );
    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn expiry_hides_the_latest_value_after_reopen_without_resurrecting_an_older_value()
-> Result<()> {
    let directory = tempfile::tempdir()?;
    let objects = backend(directory.path())?;
    let writer = Store::open(Role::Writer, "ttl-reopen", Arc::clone(&objects)).await?;
    writer.put(b"key", b"older").await?;
    writer.put_with_ttl(b"key", b"latest", 1).await?;
    sleep(Duration::from_millis(10)).await;
    assert_eq!(writer.get(b"key").await?, None);
    writer.close().await?;

    // SlateDB's default close flushes memtables to SSTs. A new reader starts
    // with a cold cache and must not expose either the expired or older value.
    let reader = Store::open(Role::Reader, "ttl-reopen", Arc::clone(&objects)).await?;
    assert_eq!(reader.get(b"key").await?, None);
    assert_eq!(reader.get(b"key").await?, None);
    reader.close().await?;

    let reopened = Store::open(Role::Writer, "ttl-reopen", objects).await?;
    assert_eq!(reopened.get(b"key").await?, None);
    reopened.close().await
}

#[tokio::test]
async fn ordinary_put_clears_a_previous_expiry() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let writer = Store::open(Role::Writer, "ttl-overwrite", backend(directory.path())?).await?;
    writer.put_with_ttl(b"key", b"short", 1).await?;
    writer.put(b"key", b"forever").await?;
    sleep(Duration::from_millis(10)).await;

    assert_eq!(
        writer.get(b"key").await?.as_deref(),
        Some(b"forever".as_slice())
    );
    writer.close().await
}

#[tokio::test]
async fn zero_ttl_and_read_only_ttl_writes_follow_the_existing_forever_and_role_contracts()
-> Result<()> {
    let directory = tempfile::tempdir()?;
    let objects = backend(directory.path())?;
    let writer = Store::open(Role::Writer, "ttl-zero", Arc::clone(&objects)).await?;
    writer.put_with_ttl(b"key", b"short", 1).await?;
    writer.put_with_ttl(b"key", b"forever", 0).await?;
    let reader = Store::open(Role::Reader, "ttl-zero", objects).await?;

    assert!(reader.put_with_ttl(b"key", b"changed", 1).await.is_err());
    sleep(Duration::from_millis(10)).await;
    assert_eq!(
        writer.get(b"key").await?.as_deref(),
        Some(b"forever".as_slice())
    );

    reader.close().await?;
    writer.close().await
}

#[tokio::test]
async fn positive_ttl_overwrite_replaces_the_previous_deadline() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let writer = Store::open(Role::Writer, "ttl-extension", backend(directory.path())?).await?;
    writer.put_with_ttl(b"key", b"short", 1).await?;
    writer.put_with_ttl(b"key", b"extended", 60_000).await?;
    sleep(Duration::from_millis(10)).await;
    assert_eq!(
        writer.get(b"key").await?.as_deref(),
        Some(b"extended".as_slice())
    );
    writer.close().await
}

#[tokio::test]
async fn overflowing_ttl_is_rejected_without_mutating_the_existing_value() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let writer = Store::open(Role::Writer, "ttl-overflow", backend(directory.path())?).await?;
    writer.put(b"key", b"original").await?;

    assert!(
        writer
            .put_with_ttl(b"key", b"invalid", u64::MAX)
            .await
            .is_err()
    );
    assert_eq!(
        writer.get(b"key").await?.as_deref(),
        Some(b"original".as_slice())
    );
    writer.close().await
}
