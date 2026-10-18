use rockbee_protocol::{MutationError, MutationOutcome, table::table_key};
use rockbee_reader::Reader;
use rockbee_runtime::{QueryStorage, ReadStorage, Result, WriteStorage};
use rockbee_runtime::{config::ReaderConfig, memory::MemoryConfig};
use rockbee_writer::Writer;
use serde_json::json;
use slatedb::object_store::{ObjectStore, local::LocalFileSystem};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Instant, sleep};

#[tokio::test]
async fn capabilities_preserve_kv_table_isolation_and_reader_queries() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let objects: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let memory = MemoryConfig::default();
    let writer = Writer::open("capability-roundtrip", objects.clone(), &memory).await?;
    let raw_key = table_key("products", "42")?;
    let document = br#"{"name":"Headphones","price":89}"#;

    WriteStorage::put(&writer, &raw_key, b"opaque bytes").await?;
    WriteStorage::table_put(&writer, "products", "42", document, 0).await?;
    assert_eq!(
        ReadStorage::get(&writer, &raw_key).await?,
        Some(b"opaque bytes".to_vec())
    );
    assert_eq!(
        ReadStorage::table_get(&writer, "products", "42").await?,
        Some(document.to_vec())
    );
    let reader = Reader::open(
        "capability-roundtrip",
        objects,
        &memory,
        &ReaderConfig::default(),
    )
    .await?;
    let observed: Result<()> = async {
        let deadline = Instant::now() + Duration::from_secs(30);
        // Reader and Parquet visibility are independent of the mutation ACK.
        loop {
            let kv = ReadStorage::get(&reader, &raw_key).await?;
            let row = ReadStorage::table_get(&reader, "products", "42").await?;
            let query = QueryStorage::query(&reader, "products", "SELECT id FROM products").await;
            if kv.as_deref() == Some(b"opaque bytes")
                && row.as_deref() == Some(document.as_slice())
                && query
                    .as_ref()
                    .is_ok_and(|result| result.rows == vec![json!({"id":"42"})])
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!("capability reads did not converge: kv={kv:?}, document={row:?}, query={query:?}").into());
            }
            sleep(Duration::from_millis(25)).await;
        }
    }
    .await;
    let reader_closed = reader.close().await;
    let writer_closed = writer.close().await;
    observed?;
    reader_closed?;
    writer_closed
}

#[tokio::test]
async fn direct_table_mutations_validate_before_changing_data() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let objects = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let writer = Writer::open("capability-validation", objects, &MemoryConfig::default()).await?;
    let original = br#"{"stock":7}"#;
    WriteStorage::table_put(&writer, "products", "42", original, 0).await?;

    for (table, id, value, ttl) in [
        ("bad.table", "42", b"{}".as_slice(), 0),
        ("products", "", b"{}".as_slice(), 0),
        ("products", "42", b"[]".as_slice(), 0),
        ("products", "42", b"invalid".as_slice(), 0),
        ("products", "42", b"{}".as_slice(), u64::MAX),
    ] {
        let error = WriteStorage::table_put(&writer, table, id, value, ttl)
            .await
            .expect_err("invalid mutation accepted");
        assert_eq!(
            error
                .downcast_ref::<MutationError>()
                .map(MutationError::outcome),
            Some(MutationOutcome::Rejected)
        );
    }
    for (table, id) in [("bad.table", "42"), ("products", "")] {
        let error = WriteStorage::table_delete(&writer, table, id)
            .await
            .expect_err("invalid delete accepted");
        assert_eq!(
            error
                .downcast_ref::<MutationError>()
                .map(MutationError::outcome),
            Some(MutationOutcome::Rejected)
        );
    }
    assert_eq!(
        ReadStorage::table_get(&writer, "products", "42").await?,
        Some(original.to_vec())
    );
    WriteStorage::table_delete(&writer, "products", "42").await?;
    assert_eq!(
        ReadStorage::table_get(&writer, "products", "42").await?,
        None
    );
    writer.close().await
}
