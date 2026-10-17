use rockbee_protocol::{
    Response,
    table::{TableRequest, table_key},
};
use rockbee_test_support::{Result, Store, storage::Role};
use slatedb::object_store::{ObjectStore, local::LocalFileSystem};
use std::sync::Arc;

fn put(table: &str, id: &str, value: &[u8], ttl_millis: u64) -> TableRequest {
    TableRequest::Put {
        table: table.into(),
        id: id.into(),
        value: value.to_vec(),
        ttl_millis,
    }
}

#[tokio::test]
async fn tables_keep_flexible_json_and_isolate_binary_raw_keys_across_restart() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let objects: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let writer = Store::open(Role::Writer, "tables-restart", objects.clone()).await?;
    let raw_key = table_key("products", "42")?;
    writer.put(&raw_key, b"raw binary value").await?;
    writer
        .table(&put(
            "products",
            "42",
            br#"{"name":"Keyboard","price":2500}"#,
            0,
        ))
        .await?;
    writer
        .table(&put("users", "42", br#"{"email":"a@example.com"}"#, 0))
        .await?;
    writer
        .table(&put("products", "43", br#"{"author":"Alice"}"#, 0))
        .await?;
    assert_eq!(
        writer.get(&raw_key).await?.as_deref(),
        Some(b"raw binary value".as_slice())
    );
    writer.close().await?;
    let writer = Store::open(Role::Writer, "tables-restart", objects).await?;
    assert_eq!(
        writer
            .table(&TableRequest::Get {
                table: "products".into(),
                id: "42".into()
            })
            .await?,
        Response::Value {
            value: Some(br#"{"name":"Keyboard","price":2500}"#.to_vec())
        }
    );
    writer
        .table(&put("products", "42", br#"{"stock":3}"#, 0))
        .await?;
    assert_eq!(
        writer
            .table(&TableRequest::Get {
                table: "products".into(),
                id: "42".into()
            })
            .await?,
        Response::Value {
            value: Some(br#"{"stock":3}"#.to_vec())
        }
    );
    writer
        .table(&TableRequest::Delete {
            table: "products".into(),
            id: "42".into(),
        })
        .await?;
    assert_eq!(
        writer
            .table(&TableRequest::Get {
                table: "products".into(),
                id: "42".into()
            })
            .await?,
        Response::Value { value: None }
    );
    assert!(matches!(
        writer
            .table(&TableRequest::Get {
                table: "users".into(),
                id: "42".into()
            })
            .await?,
        Response::Value { value: Some(_) }
    ));
    writer.close().await
}

#[tokio::test]
async fn table_validation_and_ttl_are_enforced_by_storage_itself() -> Result<()> {
    let objects: Arc<dyn ObjectStore> = Arc::new(slatedb::object_store::memory::InMemory::new());
    let writer = Store::open(Role::Writer, "tables-ttl", objects.clone()).await?;
    for request in [
        put("bad.table", "42", b"{}", 0),
        put("products", "", b"{}", 0),
        put("products", "42", b"[]", 0),
        put("products", "42", b"invalid", 0),
    ] {
        let error = writer
            .table(&request)
            .await
            .err()
            .ok_or("invalid table PUT accepted")?;
        assert_eq!(
            error
                .downcast_ref::<rockbee_protocol::MutationError>()
                .map(rockbee_protocol::MutationError::outcome),
            Some(rockbee_protocol::MutationOutcome::Rejected)
        );
    }
    writer.table(&put("products", "ttl", b"{}", 1)).await?;
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    assert_eq!(
        writer
            .table(&TableRequest::Get {
                table: "products".into(),
                id: "ttl".into()
            })
            .await?,
        Response::Value { value: None }
    );
    let reader = Store::open(Role::Reader, "tables-ttl", objects).await?;
    assert!(
        reader
            .table(&put("products", "42", b"{}", 0))
            .await
            .is_err()
    );
    reader.close().await?;
    writer.close().await
}
