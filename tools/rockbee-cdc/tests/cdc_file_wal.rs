use rockbee_cdc::event_from_row;
use slatedb::Db;
use slatedb::object_store::{ObjectStoreExt, local::LocalFileSystem};
use slatedb::wal::{SlateDbWalReaderBuilder, WalReader};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn reads_file_backed_put_and_delete_from_the_wal() {
    let temp = tempfile::tempdir().unwrap();
    let objects = Arc::new(LocalFileSystem::new_with_prefix(temp.path()).unwrap());
    let db = Db::open("cdc-file", objects.clone()).await.unwrap();
    db.put(&[0, b'k', 0xff], &[0x80, 0xff, 0x00])
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    db.delete(&[0, b'k', 0xff])
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    db.close().await.unwrap();

    let reader = SlateDbWalReaderBuilder::new()
        .with_path("cdc-file".into())
        .with_object_store(objects)
        .build()
        .unwrap();
    let mut iterator = reader.iterator((1..).into()).await.unwrap();
    let mut events = Vec::new();
    while events.len() < 2 {
        let rows = tokio::time::timeout(Duration::from_secs(5), iterator.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        for row in rows.rows {
            if let Some(event) = event_from_row(row, rows.last_consumed_wal_file_id).unwrap() {
                events.push(event);
            }
        }
    }
    assert_eq!(events[0].op, "put");
    assert_eq!(events[0].key, vec![b'k', 0xff]);
    assert_eq!(events[0].value.as_deref(), Some(&[0x80, 0xff, 0x00][..]));
    assert_eq!(events[1].op, "delete");
    assert_eq!(events[1].key, vec![b'k', 0xff]);
    assert!(events[0].seq < events[1].seq);
}

#[tokio::test]
async fn missing_retained_wal_file_is_an_error_instead_of_skipping_events() {
    let temp = tempfile::tempdir().unwrap();
    let objects = Arc::new(LocalFileSystem::new_with_prefix(temp.path()).unwrap());
    let db = Db::open("cdc-gap", objects.clone()).await.unwrap();
    db.put(&[0, b'k'], b"first")
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    db.delete(&[0, b'k'])
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    db.close().await.unwrap();
    objects
        .delete(&"cdc-gap/wal/00000000000000000002.sst".into())
        .await
        .unwrap();
    let reader = SlateDbWalReaderBuilder::new()
        .with_path("cdc-gap".into())
        .with_object_store(objects)
        .build()
        .unwrap();
    let mut iterator = reader.iterator((2..).into()).await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), iterator.next())
        .await
        .unwrap();
    assert!(matches!(
        result,
        Err(slatedb::wal::WalError::WalTruncated(2))
    ));
}
