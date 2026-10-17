//! Regression coverage for the actual gateway, role servers, exporter and SQL reader.
use rockbee_client::{QueryResult, Result, RockBeeClient, Table};
use rockbee_gateway::Gateway;
use rockbee_test_support::{Server, Store, storage::Role};
use serde_json::{Value, json};
use slatedb::object_store::{ObjectStore, local::LocalFileSystem, path::Path};
use slatedb::wal::{SlateDbWalReaderBuilder, WalReader};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, timeout};

struct Running {
    stop: oneshot::Sender<()>,
    task: JoinHandle<Result<()>>,
}

impl Running {
    async fn stop(self) -> Result<()> {
        let _ = self.stop.send(());
        let mut task = self.task;
        match timeout(Duration::from_secs(10), &mut task).await {
            Ok(result) => result?,
            Err(error) => {
                task.abort();
                let _ = task.await;
                Err(error.into())
            }
        }
    }
}

fn run_role(server: Server) -> Running {
    let (stop, stopped) = oneshot::channel();
    let task = tokio::spawn(server.run_until(async {
        stopped.await?;
        Ok(())
    }));
    Running { stop, task }
}

async fn tail(prefix: &str, objects: Arc<dyn ObjectStore>) -> Result<u64> {
    Ok(SlateDbWalReaderBuilder::new()
        .with_path(Path::from(prefix))
        .with_object_store(objects)
        .build()?
        .last_wal_file_id(0)
        .await?)
}

// Poll reads only. A durable PUT ACK is deliberately not treated as immediate
// reader or Parquet visibility. The independently read WAL tail is the boundary
// the exported result must reach before assertions about newer writes are made.
async fn wait_rows(
    table: &Table,
    sql: &str,
    min_wal: u64,
    rows: Vec<Value>,
) -> Result<QueryResult> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let result = table.query(sql).await;
        if let Ok(result) = &result
            && result.exported_wal_file_id >= min_wal
            && result.rows == rows
        {
            return Ok(result.clone());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "SQL {sql:?} did not converge through WAL {min_wal}; expected {rows:?}, last result: {result:?}"
            )
            .into());
        }
        sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_document(table: &Table, id: &str, expected: Option<&Value>) -> Result<()> {
    timeout(Duration::from_secs(30), async {
        loop {
            if table.get(id).await?.as_ref() == expected {
                return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(());
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await?
}

// Catches wrong gateway routing, lost JSON/table IDs, missing export publication,
// filtering before newest-version selection, absent tombstones, and TTL resurrecting
// a predecessor. It uses no external service and owns every server it stops.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sdk_table_mutations_converge_through_parquet_sql_without_resurrection() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let prefix = format!(
        "tables-e2e-{}",
        directory
            .path()
            .file_name()
            .ok_or("temporary directory has no name")?
            .to_string_lossy()
    );
    let objects: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(directory.path())?);
    let writer_store = Store::open(Role::Writer, &prefix, Arc::clone(&objects)).await?;
    let reader_store = Store::open(Role::Reader, &prefix, Arc::clone(&objects)).await?;
    let writer = Server::bind("127.0.0.1:0", writer_store).await?;
    let reader = Server::bind("127.0.0.1:0", reader_store).await?;
    let gateway = Gateway::bind(
        "127.0.0.1:0",
        writer.local_addr()?.to_string(),
        vec![reader.local_addr()?.to_string()],
    )
    .await?;
    let address = gateway.local_addr()?.to_string();
    let writer = run_role(writer);
    let reader = run_role(reader);
    let (stop, stopped) = oneshot::channel();
    let gateway = Running {
        stop,
        task: tokio::spawn(gateway.run_until(async {
            stopped.await?;
            Ok(())
        })),
    };

    let body: Result<()> = async {
        let client = RockBeeClient::connect(&address).await?;
        let result = exercise_tables(&client, &prefix, Arc::clone(&objects)).await;
        client.shutdown().await;
        result
    }
    .await;
    // Finish every owned server even when the body returns a test error.
    let gateway_closed = gateway.stop().await;
    let reader_closed = reader.stop().await;
    let writer_closed = writer.stop().await;
    body?;
    gateway_closed?;
    reader_closed?;
    writer_closed
}

async fn exercise_tables(
    client: &RockBeeClient,
    prefix: &str,
    objects: Arc<dyn ObjectStore>,
) -> Result<()> {
    let products = client.table("products");
    let first =
        json!({"name":"Keyboard", "price":120, "nested":{"active":true, "tags":["usb","desk"]}});
    let pending = products.put(&first);
    let id = pending.id().to_owned();
    assert!(!id.is_empty());
    // Keep the handle unpolled while reads converge: submission must be eager.
    wait_document(&products, &id, Some(&first)).await?;
    assert_eq!(pending.await?, id);
    products
        .put_key(
            "other",
            &json!({"author":"Alice", "nested":{"active":false}}),
            None,
        )
        .await?;
    let sql = "SELECT id, value->>'name' AS name, value->'nested'->>'active' AS active FROM products ORDER BY id";
    let mut expected = vec![
        json!({"id":id,"name":"Keyboard","active":"true"}),
        json!({"id":"other","name":null,"active":"false"}),
    ];
    expected.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
    let initial = wait_rows(
        &products,
        sql,
        tail(prefix, Arc::clone(&objects)).await?,
        expected,
    )
    .await?;
    assert!(initial.exported_wal_file_id > 0);
    wait_rows(
        &products,
        "SELECT count(*) AS n FROM products",
        initial.exported_wal_file_id,
        vec![json!({"n":2})],
    )
    .await?;

    let matching = "SELECT id FROM products WHERE TRY_CAST(value->>'price' AS DOUBLE) > 100";
    wait_rows(
        &products,
        matching,
        initial.exported_wal_file_id,
        vec![json!({"id":id})],
    )
    .await?;
    let replacement = json!({"name":"Keyboard", "price":20, "nested":{"active":true}});
    products.put_key(&id, &replacement, None).await?;
    let updated_tail = tail(prefix, Arc::clone(&objects)).await?;
    wait_rows(&products, matching, updated_tail, vec![]).await?;
    wait_document(&products, &id, Some(&replacement)).await?;
    // Repeated warm-cache queries at/after the boundary must never resurrect the
    // older row that matched the SQL predicate before this replacement.
    for _ in 0..3 {
        let result = products.query(matching).await?;
        assert!(result.exported_wal_file_id >= updated_tail);
        assert!(result.rows.is_empty());
    }

    // Identically spelled raw bytes and differently cased tables remain separate.
    let raw_key = rockbee_test_support::protocol::table::table_key("products", &id)?;
    client.put(&raw_key, b"raw-value", None).await?;
    let upper = client.table("Products");
    upper.put_key(&id, &json!({"name":"Upper"}), None).await?;
    let isolated_tail = tail(prefix, Arc::clone(&objects)).await?;
    wait_rows(
        &upper,
        "SELECT value->>'name' AS name FROM \"Products\"",
        isolated_tail,
        vec![json!({"name":"Upper"})],
    )
    .await?;
    wait_rows(
        &products,
        "SELECT count(*) AS n FROM products",
        isolated_tail,
        vec![json!({"n":2})],
    )
    .await?;
    timeout(Duration::from_secs(30), async {
        loop {
            if client.get(&raw_key).await?.as_deref() == Some(b"raw-value".as_slice()) {
                return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(());
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await??;

    products.delete(&id).await?;
    let deleted_tail = tail(prefix, Arc::clone(&objects)).await?;
    wait_rows(
        &products,
        "SELECT id FROM products ORDER BY id",
        deleted_tail,
        vec![json!({"id":"other"})],
    )
    .await?;
    wait_document(&products, &id, None).await?;
    wait_document(&upper, &id, Some(&json!({"name":"Upper"}))).await?;
    assert_eq!(
        client.get(&raw_key).await?.as_deref(),
        Some(b"raw-value".as_slice())
    );

    // A newer expiring replacement must hide its older nonexpiring predecessor.
    products
        .put_key("expiring", &json!({"version":"old"}), None)
        .await?;
    wait_rows(
        &products,
        "SELECT value->>'version' AS version FROM products WHERE id='expiring'",
        tail(prefix, Arc::clone(&objects)).await?,
        vec![json!({"version":"old"})],
    )
    .await?;
    let started = Instant::now();
    products
        .put_key(
            "expiring",
            &json!({"version":"new"}),
            Some(Duration::from_secs(15)),
        )
        .await?;
    let ttl_tail = tail(prefix, Arc::clone(&objects)).await?;
    let ttl_sql = "SELECT value->>'version' AS version FROM products WHERE id='expiring'";
    wait_rows(&products, ttl_sql, ttl_tail, vec![json!({"version":"new"})]).await?;
    wait_document(&products, "expiring", Some(&json!({"version":"new"}))).await?;
    tokio::time::sleep_until(started + Duration::from_secs(16)).await;
    wait_rows(&products, ttl_sql, ttl_tail, vec![]).await?;
    wait_document(&products, "expiring", None).await?;
    for _ in 0..3 {
        assert!(products.query(ttl_sql).await?.rows.is_empty());
    }
    Ok(())
}
