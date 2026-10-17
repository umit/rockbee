//! Run against an existing cluster with `GATEWAY_ADDR=127.0.0.1:7100 cargo run -p rockbee-client --example tables`.
use rockbee_client::{Result, RockBeeClient};
use serde_json::json;
use std::time::Duration;
use tokio::time::{Instant, sleep};

#[tokio::main]
async fn main() -> Result<()> {
    let address = std::env::var("GATEWAY_ADDR").unwrap_or_else(|_| "127.0.0.1:7100".to_owned());
    let client = RockBeeClient::connect(&address).await?;
    let result = demonstrate(&client).await;
    client.shutdown().await;
    result
}

async fn demonstrate(client: &RockBeeClient) -> Result<()> {
    let products = client.table("products");
    let put = products.put(&json!({"name":"Keyboard", "price":99.5}));
    println!("submitted product {}", put.id());
    let id = put.await?;

    // Caller-supplied IDs allow replacements; this example uses its own UUID.
    let supplied_id = format!("example-{}", uuid::Uuid::new_v4());
    products
        .put_key(
            &supplied_id,
            &json!({"name":"Mouse", "price":25.0}),
            Some(Duration::from_secs(300)),
        )
        .await?;

    // SQL sees published Parquet exports. Poll reads only: never replay a PUT
    // whose outcome is unknown. UUIDs generated above contain no SQL quotes.
    let sql = format!(
        "SELECT id, value->>'name' AS name, TRY_CAST(value->>'price' AS DOUBLE) AS price \
         FROM products WHERE id IN ('{id}', '{supplied_id}') ORDER BY id"
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match products.query(&sql).await {
            Ok(result) if result.rows.len() == 2 => {
                println!(
                    "published through WAL {}: {}",
                    result.exported_wal_file_id,
                    serde_json::to_string_pretty(&result.rows)?
                );
                break;
            }
            Ok(_) if Instant::now() < deadline => {}
            Err(error) if Instant::now() < deadline => {
                eprintln!("waiting for published query data: {error}");
            }
            Ok(_) => return Err("products did not become query-visible within 30 seconds".into()),
            Err(error) => return Err(error),
        }
        sleep(Duration::from_millis(200)).await;
    }
    Ok(())
}
