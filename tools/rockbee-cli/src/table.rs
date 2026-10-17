//! JSON table commands share the SDK's transport, validation and mutation outcomes.
use rockbee_client::{Result, RockBeeClient};
use rockbee_protocol::table::TableRequest;
use rockbee_protocol::{Request, WireMessage};
use serde_json::{Value, json};
use std::time::Duration;

pub(crate) async fn run(address: &str, args: &[String]) -> Result<Value> {
    let (request, generated) = parse(args)?;
    // Validate before connecting, including JSON objects and the full wire frame.
    Request::Table(request.clone()).validate()?;
    let client = RockBeeClient::connect(address).await?;
    let result = async {
        match request {
            TableRequest::Put {
                table,
                id,
                value,
                ttl_millis,
            } => {
                let table = client.table(&table);
                let document = serde_json::from_slice(&value)?;
                let ttl = Duration::from_millis(ttl_millis);
                let id = if generated {
                    if ttl_millis == 0 {
                        table.put(&document).await?
                    } else {
                        table.put_with_ttl(&document, ttl).await?
                    }
                } else {
                    table.put_key(&id, &document, Some(ttl)).await?;
                    id
                };
                Ok(json!({"id": id}))
            }
            TableRequest::Get { table, id } => {
                Ok(client.table(&table).get(&id).await?.unwrap_or(Value::Null))
            }
            TableRequest::Delete { table, id } => {
                client.table(&table).delete(&id).await?;
                Ok(json!({"status": "ok"}))
            }
            TableRequest::Query { table, sql } => Ok(serde_json::to_value(
                client.table(&table).query(&sql).await?,
            )?),
        }
    }
    .await;
    client.shutdown().await;
    result
}

fn parse(args: &[String]) -> Result<(TableRequest, bool)> {
    let request = match args {
        [op, table, document, options @ ..] if op == "table-put" => {
            let mut id = None;
            let mut ttl_millis = None;
            let mut options = options.iter();
            while let Some(flag) = options.next() {
                let value = options.next().ok_or(super::USAGE)?;
                match flag.as_str() {
                    "--id" if id.is_none() => id = Some(value.clone()),
                    "--ttl-ms" if ttl_millis.is_none() => ttl_millis = Some(value.parse()?),
                    _ => return Err(super::USAGE.into()),
                }
            }
            // Match the generated UUID length when validating frame size before connect.
            let generated = id.is_none();
            let request = TableRequest::Put {
                table: table.clone(),
                id: id.unwrap_or_else(|| "00000000-0000-0000-0000-000000000000".into()),
                value: serde_json::to_vec(&serde_json::from_str::<Value>(document)?)?,
                ttl_millis: ttl_millis.unwrap_or(0),
            };
            return Ok((request, generated));
        }
        [op, table, id] if op == "table-get" => TableRequest::Get {
            table: table.clone(),
            id: id.clone(),
        },
        [op, table, id] if op == "table-delete" => TableRequest::Delete {
            table: table.clone(),
            id: id.clone(),
        },
        [op, table, sql] if op == "table-query" => TableRequest::Query {
            table: table.clone(),
            sql: sql.clone(),
        },
        _ => return Err(super::USAGE.into()),
    };
    Ok((request, false))
}
