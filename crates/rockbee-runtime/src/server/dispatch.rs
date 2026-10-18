//! Translate wire requests into the capabilities supported by each role.

use crate::protocol::table::TableRequest;
use crate::protocol::{Request, Response};
use crate::{QueryStorage, ReadStorage, Result, WriteStorage, rejected};

/// Serve reads and SQL without requiring any write capability.
/// Wrong-role mutations are definitively rejected before reaching storage.
pub async fn handle_reader_request<S: ReadStorage + QueryStorage>(
    store: &S,
    request: Request,
) -> Result<Response> {
    match request {
        Request::Get { key } => Ok(Response::Value {
            value: store.get(&key).await?,
        }),
        Request::Table(TableRequest::Get { table, id }) => Ok(Response::Value {
            value: store.table_get(&table, &id).await?,
        }),
        Request::Table(TableRequest::Query { table, sql }) => Ok(Response::Query {
            result: store.query(&table, &sql).await?,
        }),
        Request::Put { .. }
        | Request::PutWithTtl { .. }
        | Request::Delete { .. }
        | Request::Table(TableRequest::Put { .. } | TableRequest::Delete { .. }) => {
            Err(rejected("read-only server: send put/delete to the writer").into())
        }
        Request::Status => Err("Status must be handled locally by RPC".into()),
    }
}

/// Serve KV and document operations without requiring SQL query execution.
pub async fn handle_writer_request<S: ReadStorage + WriteStorage>(
    store: &S,
    request: Request,
) -> Result<Response> {
    match request {
        Request::Put { key, value } => {
            store.put(&key, &value).await?;
            Ok(Response::Ok)
        }
        Request::PutWithTtl {
            key,
            value,
            ttl_millis,
        } => {
            store.put_with_ttl(&key, &value, ttl_millis).await?;
            Ok(Response::Ok)
        }
        Request::Get { key } => Ok(Response::Value {
            value: store.get(&key).await?,
        }),
        Request::Delete { key } => {
            store.delete(&key).await?;
            Ok(Response::Ok)
        }
        Request::Table(TableRequest::Put {
            table,
            id,
            value,
            ttl_millis,
        }) => {
            store.table_put(&table, &id, &value, ttl_millis).await?;
            Ok(Response::Ok)
        }
        Request::Table(TableRequest::Get { table, id }) => Ok(Response::Value {
            value: store.table_get(&table, &id).await?,
        }),
        Request::Table(TableRequest::Delete { table, id }) => {
            store.table_delete(&table, &id).await?;
            Ok(Response::Ok)
        }
        Request::Table(request @ TableRequest::Query { .. }) => {
            request.validate()?;
            Err("SQL queries must be sent to a reader".into())
        }
        Request::Status => Err("Status must be handled locally by RPC".into()),
    }
}
