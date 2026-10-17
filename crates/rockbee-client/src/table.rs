//! JSON table operations over the client's shared bounded connection.
use crate::{
    MutationError, MutationOutcome, MutationResult, PutHandle, QueryResult, Result, RockBeeClient,
    duration_to_millis, mutation_response, rejected_put,
};
use rockbee_protocol::{Request, Response, table::TableRequest};
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

/// A named JSON table sharing its client's connection, limits, and shutdown gate.
///
/// Names and inputs are validated locally when an operation starts. Documents
/// must be JSON objects. GET and SQL queries may lag durable writer acknowledgements.
#[derive(Clone)]
pub struct Table {
    client: RockBeeClient,
    name: String,
}

/// Eager document PUT with its generated UUID available before completion.
///
/// Dropping this handle does not cancel the mutation. Awaiting success returns
/// the ID after the writer's durable acknowledgement. Errors preserve the same
/// mutation outcomes as [`PutHandle`]; uncertain writes are never replayed.
#[must_use = "await this handle to observe rejection or a durable acknowledgement"]
pub struct TablePutHandle {
    id: String,
    put: PutHandle,
}

impl RockBeeClient {
    /// Select a JSON table. Its name is validated when an operation starts.
    pub fn table(&self, name: &str) -> Table {
        Table {
            client: self.clone(),
            name: name.to_owned(),
        }
    }
}

impl Table {
    /// Generate a UUID and immediately start storing a JSON object without expiry.
    pub fn put(&self, document: &Value) -> TablePutHandle {
        self.put_generated(document, None)
    }

    /// Generate a UUID and immediately start storing a JSON object with a TTL.
    ///
    /// Zero means no expiry; positive fractions of a millisecond round up.
    pub fn put_with_ttl(&self, document: &Value, ttl: Duration) -> TablePutHandle {
        self.put_generated(document, Some(ttl))
    }

    fn put_generated(&self, document: &Value, ttl: Option<Duration>) -> TablePutHandle {
        let id = uuid::Uuid::new_v4().to_string();
        let put = self.put_key(&id, document, ttl);
        TablePutHandle { id, put }
    }

    /// Immediately store or replace a JSON object under a caller-supplied ID.
    ///
    /// Validation and serialization finish before admission or spawning. Dropping
    /// the returned handle does not cancel the write. `None` or zero TTL means
    /// no expiry; positive fractions of a millisecond round up.
    pub fn put_key(&self, id: &str, document: &Value, ttl: Option<Duration>) -> PutHandle {
        let request = (|| -> Result<TableRequest> {
            if !document.is_object() {
                return Err("table value must be a JSON object".into());
            }
            let request = TableRequest::Put {
                table: self.name.clone(),
                id: id.to_owned(),
                value: serde_json::to_vec(document)?,
                ttl_millis: ttl.map(duration_to_millis).transpose()?.unwrap_or(0),
            };
            request.validate()?;
            Ok(request)
        })();
        match request {
            Ok(request) => self.client.submit_put_request(Request::Table(request)),
            Err(error) => rejected_put(error),
        }
    }

    /// Read a JSON object, or `None` when the reader has no visible value.
    ///
    /// Readers converge eventually; a preceding durable PUT does not guarantee
    /// immediate visibility here.
    pub async fn get(&self, id: &str) -> Result<Option<Value>> {
        let request = TableRequest::Get {
            table: self.name.clone(),
            id: id.to_owned(),
        };
        request.validate()?;
        let _operation = self.client.lifecycle.start()?;
        match self.client.exchange(Request::Table(request)).await? {
            Response::Value { value } => value
                .map(|bytes| {
                    let value: Value = serde_json::from_slice(&bytes)?;
                    if !value.is_object() {
                        return Err("table value must be a JSON object".into());
                    }
                    Ok(value)
                })
                .transpose(),
            Response::Error { message, .. } => Err(message.into()),
            _ => Err("unexpected response to table get".into()),
        }
    }

    /// Delete an ID and wait for durable acknowledgement, without automatic retry.
    pub async fn delete(&self, id: &str) -> MutationResult {
        let request = TableRequest::Delete {
            table: self.name.clone(),
            id: id.to_owned(),
        };
        request
            .validate()
            .map_err(|error| MutationError::new(MutationOutcome::NotSent, error))?;
        let _operation = self
            .client
            .lifecycle
            .start()
            .map_err(|error| MutationError::new(MutationOutcome::NotSent, error))?;
        let response = self
            .client
            .exchange(Request::Table(request))
            .await
            .map_err(MutationError::from_error)?;
        mutation_response(response)
    }

    /// Run SQL against this table's eventually published Parquet data.
    ///
    /// SQL is sent verbatim to the reader. Returned rows use the selected column
    /// names; `exported_wal_file_id` identifies the published export boundary.
    pub async fn query(&self, sql: &str) -> Result<QueryResult> {
        let request = TableRequest::Query {
            table: self.name.clone(),
            sql: sql.to_owned(),
        };
        request.validate()?;
        let _operation = self.client.lifecycle.start()?;
        match self.client.exchange(Request::Table(request)).await? {
            Response::Query { result } => Ok(result),
            Response::Error { message, .. } => Err(message.into()),
            _ => Err("unexpected response to table query".into()),
        }
    }
}

impl TablePutHandle {
    /// Generated UUID, available immediately even before the handle is awaited.
    pub fn id(&self) -> &str {
        &self.id
    }
}

impl Future for TablePutHandle {
    type Output = MutationResult<String>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.put)
            .poll(cx)
            .map(|result| result.map(|()| this.id.clone()))
    }
}
