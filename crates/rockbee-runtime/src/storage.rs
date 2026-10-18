//! Storage capabilities are separate from the server lifecycle and wire protocol.

use crate::config::Config;
use crate::protocol::table::QueryResult;
use crate::protocol::{Request, Response};
use crate::{Result, Role};
use std::future::Future;

/// Reading capability shared by Writer and Reader.
///
/// `Send` allows an implementation to move between threads; `Sync` allows shared
/// references to it across threads. The server shares one storage instance across
/// concurrent RPC handlers, so both guarantees are required. Returned futures
/// are also `Send`, allowing Tokio to schedule them across worker threads.
///
/// Reads use the role's current view. A Reader can lag a durable Writer ACK.
pub trait ReadStorage: Send + Sync {
    /// Looks up an opaque byte key in the raw key-value namespace.
    ///
    /// Returns the stored bytes, or `None` if the key is absent or expired in
    /// this role's current view. Values are returned without JSON decoding.
    ///
    /// # Errors
    /// Returns an error for an invalid key or a storage read failure.
    fn get(&self, key: &[u8]) -> impl Future<Output = Result<Option<Vec<u8>>>> + Send;

    /// Looks up one JSON document by table name and document ID.
    ///
    /// Returns the document's JSON bytes, or `None` if it is absent or expired
    /// in this role's current view. Table keys use a separate namespace, so the
    /// same ID in another table or in raw KV does not refer to this document.
    /// This is a direct SlateDB lookup; it does not wait for Parquet publication.
    ///
    /// # Errors
    /// Returns an error for an invalid table name or ID, or a storage read failure.
    fn table_get(
        &self,
        table: &str,
        id: &str,
    ) -> impl Future<Output = Result<Option<Vec<u8>>>> + Send;
}

/// Writing capability implemented by Writer.
///
/// Success acknowledges durable storage. Reader and SQL visibility can lag the
/// acknowledgement. Validation failures are rejected; uncertain mutations must
/// not be replayed automatically. `Send + Sync` supports concurrent RPC handlers,
/// as described in [`ReadStorage`].
pub trait WriteStorage: Send + Sync {
    /// Inserts or replaces raw bytes and removes any previous expiry.
    /// Returns only after the write is durable.
    fn put(&self, key: &[u8], value: &[u8]) -> impl Future<Output = Result<()>> + Send;

    /// Inserts or replaces raw bytes with a TTL measured from the writer's clock.
    /// A zero TTL means no expiry. Returns only after the write is durable.
    fn put_with_ttl(
        &self,
        key: &[u8],
        value: &[u8],
        ttl_millis: u64,
    ) -> impl Future<Output = Result<()>> + Send;
    /// Durably deletes a raw KV key, including when the key is already absent.
    fn delete(&self, key: &[u8]) -> impl Future<Output = Result<()>> + Send;

    /// Inserts or replaces a JSON object under an explicit table and document ID.
    /// The document and table catalogue entry become durable in the same batch.
    /// A zero TTL means no expiry; Parquet publication happens asynchronously.
    fn table_put(
        &self,
        table: &str,
        id: &str,
        value: &[u8],
        ttl_millis: u64,
    ) -> impl Future<Output = Result<()>> + Send;
    /// Durably deletes a table document. The SQL projection catches up asynchronously.
    fn table_delete(&self, table: &str, id: &str) -> impl Future<Output = Result<()>> + Send;
}

/// SQL query capability implemented by Reader.
///
/// Queries read the published Parquet projection, which may lag document writes
/// visible through [`ReadStorage::table_get`]. `Send + Sync` supports concurrent
/// RPC handlers, as described in [`ReadStorage`].
pub trait QueryStorage: Send + Sync {
    /// Executes a SELECT query for a table and returns rows as JSON objects,
    /// together with the projection's exported WAL watermark.
    ///
    /// # Errors
    /// Returns an error for invalid SQL, an unpublished initial projection,
    /// query resource limits, or failures reading or executing the projection.
    fn query(&self, table: &str, sql: &str) -> impl Future<Output = Result<QueryResult>> + Send;
}

/// Server lifecycle and wire-request dispatch shared by both roles.
///
/// Concrete roles delegate requests to a handler bounded by their supported
/// capability traits. Implementing this interface does not add read, write, or
/// SQL methods; those are provided separately by [`ReadStorage`], [`WriteStorage`],
/// and [`QueryStorage`].
pub trait Storage: Send + Sync + 'static {
    /// Opens the concrete role using validated configuration and its object store.
    fn open_config(config: &Config) -> impl Future<Output = Result<Self>> + Send
    where
        Self: Sized;
    /// Reports the role used by RPC status and request logging.
    fn role(&self) -> Role;
    /// Dispatches a wire request, rejecting operations unsupported by this role.
    fn handle_request(&self, request: Request) -> impl Future<Output = Result<Response>> + Send;
    /// Releases storage resources after the server has drained its RPC work.
    fn close(&self) -> impl Future<Output = Result<()>> + Send;
}
