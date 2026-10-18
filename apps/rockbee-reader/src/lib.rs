//! Reader role: opens SlateDB's read-only engine and follows durable writer updates.
//!
//! Readers may lag the writer; GET provides eventual consistency.

use rockbee_protocol::table::{QueryResult, TableRequest, table_key};
use rockbee_runtime::config::ReaderConfig;
use rockbee_runtime::memory::MemoryConfig;
use rockbee_runtime::ttl::visible_value;
use rockbee_runtime::{QueryStorage, ReadStorage, Result, Role, Storage};
use slatedb::DbReader;
use slatedb::object_store::ObjectStore;
use std::sync::Arc;

/// Runs the reader executable with its storage role fixed to reader.
///
/// # Errors
/// Returns configuration, storage-open, or server errors.
pub async fn run(args: impl IntoIterator<Item = String>) -> Result<()> {
    rockbee_runtime::run_for_role::<Reader>(Role::Reader, args).await
}

/// Read-only node combining SlateDB lookups with SQL over published Parquet files.
///
/// Implements [`ReadStorage`] and [`QueryStorage`]. Both views can lag writer
/// acknowledgements, and the SQL projection can lag direct document reads.
/// Mutations are not part of this type's API.
///
/// ```compile_fail,E0599
/// use rockbee_reader::Reader;
/// use rockbee_runtime::*;
///
/// async fn cannot_write(reader: &Reader) -> Result<()> {
///     reader.put(b"cart/42", b"changed").await
/// }
/// ```
///
/// ```compile_fail,E0599
/// use rockbee_reader::Reader;
/// use rockbee_runtime::*;
///
/// async fn cannot_write_with_ttl(reader: &Reader) -> Result<()> {
///     reader.put_with_ttl(b"cart/42", b"changed", 1000).await
/// }
/// ```
///
/// ```compile_fail,E0599
/// use rockbee_reader::Reader;
/// use rockbee_runtime::*;
///
/// async fn cannot_delete(reader: &Reader) -> Result<()> {
///     reader.delete(b"cart/42").await
/// }
/// ```
pub struct Reader {
    db: DbReader,
    query: rockbee_query::QueryReader,
}

impl Reader {
    /// Opens an independent SlateDB reader and configures bounded SQL execution.
    /// The writer must have initialized the prefix; reader freshness follows its
    /// manifest/WAL refresh settings rather than writer acknowledgements.
    pub async fn open(
        prefix: &str,
        objects: Arc<dyn ObjectStore>,
        memory: &MemoryConfig,
        config: &ReaderConfig,
    ) -> Result<Self> {
        config.validate()?;
        let limits = config.query();
        let query = rockbee_query::QueryReader::with_limits(
            prefix.into(),
            objects.clone(),
            limits.timeout(),
            limits.memory_limit_mb(),
            limits.max_concurrent(),
        )?;
        let cache = memory.db_cache()?;
        let db = DbReader::builder(prefix, objects.clone())
            .with_options(config.options(memory))
            .with_db_cache(cache, 0)
            .build()
            .await?;
        Ok(Self { db, query })
    }

    /// Reads an opaque raw KV key from this reader's current, eventual view.
    /// Returns the stored bytes, or `None` when absent or expired.
    /// See [`ReadStorage::get`] for the shared contract.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let key = rockbee_runtime::key::to_storage_key(key)?;
        visible_value(self.db.get_key_value(&key).await?)
    }

    /// Reads one document's JSON bytes from SlateDB by table name and ID.
    /// Returns `None` when absent or expired in this reader's current view.
    /// This uses the table namespace and does not wait for the SQL projection.
    /// See [`ReadStorage::table_get`] for validation and namespace semantics.
    pub async fn table_get(&self, table: &str, id: &str) -> Result<Option<Vec<u8>>> {
        visible_value(self.db.get_key_value(table_key(table, id)?).await?)
    }

    /// Runs SQL against the table's published Parquet projection via DuckDB.
    /// Returns JSON result rows and the exported WAL watermark; recent writes
    /// may not be published yet. See [`QueryStorage::query`] for errors.
    pub async fn query(&self, table: &str, sql: &str) -> Result<QueryResult> {
        TableRequest::Query {
            table: table.into(),
            sql: sql.into(),
        }
        .validate()?;
        let mut catalogue = vec![2];
        catalogue.extend_from_slice(table.as_bytes());
        let registered = self.db.get(catalogue).await?.is_some();
        self.query.execute(table, sql, registered).await
    }

    /// Closes this reader's SlateDB handle and its background refresh work.
    pub async fn close(&self) -> Result<()> {
        self.db.close().await?;
        Ok(())
    }
}

impl Storage for Reader {
    async fn open_config(config: &rockbee_runtime::config::Config) -> Result<Self> {
        if config.role != Role::Reader {
            return Err("reader requires reader configuration".into());
        }
        Self::open(
            &config.prefix,
            config.backend.connect()?,
            &config.memory,
            &config.reader,
        )
        .await
    }
    fn role(&self) -> Role {
        Role::Reader
    }
    async fn handle_request(
        &self,
        request: rockbee_protocol::Request,
    ) -> Result<rockbee_protocol::Response> {
        rockbee_runtime::server::handle_reader_request(self, request).await
    }
    async fn close(&self) -> Result<()> {
        self.close().await
    }
}

impl ReadStorage for Reader {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get(key).await
    }
    async fn table_get(&self, table: &str, id: &str) -> Result<Option<Vec<u8>>> {
        self.table_get(table, id).await
    }
}

impl QueryStorage for Reader {
    async fn query(&self, table: &str, sql: &str) -> Result<QueryResult> {
        self.query(table, sql).await
    }
}
