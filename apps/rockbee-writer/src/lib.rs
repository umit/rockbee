//! Writer role: opens SlateDB's writable engine and acknowledges durable mutations.

use rockbee_protocol::{MutationError, MutationOutcome};
use rockbee_runtime::memory::MemoryConfig;
use rockbee_runtime::ttl::{expiry_from, unix_millis, visible_value};
use rockbee_runtime::{ReadStorage, Result, Role, Storage, WriteStorage, rejected};
use slatedb::Db;
use slatedb::config::{PutOptions, Ttl, WriteOptions};
use slatedb::object_store::ObjectStore;
use std::sync::Arc;

/// Runs the writer executable with its storage role fixed to writer.
///
/// # Errors
/// Returns configuration, storage-open, or server errors.
pub async fn run(args: impl IntoIterator<Item = String>) -> Result<()> {
    rockbee_runtime::run_for_role::<Writer>(Role::Writer, args).await
}

/// The durable writer endpoint.
///
/// Implements [`ReadStorage`] and [`WriteStorage`] over the writable SlateDB
/// engine. Mutations acknowledge durability, while a background exporter
/// publishes JSON tables as Parquet. SQL queries run on reader compute.
///
/// ```compile_fail
/// use rockbee_runtime::QueryStorage;
/// use rockbee_writer::Writer;
///
/// fn requires_query<T: QueryStorage>() {}
/// requires_query::<Writer>();
/// ```
pub struct Writer {
    db: Arc<Db>,
    prefix: String,
    objects: Arc<dyn ObjectStore>,
    exporter: tokio::sync::Mutex<Option<rockbee_query::Exporter>>,
}

impl Writer {
    /// Opens the writable SlateDB engine and resumes table export when a catalogue exists.
    /// Only one active writer may own a storage prefix; SlateDB enforces writer fencing.
    pub async fn open(
        prefix: &str,
        objects: Arc<dyn ObjectStore>,
        memory: &MemoryConfig,
    ) -> Result<Self> {
        let cache = memory.db_cache()?;
        let db = Db::builder(prefix, objects.clone())
            .with_settings(memory.writer_settings()?)
            .with_db_cache(cache, 0)
            .build()
            .await?;
        let writer = Self {
            db: Arc::new(db),
            prefix: prefix.into(),
            objects,
            exporter: tokio::sync::Mutex::new(None),
        };
        // Resume projection after a restart even if no new table mutation arrives.
        let resume = async {
            let mut rows = writer.db.scan(vec![2]..vec![3]).await?;
            Ok::<_, slatedb::Error>(rows.next().await?.is_some())
        }
        .await;
        match resume {
            Ok(true) => {
                if let Err(error) = writer.ensure_exporter().await {
                    let _ = writer.db.close().await;
                    return Err(error);
                }
            }
            Ok(false) => {}
            Err(error) => {
                let _ = writer.db.close().await;
                return Err(error.into());
            }
        }
        Ok(writer)
    }

    /// Inserts or replaces raw KV bytes without expiry and waits for durability.
    /// Reader visibility can lag this acknowledgement. See [`WriteStorage::put`].
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let key = rockbee_runtime::key::to_storage_key(key).map_err(rejected)?;
        rockbee_protocol::validate_value_bytes(value).map_err(rejected)?;
        self.db
            .put_with_options(
                &key,
                value,
                &PutOptions { ttl: Ttl::NoExpiry },
                &WriteOptions::default(),
            )
            .await
            .map_err(unknown)?
            .await_durable()
            .await
            .map_err(unknown)?;
        Ok(())
    }

    /// Durably writes raw KV bytes with expiry based on the writer's wall clock.
    /// Zero means no expiry; invalid TTLs are rejected before mutation.
    pub async fn put_with_ttl(&self, key: &[u8], value: &[u8], ttl_millis: u64) -> Result<()> {
        let key = rockbee_runtime::key::to_storage_key(key).map_err(rejected)?;
        rockbee_protocol::validate_value_bytes(value).map_err(rejected)?;
        let expiry = expiry_from(unix_millis().map_err(rejected)?, ttl_millis).map_err(rejected)?;
        let ttl = match expiry {
            Some(expiry) => Ttl::ExpireAtMillis(expiry),
            None => Ttl::NoExpiry,
        };
        self.db
            .put_with_options(&key, value, &PutOptions { ttl }, &WriteOptions::default())
            .await
            .map_err(unknown)?
            .await_durable()
            .await
            .map_err(unknown)?;
        Ok(())
    }

    /// Waits for a durable raw KV deletion. Readers may temporarily retain their older view.
    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        let key = rockbee_runtime::key::to_storage_key(key).map_err(rejected)?;
        self.db
            .delete(&key)
            .await
            .map_err(unknown)?
            .await_durable()
            .await
            .map_err(unknown)?;
        Ok(())
    }

    /// Reads an opaque raw KV key from this writer's current view.
    /// Returns the stored bytes, or `None` when absent or expired.
    /// See [`ReadStorage::get`] for the shared contract.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let key = rockbee_runtime::key::to_storage_key(key)?;
        visible_value(self.db.get_key_value(&key).await?)
    }

    async fn ensure_exporter(&self) -> Result<()> {
        let mut exporter = self.exporter.lock().await;
        if exporter.is_none() {
            *exporter = Some(
                rockbee_query::Exporter::start(
                    self.db.clone(),
                    self.prefix.clone(),
                    self.objects.clone(),
                )
                .await?,
            );
        }
        Ok(())
    }

    /// Reads one document's JSON bytes from SlateDB by table name and ID.
    /// Returns `None` when absent or expired. Table and raw KV namespaces are
    /// separate; this lookup does not wait for Parquet publication.
    /// See [`ReadStorage::table_get`] for validation and namespace semantics.
    pub async fn table_get(&self, table: &str, id: &str) -> Result<Option<Vec<u8>>> {
        let key = rockbee_protocol::table::table_key(table, id)?;
        visible_value(self.db.get_key_value(key).await?)
    }

    /// Validates and durably replaces a JSON document together with its catalogue entry.
    /// A zero TTL means no expiry. The SQL projection is published asynchronously;
    /// exporter startup failure cannot revoke an already durable acknowledgement.
    pub async fn table_put(
        &self,
        table: &str,
        id: &str,
        value: &[u8],
        ttl_millis: u64,
    ) -> Result<()> {
        let key = rockbee_protocol::table::table_key(table, id).map_err(rejected)?;
        rockbee_protocol::table::validate_document(value).map_err(rejected)?;
        let expiry = expiry_from(unix_millis().map_err(rejected)?, ttl_millis).map_err(rejected)?;
        let ttl = expiry.map(Ttl::ExpireAtMillis).unwrap_or(Ttl::NoExpiry);
        let mut batch = slatedb::WriteBatch::new();
        batch.put_with_options(key, value, &PutOptions { ttl });
        let mut catalogue = vec![2];
        catalogue.extend_from_slice(table.as_bytes());
        batch.put_with_options(
            catalogue,
            table.as_bytes(),
            &PutOptions { ttl: Ttl::NoExpiry },
        );
        self.db
            .write(batch)
            .await
            .map_err(unknown)?
            .await_durable()
            .await
            .map_err(unknown)?;
        // Projection startup failure cannot revoke the durable document ACK.
        if let Err(error) = self.ensure_exporter().await {
            tracing::error!(error=%error, "table exporter startup failed");
        }
        Ok(())
    }

    /// Validates the table/ID and waits for a durable document deletion.
    /// Direct readers and the SQL projection converge independently afterward.
    pub async fn table_delete(&self, table: &str, id: &str) -> Result<()> {
        let key = rockbee_protocol::table::table_key(table, id).map_err(rejected)?;
        self.db
            .delete(key)
            .await
            .map_err(unknown)?
            .await_durable()
            .await
            .map_err(unknown)?;
        Ok(())
    }

    /// Stops the exporter, then closes SlateDB, preserving errors from either stage.
    /// Closing cancels active export; it does not wait for the latest SQL projection.
    pub async fn close(&self) -> Result<()> {
        let exported = match self.exporter.lock().await.as_mut() {
            Some(exporter) => exporter.close().await,
            None => Ok(()),
        };
        let closed = self
            .db
            .close()
            .await
            .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { Box::new(error) });
        match (exported, closed) {
            (Err(export), Err(close)) => Err(format!(
                "exporter close failed: {export}; storage close also failed: {close}"
            )
            .into()),
            (Err(error), _) | (_, Err(error)) => Err(error),
            _ => Ok(()),
        }
    }
}

// An engine error can follow a successful WAL write whose ACK was lost.
fn unknown(error: slatedb::Error) -> MutationError {
    MutationError::new(MutationOutcome::Unknown, error)
}

impl Storage for Writer {
    async fn open_config(config: &rockbee_runtime::config::Config) -> Result<Self> {
        if config.role != Role::Writer {
            return Err("writer requires writer configuration".into());
        }
        Self::open(&config.prefix, config.backend.connect()?, &config.memory).await
    }
    fn role(&self) -> Role {
        Role::Writer
    }
    async fn handle_request(
        &self,
        request: rockbee_protocol::Request,
    ) -> Result<rockbee_protocol::Response> {
        rockbee_runtime::server::handle_writer_request(self, request).await
    }
    async fn close(&self) -> Result<()> {
        self.close().await
    }
}

impl ReadStorage for Writer {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get(key).await
    }
    async fn table_get(&self, table: &str, id: &str) -> Result<Option<Vec<u8>>> {
        self.table_get(table, id).await
    }
}

impl WriteStorage for Writer {
    async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(key, value).await
    }
    async fn put_with_ttl(&self, key: &[u8], value: &[u8], ttl: u64) -> Result<()> {
        self.put_with_ttl(key, value, ttl).await
    }
    async fn delete(&self, key: &[u8]) -> Result<()> {
        self.delete(key).await
    }
    async fn table_put(&self, table: &str, id: &str, value: &[u8], ttl: u64) -> Result<()> {
        self.table_put(table, id, value, ttl).await
    }
    async fn table_delete(&self, table: &str, id: &str) -> Result<()> {
        self.table_delete(table, id).await
    }
}
