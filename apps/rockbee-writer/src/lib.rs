//! Writer role: opens SlateDB's writable engine and acknowledges durable mutations.

use rockbee_protocol::{MutationError, MutationOutcome};
use rockbee_runtime::memory::MemoryConfig;
use rockbee_runtime::ttl::{expiry_from, unix_millis, visible_value};
use rockbee_runtime::{Result, Role, Storage, rejected};
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

pub struct Writer {
    db: Arc<Db>,
    prefix: String,
    objects: Arc<dyn ObjectStore>,
    exporter: tokio::sync::Mutex<Option<rockbee_query::Exporter>>,
}

impl Writer {
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

    pub async fn table(
        &self,
        request: &rockbee_protocol::table::TableRequest,
    ) -> Result<rockbee_protocol::Response> {
        use rockbee_protocol::{
            Response,
            table::{TableRequest, table_key},
        };
        if request.is_mutation() {
            request.validate().map_err(rejected)?;
        } else {
            request.validate()?;
        }
        match request {
            TableRequest::Put {
                table,
                id,
                value,
                ttl_millis,
            } => {
                let expiry =
                    expiry_from(unix_millis().map_err(rejected)?, *ttl_millis).map_err(rejected)?;
                let ttl = expiry.map(Ttl::ExpireAtMillis).unwrap_or(Ttl::NoExpiry);
                let mut batch = slatedb::WriteBatch::new();
                batch.put_with_options(table_key(table, id)?, value, &PutOptions { ttl });
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
                Ok(Response::Ok)
            }
            TableRequest::Get { table, id } => Ok(Response::Value {
                value: visible_value(self.db.get_key_value(table_key(table, id)?).await?)?,
            }),
            TableRequest::Delete { table, id } => {
                self.db
                    .delete(table_key(table, id)?)
                    .await
                    .map_err(unknown)?
                    .await_durable()
                    .await
                    .map_err(unknown)?;
                Ok(Response::Ok)
            }
            TableRequest::Query { .. } => Err("SQL queries must be sent to a reader".into()),
        }
    }

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
    async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(key, value).await
    }
    async fn put_with_ttl(&self, key: &[u8], value: &[u8], ttl: u64) -> Result<()> {
        self.put_with_ttl(key, value, ttl).await
    }
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get(key).await
    }
    async fn delete(&self, key: &[u8]) -> Result<()> {
        self.delete(key).await
    }
    async fn table(
        &self,
        request: &rockbee_protocol::table::TableRequest,
    ) -> Result<rockbee_protocol::Response> {
        self.table(request).await
    }
    async fn close(&self) -> Result<()> {
        self.close().await
    }
}
