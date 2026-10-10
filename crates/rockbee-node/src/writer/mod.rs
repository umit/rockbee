//! Writer role: opens SlateDB's writable engine and acknowledges durable mutations.

use crate::storage::memory::MemoryConfig;
use crate::storage::ttl::{expiry_from, unix_millis, visible_value};
use crate::{Result, storage::Role};
use rockbee_protocol::{MutationError, MutationOutcome};
use slatedb::Db;
use slatedb::config::{PutOptions, Ttl, WriteOptions};
use slatedb::object_store::ObjectStore;
use std::sync::Arc;

/// Runs the writer executable with its storage role fixed to writer.
///
/// # Errors
/// Returns configuration, storage-open, or server errors.
pub async fn run(args: impl IntoIterator<Item = String>) -> Result<()> {
    crate::runtime::run_for_role(Role::Writer, args).await
}

pub(crate) struct Writer {
    db: Db,
}

impl Writer {
    pub(crate) async fn open(
        prefix: &str,
        objects: Arc<dyn ObjectStore>,
        memory: &MemoryConfig,
    ) -> Result<Self> {
        let cache = memory.db_cache()?;
        let db = Db::builder(prefix, objects)
            .with_settings(memory.writer_settings()?)
            .with_db_cache(cache, 0)
            .build()
            .await?;
        Ok(Self { db })
    }

    pub(crate) async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let key = crate::key::to_storage_key(key).map_err(rejected)?;
        crate::protocol::validate_value_bytes(value).map_err(rejected)?;
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

    pub(crate) async fn put_with_ttl(
        &self,
        key: &[u8],
        value: &[u8],
        ttl_millis: u64,
    ) -> Result<()> {
        let key = crate::key::to_storage_key(key).map_err(rejected)?;
        crate::protocol::validate_value_bytes(value).map_err(rejected)?;
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

    pub(crate) async fn delete(&self, key: &[u8]) -> Result<()> {
        let key = crate::key::to_storage_key(key).map_err(rejected)?;
        self.db
            .delete(&key)
            .await
            .map_err(unknown)?
            .await_durable()
            .await
            .map_err(unknown)?;
        Ok(())
    }

    pub(crate) async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let key = crate::key::to_storage_key(key)?;
        visible_value(self.db.get_key_value(&key).await?)
    }

    pub(crate) async fn close(&self) -> Result<()> {
        self.db.close().await?;
        Ok(())
    }
}

pub(crate) fn rejected(
    error: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> MutationError {
    MutationError::new(MutationOutcome::Rejected, error)
}

// An engine error can follow a successful WAL write whose ACK was lost.
fn unknown(error: slatedb::Error) -> MutationError {
    MutationError::new(MutationOutcome::Unknown, error)
}
