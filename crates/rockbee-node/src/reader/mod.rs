//! Reader role: opens SlateDB's read-only engine and follows durable writer updates.
//!
//! Readers may lag the writer; GET provides eventual consistency.

use crate::config::ReaderConfig;
use crate::storage::memory::MemoryConfig;
use crate::storage::ttl::visible_value;
use crate::{Result, storage::Role};
use slatedb::DbReader;
use slatedb::object_store::ObjectStore;
use std::sync::Arc;

/// Runs the reader executable with its storage role fixed to reader.
///
/// # Errors
/// Returns configuration, storage-open, or server errors.
pub async fn run(args: impl IntoIterator<Item = String>) -> Result<()> {
    crate::runtime::run_for_role(Role::Reader, args).await
}

pub(crate) struct Reader {
    db: DbReader,
}

impl Reader {
    pub(crate) async fn open(
        prefix: &str,
        objects: Arc<dyn ObjectStore>,
        memory: &MemoryConfig,
        config: &ReaderConfig,
    ) -> Result<Self> {
        let cache = memory.db_cache()?;
        let db = DbReader::builder(prefix, objects)
            .with_options(config.options(memory))
            .with_db_cache(cache, 0)
            .build()
            .await?;
        Ok(Self { db })
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
