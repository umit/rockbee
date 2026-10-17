//! Reader role: opens SlateDB's read-only engine and follows durable writer updates.
//!
//! Readers may lag the writer; GET provides eventual consistency.

use rockbee_runtime::config::ReaderConfig;
use rockbee_runtime::memory::MemoryConfig;
use rockbee_runtime::ttl::visible_value;
use rockbee_runtime::{Result, Role, Storage};
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

pub struct Reader {
    db: DbReader,
    query: rockbee_query::QueryReader,
}

impl Reader {
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

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let key = rockbee_runtime::key::to_storage_key(key)?;
        visible_value(self.db.get_key_value(&key).await?)
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
            return Err(rockbee_runtime::rejected(
                "read-only server: send put/delete to the writer",
            )
            .into());
        }
        request.validate()?;
        match request {
            TableRequest::Get { table, id } => Ok(Response::Value {
                value: visible_value(self.db.get_key_value(table_key(table, id)?).await?)?,
            }),
            TableRequest::Query { table, sql } => {
                let mut catalogue = vec![2];
                catalogue.extend_from_slice(table.as_bytes());
                let registered = self.db.get(catalogue).await?.is_some();
                Ok(Response::Query {
                    result: self.query.execute(table, sql, registered).await?,
                })
            }
            TableRequest::Put { .. } | TableRequest::Delete { .. } => {
                unreachable!("mutations rejected before dispatch")
            }
        }
    }

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
    async fn put(&self, _: &[u8], _: &[u8]) -> Result<()> {
        Err(rockbee_runtime::rejected("read-only server: send put/delete to the writer").into())
    }
    async fn put_with_ttl(&self, _: &[u8], _: &[u8], _: u64) -> Result<()> {
        Err(rockbee_runtime::rejected("read-only server: send put/delete to the writer").into())
    }
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get(key).await
    }
    async fn delete(&self, _: &[u8]) -> Result<()> {
        Err(rockbee_runtime::rejected("read-only server: send put/delete to the writer").into())
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
