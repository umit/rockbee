//! Shared storage facade: selects a writer or reader and preserves the Store API.

use crate::Result;
use crate::config::ReaderConfig;
use crate::reader::Reader;
use crate::writer::{Writer, rejected};
use slatedb::object_store::ObjectStore;
use std::sync::Arc;

pub mod memory;
pub(crate) mod ttl;

use memory::MemoryConfig;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Writer,
    Reader,
}

enum Engine {
    Writer(Writer),
    Reader(Box<Reader>),
}

/// One SlateDB handle, using the default or supplied per-engine memory profile. No TCP or JSON here.
pub struct Store {
    engine: Engine,
}

impl Store {
    pub async fn open(role: Role, prefix: &str, objects: Arc<dyn ObjectStore>) -> Result<Self> {
        Self::open_with_memory(role, prefix, objects, MemoryConfig::default()).await
    }

    /// Opens one SlateDB engine with its own validated memory configuration.
    pub async fn open_with_memory(
        role: Role,
        prefix: &str,
        objects: Arc<dyn ObjectStore>,
        memory: MemoryConfig,
    ) -> Result<Self> {
        Self::open_with_memory_and_reader_config(
            role,
            prefix,
            objects,
            memory,
            ReaderConfig::default(),
        )
        .await
    }

    /// Opens one SlateDB engine with a validated memory profile and reader
    /// refresh settings. Writer opens intentionally ignore `reader` because
    /// they do not run SlateDB's read-only manifest/WAL poller.
    pub async fn open_with_memory_and_reader_config(
        role: Role,
        prefix: &str,
        objects: Arc<dyn ObjectStore>,
        memory: MemoryConfig,
        reader: ReaderConfig,
    ) -> Result<Self> {
        memory.validate()?;
        reader.validate()?;
        let engine = match role {
            Role::Writer => Engine::Writer(Writer::open(prefix, objects, &memory).await?),
            Role::Reader => Engine::Reader(Box::new(
                Reader::open(prefix, objects, &memory, &reader).await?,
            )),
        };
        Ok(Self { engine })
    }

    pub fn role(&self) -> Role {
        match self.engine {
            Engine::Writer(_) => Role::Writer,
            Engine::Reader(_) => Role::Reader,
        }
    }

    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.writer()?.put(key, value).await
    }

    /// Stores opaque bytes until the writer's wall-clock expiry, if one is requested.
    pub async fn put_with_ttl(&self, key: &[u8], value: &[u8], ttl_millis: u64) -> Result<()> {
        self.writer()?.put_with_ttl(key, value, ttl_millis).await
    }

    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        self.writer()?.delete(key).await
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match &self.engine {
            Engine::Writer(writer) => writer.get(key).await,
            Engine::Reader(reader) => reader.get(key).await,
        }
    }

    pub async fn close(&self) -> Result<()> {
        match &self.engine {
            Engine::Writer(writer) => writer.close().await,
            Engine::Reader(reader) => reader.close().await,
        }
    }

    fn writer(&self) -> Result<&Writer> {
        match &self.engine {
            Engine::Writer(writer) => Ok(writer),
            Engine::Reader(_) => {
                Err(rejected("read-only server: send put/delete to the writer").into())
            }
        }
    }
}
