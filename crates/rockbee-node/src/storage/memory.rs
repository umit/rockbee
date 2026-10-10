use crate::Result;
use serde::{Deserialize, Serialize, de::Error as _};
use slatedb::config::{DbReaderOptions, Settings};
use slatedb::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
use slatedb::db_cache::{
    DEFAULT_BLOCK_CACHE_CAPACITY, DEFAULT_META_CACHE_CAPACITY, DbCache, SplitCache,
};
use std::sync::Arc;

/// Maximum shard count accepted from configuration to avoid pathological cache construction.
const MAX_CACHE_SHARDS: usize = 65_536;

/// Per-engine SlateDB memory settings. Each writer or reader process owns its own cache.
///
/// The pinned Foyer cache uses LRU; this configuration exposes capacity and shards only.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct MemoryConfig {
    pub block_cache_bytes: u64,
    pub metadata_cache_bytes: u64,
    #[serde(deserialize_with = "deserialize_shards")]
    pub block_cache_shards: usize,
    #[serde(deserialize_with = "deserialize_shards")]
    pub metadata_cache_shards: usize,
    pub reader_max_memtable_bytes: u64,
    pub writer_max_unflushed_bytes: u64,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        let cache_options = FoyerCacheOptions::default();
        Self {
            block_cache_bytes: DEFAULT_BLOCK_CACHE_CAPACITY,
            metadata_cache_bytes: DEFAULT_META_CACHE_CAPACITY,
            block_cache_shards: cache_options.shards,
            metadata_cache_shards: cache_options.shards,
            reader_max_memtable_bytes: DbReaderOptions::default().max_memtable_bytes,
            writer_max_unflushed_bytes: Settings::default().max_unflushed_bytes as u64,
        }
    }
}

impl MemoryConfig {
    /// Validates values before SlateDB opens or can write object-store metadata.
    pub fn validate(&self) -> Result<()> {
        validate_cache_capacity(
            "block_cache_bytes",
            self.block_cache_bytes,
            self.block_cache_shards,
        )?;
        validate_cache_capacity(
            "metadata_cache_bytes",
            self.metadata_cache_bytes,
            self.metadata_cache_shards,
        )?;
        let combined_cache_bytes = self
            .block_cache_bytes
            .checked_add(self.metadata_cache_bytes)
            .ok_or("combined cache capacity overflows u64")?;
        let _ = usize::try_from(combined_cache_bytes)
            .map_err(|_| "combined cache capacity exceeds platform usize")?;
        positive_u64("reader_max_memtable_bytes", self.reader_max_memtable_bytes)?;
        let writer_max_unflushed_bytes = usize::try_from(positive_u64(
            "writer_max_unflushed_bytes",
            self.writer_max_unflushed_bytes,
        )?)
        .map_err(|_| "writer_max_unflushed_bytes exceeds platform usize")?;
        let settings = Settings::default();
        if writer_max_unflushed_bytes <= settings.l0_sst_size_bytes {
            return Err(format!(
                "writer_max_unflushed_bytes must exceed the default l0_sst_size_bytes ({})",
                settings.l0_sst_size_bytes
            )
            .into());
        }
        Ok(())
    }

    pub(crate) fn db_cache(&self) -> Result<Arc<dyn DbCache>> {
        self.validate()?;
        let block_cache: Arc<dyn DbCache> =
            Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
                max_capacity: self.block_cache_bytes,
                shards: self.block_cache_shards,
            }));
        let metadata_cache: Arc<dyn DbCache> =
            Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
                max_capacity: self.metadata_cache_bytes,
                shards: self.metadata_cache_shards,
            }));
        Ok(Arc::new(
            SplitCache::new()
                .with_block_cache(Some(block_cache))
                .with_meta_cache(Some(metadata_cache))
                .build(),
        ))
    }

    pub(crate) fn writer_settings(&self) -> Result<Settings> {
        Ok(Settings {
            max_unflushed_bytes: usize::try_from(self.writer_max_unflushed_bytes)
                .map_err(|_| "writer_max_unflushed_bytes exceeds platform usize")?,
            ..Settings::default()
        })
    }

    pub(crate) fn reader_options(&self) -> DbReaderOptions {
        DbReaderOptions {
            max_memtable_bytes: self.reader_max_memtable_bytes,
            ..DbReaderOptions::default()
        }
    }
}

fn positive_u64(name: &str, value: u64) -> Result<u64> {
    if value == 0 {
        return Err(format!("{name} must be positive").into());
    }
    Ok(value)
}

fn validate_cache_capacity(name: &str, bytes: u64, shards: usize) -> Result<()> {
    let bytes = positive_u64(name, bytes)?;
    if shards == 0 {
        return Err(format!("{name} shards must be positive").into());
    }
    if shards > MAX_CACHE_SHARDS {
        return Err(format!("{name} shards exceeds {MAX_CACHE_SHARDS}").into());
    }
    let bytes = usize::try_from(bytes).map_err(|_| format!("{name} exceeds platform usize"))?;
    if bytes < shards {
        return Err(format!("{name} must be at least its shard count").into());
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ShardCount {
    Count(usize),
    Auto(String),
}

fn deserialize_shards<'de, D>(deserializer: D) -> std::result::Result<usize, D::Error>
where
    D: serde::Deserializer<'de>,
{
    match ShardCount::deserialize(deserializer)? {
        ShardCount::Count(shards) => Ok(shards),
        ShardCount::Auto(value) if value == "auto" => Ok(FoyerCacheOptions::default().shards),
        ShardCount::Auto(value) => Err(D::Error::custom(format!(
            "cache shards must be a positive integer or 'auto', got {value:?}"
        ))),
    }
}
