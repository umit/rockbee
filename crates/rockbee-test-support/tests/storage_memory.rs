use rockbee_test_support::Store;
use rockbee_test_support::storage::Role;
use rockbee_test_support::storage::memory::MemoryConfig;
use slatedb::object_store::{ObjectStore, memory::InMemory};
use std::sync::Arc;

#[test]
fn memory_config_defaults_match_the_pinned_engine_profile() {
    let memory = MemoryConfig::default();

    assert_eq!(memory.block_cache_bytes, 512 * 1024 * 1024);
    assert_eq!(memory.metadata_cache_bytes, 128 * 1024 * 1024);
    assert_eq!(memory.reader_max_memtable_bytes, 64 * 1024 * 1024);
    assert_eq!(memory.writer_max_unflushed_bytes, 1024 * 1024 * 1024);
    assert!(memory.block_cache_shards > 0);
    assert!(memory.metadata_cache_shards > 0);
}

#[test]
fn memory_config_rejects_zero_oversharded_and_unknown_values() {
    let zero = MemoryConfig {
        block_cache_bytes: 0,
        ..MemoryConfig::default()
    };
    assert!(zero.validate().is_err());

    let oversharded = MemoryConfig {
        block_cache_bytes: 1,
        block_cache_shards: 2,
        ..MemoryConfig::default()
    };
    assert!(oversharded.validate().is_err());

    let zero_shards = MemoryConfig {
        metadata_cache_shards: 0,
        ..MemoryConfig::default()
    };
    assert!(zero_shards.validate().is_err());

    let overflowing_cache_sum = MemoryConfig {
        block_cache_bytes: u64::MAX,
        metadata_cache_bytes: 1,
        metadata_cache_shards: 1,
        ..MemoryConfig::default()
    };
    assert!(overflowing_cache_sum.validate().is_err());

    let too_small_writer = MemoryConfig {
        writer_max_unflushed_bytes: 64 * 1024 * 1024,
        ..MemoryConfig::default()
    };
    assert!(too_small_writer.validate().is_err());

    let unknown = serde_json::from_str::<MemoryConfig>(r#"{"unknown": 1}"#);
    assert!(unknown.is_err());
}

#[test]
fn memory_config_accepts_auto_shards_and_rejects_other_strings() {
    let configured: MemoryConfig =
        toml::from_str("block_cache_shards = 'auto'\nmetadata_cache_shards = 'auto'\n")
            .expect("auto shards should resolve using the pinned Foyer default");
    let defaults = MemoryConfig::default();
    assert_eq!(configured.block_cache_bytes, defaults.block_cache_bytes);
    assert_eq!(
        configured.metadata_cache_bytes,
        defaults.metadata_cache_bytes
    );
    assert_eq!(
        configured.reader_max_memtable_bytes,
        defaults.reader_max_memtable_bytes
    );
    assert_eq!(
        configured.writer_max_unflushed_bytes,
        defaults.writer_max_unflushed_bytes
    );
    assert_eq!(configured.block_cache_shards, defaults.block_cache_shards);
    assert_eq!(
        configured.metadata_cache_shards,
        defaults.metadata_cache_shards
    );

    let invalid = toml::from_str::<MemoryConfig>("block_cache_shards = 'cpu'");
    assert!(invalid.is_err());
}

#[tokio::test]
async fn invalid_memory_config_is_rejected_before_opening_or_writing_storage()
-> rockbee_test_support::Result<()> {
    let store = Arc::new(InMemory::new());
    let objects: Arc<dyn ObjectStore> = store.clone();
    let memory = MemoryConfig {
        writer_max_unflushed_bytes: 0,
        ..MemoryConfig::default()
    };

    let error = match Store::open_with_memory(Role::Writer, "invalid-memory", objects, memory).await
    {
        Ok(_) => return Err("invalid memory config unexpectedly opened SlateDB".into()),
        Err(error) => error,
    };
    assert!(error.to_string().contains("writer_max_unflushed_bytes"));
    let listed = store.list_with_delimiter(None).await?;
    assert!(listed.objects.is_empty());
    assert!(listed.common_prefixes.is_empty());
    Ok(())
}

#[tokio::test]
async fn custom_memory_profile_opens_the_real_writer_and_preserves_durable_bytes()
-> rockbee_test_support::Result<()> {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let minimum_shards = MemoryConfig::default().block_cache_shards;
    let memory = MemoryConfig {
        block_cache_bytes: 4096,
        metadata_cache_bytes: 4096,
        block_cache_shards: minimum_shards,
        metadata_cache_shards: minimum_shards,
        reader_max_memtable_bytes: 1024,
        writer_max_unflushed_bytes: 128 * 1024 * 1024,
    };
    let writer = Store::open_with_memory(
        Role::Writer,
        "custom-memory",
        objects.clone(),
        memory.clone(),
    )
    .await?;

    writer.put(b"memory-profile", b"exact\0bytes").await?;
    assert_eq!(
        writer.get(b"memory-profile").await?.as_deref(),
        Some(b"exact\0bytes".as_slice())
    );
    writer.close().await?;

    let reader = Store::open_with_memory(Role::Reader, "custom-memory", objects, memory).await?;
    assert_eq!(
        reader.get(b"memory-profile").await?.as_deref(),
        Some(b"exact\0bytes".as_slice())
    );
    assert!(reader.put(b"read-only", b"never").await.is_err());
    reader.close().await
}
