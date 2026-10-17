//! Local, opt-in evidence for a million-document bounded snapshot and SQL path.
//!
//! Run with `cargo test -p rockbee-query --test scale -- --ignored --nocapture`.
//! The local macOS toolchain needs `CXXFLAGS='-isystem /Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/include/c++/v1'`.
//! These timings describe the local file backend and debug build, not production S3.

use std::sync::Arc;
use std::time::{Duration, Instant};

use rockbee_protocol::table::table_key;
use rockbee_query::exporter::load_manifest;
use rockbee_query::{Exporter, QueryReader, Result};
use serde_json::json;
use slatedb::object_store::{ObjectStore, ObjectStoreExt, local::LocalFileSystem, path::Path};
use slatedb::{Db, WriteBatch};

const RECORDS: u64 = 1_000_000;
const WRITE_BATCH: u64 = 10_000;
const TABLE: &str = "scale_docs";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "standalone million-record local scale verification"]
async fn million_document_snapshot_and_cold_warm_queries() -> Result<()> {
    let directory = tempfile::Builder::new()
        .prefix("rockbee-million-")
        .tempdir()?;
    let objects: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(directory.path())?);
    let prefix = format!("scale-{}", uuid::Uuid::new_v4());
    let db = Arc::new(Db::open(prefix.clone(), objects.clone()).await?);
    let mut exporter = None;
    let total = Instant::now();
    let result: Result<()> = async {
        let started = Instant::now();
        let mut catalogue = vec![2];
        catalogue.extend_from_slice(TABLE.as_bytes());
        for start in (0..RECORDS).step_by(usize::try_from(WRITE_BATCH)?) {
            let mut batch = WriteBatch::new();
            if start == 0 { batch.put(&catalogue, TABLE.as_bytes()); }
            for index in start..(start + WRITE_BATCH).min(RECORDS) {
                let id = format!("record-{index:07}");
                let value = serde_json::to_vec(&json!({"n":index,"group":index % 100}))?;
                batch.put(table_key(TABLE, &id)?, value);
            }
            db.write(batch).await?.await_durable().await?;
        }
        eprintln!("local_scale fixture_records={RECORDS} fixture_ms={} build={} backend=local_file os={} arch={}",
            started.elapsed().as_millis(), if cfg!(debug_assertions) { "debug" } else { "release" },
            std::env::consts::OS, std::env::consts::ARCH);

        // Start only after the complete durable fixture: this exercises a real
        // streaming bootstrap, rather than exporting a small table while filling it.
        let started = Instant::now();
        exporter = Some(Exporter::start(db.clone(), prefix.clone(), objects.clone()).await?);
        let manifest = tokio::time::timeout(Duration::from_secs(600), async {
            loop {
                if let Some(manifest) = load_manifest(objects.as_ref(), &prefix, TABLE).await? {
                    return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(manifest);
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }).await.map_err(|_| "million-row Parquet bootstrap exceeded 10 minutes")??;
        let parquet_bytes: u64 = manifest.segments.iter().map(|segment| segment.bytes).sum();
        eprintln!("local_scale bootstrap_ms={} segments={} parquet_bytes={parquet_bytes} exported_wal_file_id={}", started.elapsed().as_millis(), manifest.segments.len(), manifest.exported_wal_file_id);
        if manifest.segments.len() < 2 || manifest.exported_wal_file_id == 0 {
            return Err("million-row bootstrap did not produce multiple committed Parquet segments".into());
        }
        let first = objects.get(&Path::from(manifest.segments[0].path.clone())).await?.bytes().await?;
        if !first.starts_with(b"PAR1") || !first.ends_with(b"PAR1") {
            return Err("exported scale object is not a Parquet file".into());
        }

        for (label, sql, expected) in [
            ("count", "SELECT count(*) AS n FROM scale_docs", RECORDS),
            ("json_filter", "SELECT count(*) AS n FROM scale_docs WHERE TRY_CAST(value->>'group' AS BIGINT) = 7", RECORDS / 100),
        ] {
            // A fresh reader gives each query its own cold immutable-object cache.
            let reader = QueryReader::new(prefix.clone(), objects.clone());
            for temperature in ["cold", "warm"] {
                let started = Instant::now();
                let output = reader.execute(TABLE, sql, true).await;
                eprintln!("local_scale query={label} cache={temperature} elapsed_ms={} outcome={}", started.elapsed().as_millis(), if output.is_ok() { "ok" } else { "error" });
                let output = output?;
                if output.rows != vec![json!({"n":expected})] {
                    return Err(format!("{label} {temperature} returned incorrect aggregate: {:?}", output.rows).into());
                }
                if output.exported_wal_file_id < manifest.exported_wal_file_id {
                    return Err("scale query watermark regressed".into());
                }
            }
        }
        eprintln!("local_scale total_ms={}", total.elapsed().as_millis());
        Ok(())
    }.await;

    // Cleanup runs for success and Result failures; only this fixture's exporter,
    // SlateDB instance and temporary local store are ever touched.
    let exporter_closed = match &mut exporter {
        Some(exporter) => exporter.close().await,
        None => Ok(()),
    };
    let db_closed = db.close().await;
    result?;
    exporter_closed?;
    db_closed?;
    Ok(())
}
