//! Durable, bounded write-side Parquet generations for JSON table queries.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rockbee_protocol::Result;
use rockbee_protocol::table::{table_prefix, validate_document};
use serde::{Deserialize, Serialize};
use slatedb::admin::Admin;
use slatedb::config::{CheckpointOptions, FlushOptions, FlushType};
use slatedb::object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, path::Path};
use slatedb::wal::{SlateDbWalReaderBuilder, SlateDbWalReaderOptions, WalError, WalReader};
use slatedb::{Db, DbReader, DbReaderMode, ValueDeletable};
use tokio::sync::oneshot;
use uuid::Uuid;

const BATCH_ROWS: usize = 1000;
const BATCH_BYTES: usize = 8 * 1024 * 1024;
const MAX_ENCODED_SEGMENT_BYTES: usize = 32 * 1024 * 1024;
const COMPACT_DELTAS: usize = 128;
// Bounds retained files after a hard crash; expired snapshots never publish.
const SNAPSHOT_LEASE: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Debug, Clone, Serialize, Deserialize)]
/// An immutable Parquet object belonging to one published table generation.
pub struct Segment {
    pub path: String,
    pub bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// An atomically published segment set and its fully consumed WAL watermark.
pub struct TableManifest {
    pub format_version: u32,
    pub revision: u64,
    pub writer_epoch: u64,
    pub exported_wal_file_id: u64,
    pub segments: Vec<Segment>,
    #[serde(default)]
    pub snapshot_segment_count: usize,
    #[serde(default)]
    pub compacted_at_unix_ms: u64,
}

/// Returns the directory prefix containing immutable numbered manifest revisions.
pub fn manifest_path(prefix: &str, table: &str) -> Path {
    table_path(prefix, table).join("manifests")
}

/// Encodes names so case-sensitive tables remain distinct on local filesystems.
pub fn table_path(prefix: &str, table: &str) -> Path {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let encoded: String = table
        .bytes()
        .flat_map(|byte| {
            [
                char::from(HEX[usize::from(byte >> 4)]),
                char::from(HEX[usize::from(byte & 15)]),
            ]
        })
        .collect();
    Path::from(format!(
        "{}/_rockbee_tables/{encoded}",
        prefix.trim_end_matches('/')
    ))
}

/// Loads the newest immutable revision without collecting the manifest history.
/// Keeping all revision slots prevents stale writers from reusing an old CAS slot.
pub async fn load_manifest(
    objects: &dyn ObjectStore,
    prefix: &str,
    table: &str,
) -> Result<Option<TableManifest>> {
    table_prefix(table)?;
    let path = manifest_path(prefix, table);
    let mut listing = objects.list(Some(&path));
    let mut latest: Option<(u64, Path)> = None;
    while let Some(object) = std::future::poll_fn(|cx| listing.as_mut().poll_next(cx)).await {
        let object = object?;
        let name = object.location.filename().unwrap_or_default();
        if object.location != path.clone().join(name) {
            continue;
        }
        let Some(number) = name.strip_suffix(".json").filter(|s| s.len() == 20) else {
            continue;
        };
        if !number.bytes().all(|byte| byte.is_ascii_digit()) {
            continue;
        }
        let Ok(revision) = number.parse::<u64>() else {
            continue;
        };
        if revision == 0 {
            continue;
        }
        if latest.as_ref().is_none_or(|(old, _)| revision > *old) {
            latest = Some((revision, object.location));
        }
    }
    let Some((revision, path)) = latest else {
        return Ok(None);
    };
    let bytes = objects.get(&path).await?.bytes().await?;
    let manifest: TableManifest =
        tokio::task::spawn_blocking(move || serde_json::from_slice(&bytes)).await??;
    if manifest.format_version != 1 || manifest.revision != revision {
        return Err("unsupported or inconsistent Parquet manifest format".into());
    }
    Ok(Some(manifest))
}

/// One bounded background export worker owned by a SlateDB writer.
pub struct Exporter {
    stop: Option<oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<Result<()>>>,
}

#[derive(Debug)]
struct ExporterFenced;

impl std::fmt::Display for ExporterFenced {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Parquet exporter fenced by a newer writer")
    }
}

impl std::error::Error for ExporterFenced {}

impl Exporter {
    /// Starts export independently of mutation durability acknowledgements.
    ///
    /// # Errors
    /// Initialization failures are returned; asynchronous failures retry or fence the worker.
    pub async fn start(db: Arc<Db>, prefix: String, objects: Arc<dyn ObjectStore>) -> Result<Self> {
        let worker = Worker::new(db, prefix, objects);
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(500));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = &mut stopped => return Ok(()),
                    _ = interval.tick() => {}
                }
                let result = tokio::select! {
                    _ = &mut stopped => return Ok(()),
                    result = worker.tick() => result,
                };
                if let Err(error) = result {
                    // Transient object-store failures retry; a fenced owner must stop.
                    if error.downcast_ref::<ExporterFenced>().is_some() {
                        return Err(error);
                    }
                    tracing::warn!(%error, "Parquet export tick failed; retrying");
                }
            }
        });
        Ok(Self {
            stop: Some(stop),
            task: Some(task),
        })
    }

    /// Cancels active export and waits for the worker to exit. Owned snapshot
    /// cleanup continues asynchronously; its finite lease bounds crash retention.
    ///
    /// # Errors
    /// Returns worker fencing, shutdown or task failures.
    pub async fn close(&mut self) -> Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.await??;
        }
        Ok(())
    }
}

struct Worker {
    db: Arc<Db>,
    prefix: String,
    objects: Arc<dyn ObjectStore>,
    epoch: u64,
}

impl Worker {
    fn new(db: Arc<Db>, prefix: String, objects: Arc<dyn ObjectStore>) -> Self {
        let epoch = db.manifest().writer_epoch();
        Self {
            db,
            prefix,
            objects,
            epoch,
        }
    }

    fn admin(&self) -> Admin {
        Admin::builder(self.prefix.clone(), self.objects.clone()).build()
    }

    async fn tick(&self) -> Result<()> {
        self.check_epoch().await?;
        let mut catalogue = self.db.scan_prefix([2], ..).await?;
        while let Some(row) = catalogue.next().await? {
            let table = std::str::from_utf8(&row.key[1..])?;
            table_prefix(table)?;
            if let Err(error) = self.export_table(table).await {
                if error.downcast_ref::<ExporterFenced>().is_some() {
                    return Err(error);
                }
                tracing::warn!(%table, %error, "Parquet export failed; retaining published generation");
            }
        }
        Ok(())
    }

    async fn check_epoch(&self) -> Result<()> {
        let manifest = self
            .admin()
            .read_manifest(None)
            .await?
            .ok_or("missing SlateDB manifest")?;
        if manifest.writer_epoch() != self.epoch {
            return Err(ExporterFenced.into());
        }
        Ok(())
    }

    async fn load_manifest(&self, table: &str) -> Result<Option<TableManifest>> {
        let manifest = load_manifest(self.objects.as_ref(), &self.prefix, table).await?;
        if manifest
            .as_ref()
            .is_some_and(|m| m.writer_epoch > self.epoch)
        {
            return Err(ExporterFenced.into());
        }
        Ok(manifest)
    }

    async fn export_table(&self, table: &str) -> Result<()> {
        self.check_epoch().await?;
        let current = self.load_manifest(table).await?;
        let compact = current.as_ref().is_some_and(|m| {
            m.segments.len().saturating_sub(m.snapshot_segment_count) >= COMPACT_DELTAS
                && now_ms().saturating_sub(m.compacted_at_unix_ms) >= 60_000
        });
        if current.is_none() || compact {
            return self.snapshot(table, current).await;
        }
        let mut manifest = current.expect("checked existing manifest");
        let revision = manifest.revision;
        let old_cursor = manifest.exported_wal_file_id;
        match self.append_wal(table, &mut manifest).await {
            Ok(()) => {
                if manifest.exported_wal_file_id != old_cursor
                    || manifest.writer_epoch != self.epoch
                {
                    self.publish(table, manifest, Some(revision)).await?;
                }
                Ok(())
            }
            Err(error)
                if error
                    .downcast_ref::<WalError>()
                    .is_some_and(|e| matches!(e, WalError::WalTruncated(_))) =>
            {
                // No partial cursor/segments were published. Rebuild from a pinned snapshot.
                self.snapshot(table, self.load_manifest(table).await?).await
            }
            Err(error) => Err(error),
        }
    }

    async fn snapshot(&self, table: &str, current: Option<TableManifest>) -> Result<()> {
        // Make the checkpoint a latest-version base instead of replaying old WAL
        // versions into a purported compaction. Serving/ACK paths do not await this.
        self.db
            .flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await?;
        let admin = Arc::new(self.admin());
        let deadline = tokio::time::Instant::now() + SNAPSHOT_LEASE;
        let checkpoint = admin
            .create_detached_checkpoint(&CheckpointOptions {
                lifetime: Some(SNAPSHOT_LEASE),
                ..Default::default()
            })
            .await?;
        let mut guard = SnapshotGuard {
            admin,
            checkpoint: checkpoint.id,
            reader: None,
            active: true,
        };
        let reader = Arc::new(
            DbReader::builder(self.prefix.clone(), self.objects.clone())
                .with_reader_mode(DbReaderMode::Checkpoint(checkpoint.id))
                .build()
                .await?,
        );
        guard.reader = Some(reader.clone());
        let result = self
            .snapshot_checkpoint(
                table,
                reader.as_ref(),
                current.map(|m| m.revision),
                deadline,
            )
            .await;
        let cleanup = guard.close().await;
        result?;
        cleanup?;
        Ok(())
    }

    async fn snapshot_checkpoint(
        &self,
        table: &str,
        reader: &DbReader,
        revision: Option<u64>,
        deadline: tokio::time::Instant,
    ) -> Result<()> {
        ensure_snapshot_lease(deadline)?;
        let high = reader.manifest().next_wal_sst_id().saturating_sub(1);
        let mut manifest = TableManifest {
            format_version: 1,
            revision: revision.unwrap_or(0),
            writer_epoch: self.epoch,
            exported_wal_file_id: high,
            segments: Vec::new(),
            snapshot_segment_count: 0,
            compacted_at_unix_ms: now_ms(),
        };
        let prefix = table_prefix(table)?;
        let mut source = reader.scan_prefix(&prefix, ..).await?;
        let mut batch = Batch::default();
        while let Some(row) = source.next().await? {
            let id = std::str::from_utf8(&row.key[prefix.len()..])?.to_owned();
            batch.push(ExportRow {
                id,
                value: Some(String::from_utf8(row.value.to_vec())?),
                seq: row.seq,
                expires_at_unix_ms: row.expire_ts,
                tombstone: false,
                wal_file_id: high,
                row_ordinal: 0,
            });
            if batch.full() {
                self.flush(table, &mut batch, &mut manifest).await?;
            }
        }
        self.flush(table, &mut batch, &mut manifest).await?;
        manifest.snapshot_segment_count = manifest.segments.len();
        // Reconcile a finite committed tail. A gap discards this unpublished generation.
        ensure_snapshot_lease(deadline)?;
        self.append_wal(table, &mut manifest).await?;
        ensure_snapshot_lease(deadline)?;
        self.publish(table, manifest, revision).await
    }

    async fn append_wal(&self, table: &str, manifest: &mut TableManifest) -> Result<()> {
        let wal = SlateDbWalReaderBuilder::new()
            .with_path(Path::from(self.prefix.clone()))
            .with_object_store(self.objects.clone())
            .with_options(SlateDbWalReaderOptions {
                max_buffered_bytes: BATCH_BYTES,
                max_fetch_tasks: 2,
                read_ahead_bytes: 1024 * 1024,
            })
            .build()?;
        let end = wal.last_wal_file_id(manifest.exported_wal_file_id).await?;
        let prefix = table_prefix(table)?;
        let mut batch = Batch::default();
        let start = manifest
            .exported_wal_file_id
            .checked_add(1)
            .ok_or("WAL cursor overflow")?;
        let streaming_wal = slatedb::WalReader::new(self.prefix.clone(), self.objects.clone());
        for file in start..=end {
            // The newer WalRows API materializes an entire WAL SST. This older public
            // row iterator streams it with a fixed upstream 256MiB read-ahead budget.
            let mut source = streaming_wal
                .get(file)
                .iterator()
                .await
                .map_err(|e| classify_wal_error(e, file))?;
            let mut ordinal = 0u64;
            while let Some(row) = source
                .next()
                .await
                .map_err(|e| classify_wal_error(e, file))?
            {
                let row_ordinal = ordinal;
                ordinal = ordinal.checked_add(1).ok_or("WAL row ordinal overflow")?;
                if !row.key.starts_with(&prefix) {
                    continue;
                }
                let (value, tombstone) = match row.value {
                    ValueDeletable::Value(bytes) => {
                        (Some(String::from_utf8(bytes.to_vec())?), false)
                    }
                    ValueDeletable::Tombstone => (None, true),
                    ValueDeletable::Merge(_) => {
                        return Err("merge rows are unsupported for JSON tables".into());
                    }
                };
                batch.push(ExportRow {
                    id: std::str::from_utf8(&row.key[prefix.len()..])?.into(),
                    value,
                    seq: row.seq,
                    expires_at_unix_ms: row.expire_ts,
                    tombstone,
                    wal_file_id: file,
                    row_ordinal,
                });
                if batch.full() {
                    self.flush(table, &mut batch, manifest).await?;
                }
            }
            // Cursor advances only after the file iterator reaches EOF, never after
            // an uploaded partial batch. All metadata remains private until CAS.
            manifest.exported_wal_file_id = file;
        }
        self.flush(table, &mut batch, manifest).await
    }

    async fn flush(
        &self,
        table: &str,
        batch: &mut Batch,
        manifest: &mut TableManifest,
    ) -> Result<()> {
        if batch.rows.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut batch.rows);
        batch.bytes = 0;
        let bytes = tokio::task::spawn_blocking(move || encode_parquet(rows)).await??;
        if bytes.len() > MAX_ENCODED_SEGMENT_BYTES {
            return Err("encoded Parquet segment exceeds supported 32MiB bound".into());
        }
        let path = table_path(&self.prefix, table)
            .join("segments")
            .join(format!("{}.parquet", Uuid::new_v4()));
        let length = u64::try_from(bytes.len())?;
        self.objects
            .put_opts(
                &path,
                bytes.into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await?;
        manifest.segments.push(Segment {
            path: path.to_string(),
            bytes: length,
        });
        Ok(())
    }

    async fn publish(
        &self,
        table: &str,
        mut manifest: TableManifest,
        revision: Option<u64>,
    ) -> Result<()> {
        self.check_epoch().await?;
        let current = self.load_manifest(table).await?;
        if current.as_ref().map(|m| m.revision) != revision {
            return Err(
                "Parquet manifest publication conflict; retry from current generation".into(),
            );
        }
        if current
            .as_ref()
            .is_some_and(|m| m.exported_wal_file_id > manifest.exported_wal_file_id)
        {
            return Err("Parquet manifest watermark must not regress".into());
        }
        manifest.writer_epoch = self.epoch;
        manifest.revision = revision
            .unwrap_or(0)
            .checked_add(1)
            .ok_or("Parquet manifest revision overflow")?;
        let path =
            manifest_path(&self.prefix, table).join(format!("{:020}.json", manifest.revision));
        let bytes = tokio::task::spawn_blocking(move || serde_json::to_vec(&manifest)).await??;
        self.check_epoch().await?;
        // A revision slot is immutable and is never deleted. Concurrent/stale writers
        // compete for exactly one successor slot; only a complete manifest can win.
        self.objects
            .put_opts(
                &path,
                bytes.into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await?;
        Ok(())
    }
}

/// Cancelling an export drops this guard. Only its owned checkpoint/reader are cleaned.
struct SnapshotGuard {
    admin: Arc<Admin>,
    checkpoint: Uuid,
    reader: Option<Arc<DbReader>>,
    active: bool,
}

impl SnapshotGuard {
    async fn close(&mut self) -> Result<()> {
        let reader_result = match &self.reader {
            Some(reader) => reader.close().await,
            None => Ok(()),
        };
        let checkpoint_result = self.admin.delete_checkpoint(self.checkpoint).await;
        if checkpoint_result.is_ok() {
            self.active = false;
        }
        reader_result?;
        checkpoint_result?;
        Ok(())
    }
}

impl Drop for SnapshotGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let admin = self.admin.clone();
        let checkpoint = self.checkpoint;
        let reader = self.reader.clone();
        runtime.spawn(async move {
            if let Some(reader) = reader { let _ = reader.close().await; }
            if let Err(error) = admin.delete_checkpoint(checkpoint).await {
                tracing::warn!(%checkpoint, %error, "failed to clean cancelled Parquet snapshot checkpoint");
            }
        });
    }
}

fn ensure_snapshot_lease(deadline: tokio::time::Instant) -> Result<()> {
    if tokio::time::Instant::now() >= deadline {
        return Err(
            "Parquet snapshot checkpoint lease expired; rebuilding unpublished generation".into(),
        );
    }
    Ok(())
}

fn classify_wal_error(
    error: slatedb::Error,
    file: u64,
) -> Box<dyn std::error::Error + Send + Sync> {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&error);
    while let Some(current) = source {
        if current
            .downcast_ref::<slatedb::object_store::Error>()
            .is_some_and(|e| matches!(e, slatedb::object_store::Error::NotFound { .. }))
            || current
                .downcast_ref::<Arc<slatedb::object_store::Error>>()
                .is_some_and(|e| {
                    matches!(e.as_ref(), slatedb::object_store::Error::NotFound { .. })
                })
        {
            return WalError::WalTruncated(file).into();
        }
        source = current.source();
    }
    error.into()
}

struct ExportRow {
    id: String,
    value: Option<String>,
    seq: u64,
    expires_at_unix_ms: Option<i64>,
    tombstone: bool,
    wal_file_id: u64,
    row_ordinal: u64,
}

#[derive(Default)]
struct Batch {
    rows: Vec<ExportRow>,
    bytes: usize,
}

impl Batch {
    fn push(&mut self, row: ExportRow) {
        self.bytes = self
            .bytes
            .saturating_add(row.id.len() + row.value.as_ref().map_or(0, String::len) + 64);
        self.rows.push(row);
    }
    fn full(&self) -> bool {
        self.rows.len() >= BATCH_ROWS || self.bytes >= BATCH_BYTES
    }
}

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

fn encode_parquet(rows: Vec<ExportRow>) -> Result<Vec<u8>> {
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("segment.parquet");
    let connection = duckdb::Connection::open_in_memory()?;
    connection.execute_batch("SET threads=1; SET memory_limit='64MB'; CREATE TABLE export_rows(id VARCHAR, value JSON, seq UBIGINT, expires_at_unix_ms BIGINT, tombstone BOOLEAN, wal_file_id UBIGINT, row_ordinal UBIGINT)")?;
    {
        let mut appender = connection.appender("export_rows")?;
        for row in rows {
            if let Some(value) = &row.value {
                validate_document(value.as_bytes())?;
            }
            appender.append_row(duckdb::params![
                row.id,
                row.value,
                row.seq,
                row.expires_at_unix_ms,
                row.tombstone,
                row.wal_file_id,
                row.row_ordinal
            ])?;
        }
        appender.flush()?;
    }
    let escaped = path.to_string_lossy().replace('\'', "''");
    connection.execute_batch(&format!(
        "COPY export_rows TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    ))?;
    Ok(std::fs::read(path)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::object_store::memory::InMemory;

    #[tokio::test]
    async fn close_cancels_blocked_export_and_releases_its_checkpoint() {
        use slatedb::object_store::throttle::{ThrottleConfig, ThrottledStore};
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open("blocked-export", objects.clone()).await.unwrap());
        db.put([2, b't'], b"t")
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        db.put(
            rockbee_protocol::table::table_key("t", "a").unwrap(),
            br#"{"n":1}"#,
        )
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
        let throttled = Arc::new(ThrottledStore::new(
            objects.clone(),
            ThrottleConfig {
                wait_put_per_call: Duration::from_millis(100),
                ..Default::default()
            },
        ));
        let admin = Admin::builder("blocked-export", objects).build();
        let mut exporter = Exporter::start(db.clone(), "blocked-export".into(), throttled.clone())
            .await
            .unwrap();
        let checkpoint = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(checkpoint) = admin
                    .list_checkpoints(None)
                    .await
                    .unwrap()
                    .into_iter()
                    .next()
                {
                    break checkpoint.id;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        // Capture an object-store get that blocks far longer than shutdown's
        // deadline, while the unthrottled fixture can observe checkpoint cleanup.
        throttled.config_mut(|config| {
            config.wait_get_per_call = Duration::from_secs(60);
        });
        tokio::time::sleep(Duration::from_millis(250)).await;
        // An in-flight throttled get retains its captured sleep. New cleanup
        // requests use zero delay and can release the pin after cancellation.
        throttled.config_mut(|config| *config = ThrottleConfig::default());
        let closed = tokio::time::timeout(Duration::from_millis(500), exporter.close()).await;
        closed
            .expect("exporter close must cancel the active tick")
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if admin
                    .list_checkpoints(None)
                    .await
                    .unwrap()
                    .iter()
                    .all(|cp| cp.id != checkpoint)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn snapshot_compaction_removes_superseded_versions() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open("compact-export", objects.clone()).await.unwrap());
        db.put([2, b't'], b"t")
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        let key = rockbee_protocol::table::table_key("t", "a").unwrap();
        db.put(&key, br#"{"n":1}"#)
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        let worker = Worker::new(db.clone(), "compact-export".into(), objects.clone());
        worker.export_table("t").await.unwrap();
        db.put(&key, br#"{"n":2}"#)
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        worker.export_table("t").await.unwrap();
        let before = worker.load_manifest("t").await.unwrap().unwrap();
        assert_eq!(inspect_rows(objects.as_ref(), &before).await.2, 2);
        worker.snapshot("t", Some(before.clone())).await.unwrap();
        let compacted = worker.load_manifest("t").await.unwrap().unwrap();
        assert!(compacted.exported_wal_file_id >= before.exported_wal_file_id);
        assert!(
            compacted
                .segments
                .iter()
                .all(|s| before.segments.iter().all(|old| s.path != old.path))
        );
        assert_eq!(
            inspect_rows(objects.as_ref(), &compacted).await,
            (Some(r#"{"n":2}"#.into()), false, 1)
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn expired_snapshot_lease_does_not_publish_partial_generation() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open("expired-export", objects.clone()).await.unwrap());
        let worker = Worker::new(db.clone(), "expired-export".into(), objects.clone());
        let admin = worker.admin();
        let checkpoint = admin
            .create_detached_checkpoint(&CheckpointOptions {
                lifetime: Some(SNAPSHOT_LEASE),
                ..Default::default()
            })
            .await
            .unwrap();
        let reader = DbReader::builder("expired-export", objects)
            .with_reader_mode(DbReaderMode::Checkpoint(checkpoint.id))
            .build()
            .await
            .unwrap();
        let result = worker
            .snapshot_checkpoint(
                "t",
                &reader,
                None,
                tokio::time::Instant::now() - Duration::from_secs(1),
            )
            .await;
        assert!(result.unwrap_err().to_string().contains("lease expired"));
        assert!(worker.load_manifest("t").await.unwrap().is_none());
        reader.close().await.unwrap();
        admin.delete_checkpoint(checkpoint.id).await.unwrap();
        db.close().await.unwrap();
    }

    async fn inspect_rows(
        objects: &dyn ObjectStore,
        manifest: &TableManifest,
    ) -> (Option<String>, bool, i64) {
        let directory = tempfile::tempdir().unwrap();
        let mut paths = Vec::new();
        for (index, segment) in manifest.segments.iter().enumerate() {
            let path = directory.path().join(format!("{index}.parquet"));
            let bytes = objects
                .get(&Path::from(segment.path.clone()))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            tokio::fs::write(&path, bytes).await.unwrap();
            paths.push(path.to_string_lossy().into_owned());
        }
        tokio::task::spawn_blocking(move || {
            let connection = duckdb::Connection::open_in_memory().unwrap();
            let files = paths.iter().map(|p| format!("'{}'", p.replace('\'', "''"))).collect::<Vec<_>>().join(",");
            let _directory = directory;
            connection.query_row(&format!("SELECT CAST(value AS VARCHAR), tombstone, count(*) OVER () FROM read_parquet([{files}]) ORDER BY seq DESC, wal_file_id DESC, row_ordinal DESC LIMIT 1"), [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap()
        }).await.unwrap()
    }

    async fn latest_row(
        objects: &dyn ObjectStore,
        manifest: &TableManifest,
    ) -> (Option<String>, bool) {
        let (value, tombstone, _) = inspect_rows(objects, manifest).await;
        (value, tombstone)
    }

    #[tokio::test]
    async fn parquet_preserves_json_expiry_and_unsigned_ordering_columns() {
        tokio::task::spawn_blocking(|| {
            let bytes = encode_parquet(vec![ExportRow {
                id: "a".into(), value: Some(r#"{"nested":{"x":1}}"#.into()),
                seq: u64::MAX, expires_at_unix_ms: Some(1_900_000_000_000),
                tombstone: false, wal_file_id: u64::MAX, row_ordinal: u64::MAX,
            }]).unwrap();
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("schema.parquet");
            std::fs::write(&path, bytes).unwrap();
            let connection = duckdb::Connection::open_in_memory().unwrap();
            let sql = format!("SELECT id, value->'nested'->>'x', seq, expires_at_unix_ms, tombstone, wal_file_id, row_ordinal FROM read_parquet('{}')", path.to_string_lossy().replace('\'', "''"));
            let row: (String, String, u64, i64, bool, u64, u64) = connection.query_row(&sql, [], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?))
            }).unwrap();
            assert_eq!(row, ("a".into(), "1".into(), u64::MAX, 1_900_000_000_000, false, u64::MAX, u64::MAX));
        }).await.unwrap();
    }

    #[tokio::test]
    async fn cancelling_snapshot_task_releases_its_checkpoint() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Db::open("cancel-export", objects.clone()).await.unwrap();
        let admin = Arc::new(Admin::builder("cancel-export", objects).build());
        let checkpoint = admin
            .create_detached_checkpoint(&CheckpointOptions::default())
            .await
            .unwrap();
        let guard = SnapshotGuard {
            admin: admin.clone(),
            checkpoint: checkpoint.id,
            reader: None,
            active: true,
        };
        let (entered, started) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _guard = guard;
            let _ = entered.send(());
            std::future::pending::<()>().await;
        });
        started.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if admin
                    .list_checkpoints(None)
                    .await
                    .unwrap()
                    .iter()
                    .all(|cp| cp.id != checkpoint.id)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn local_filesystem_publishes_consecutive_create_only_revisions() {
        let directory = tempfile::tempdir().unwrap();
        let objects: Arc<dyn ObjectStore> = Arc::new(
            slatedb::object_store::local::LocalFileSystem::new_with_prefix(directory.path())
                .unwrap(),
        );
        let db = Arc::new(Db::open("local-export", objects.clone()).await.unwrap());
        db.put([2, b't'], b"t")
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        let key = rockbee_protocol::table::table_key("t", "a").unwrap();
        db.put(&key, br#"{"n":1}"#)
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        let worker = Worker::new(db.clone(), "local-export".into(), objects.clone());
        worker.export_table("t").await.unwrap();
        let first = load_manifest(objects.as_ref(), "local-export", "t")
            .await
            .unwrap()
            .unwrap();
        db.put(&key, br#"{"n":2}"#)
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        worker.export_table("t").await.unwrap();
        let second = load_manifest(objects.as_ref(), "local-export", "t")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second.revision, first.revision + 1);
        assert!(second.exported_wal_file_id > first.exported_wal_file_id);
        assert!(
            objects
                .head(
                    &manifest_path("local-export", "t")
                        .join(format!("{:020}.json", first.revision))
                )
                .await
                .is_ok()
        );
        assert!(
            load_manifest(objects.as_ref(), "local-export", "untouched")
                .await
                .unwrap()
                .is_none()
        );
        db.put([2, b'T'], b"T")
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        db.put(
            rockbee_protocol::table::table_key("T", "a").unwrap(),
            br#"{"n":99}"#,
        )
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
        worker.export_table("T").await.unwrap();
        let upper = load_manifest(objects.as_ref(), "local-export", "T")
            .await
            .unwrap()
            .unwrap();
        assert!(
            upper
                .segments
                .iter()
                .all(|s| second.segments.iter().all(|other| s.path != other.path))
        );
        assert_eq!(
            latest_row(objects.as_ref(), &upper).await,
            (Some(r#"{"n":99}"#.into()), false)
        );
        assert_eq!(
            latest_row(objects.as_ref(), &second).await,
            (Some(r#"{"n":2}"#.into()), false)
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn stale_publication_preserves_cursor_and_uploaded_segments() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open("parquet-cas", objects.clone()).await.unwrap());
        db.put([2, b't'], b"t")
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        db.put(
            rockbee_protocol::table::table_key("t", "a").unwrap(),
            br#"{"n":1}"#,
        )
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
        let worker = Worker::new(db.clone(), "parquet-cas".into(), objects);
        worker.export_table("t").await.unwrap();
        let mut stale = worker.load_manifest("t").await.unwrap().unwrap();
        let revision = stale.revision;
        let mut winner = stale.clone();
        winner.exported_wal_file_id += 1;
        worker
            .publish("t", winner.clone(), Some(revision))
            .await
            .unwrap();
        stale.exported_wal_file_id += 2;
        stale.segments.clear();
        assert!(worker.publish("t", stale, Some(revision)).await.is_err());
        let published = worker.load_manifest("t").await.unwrap().unwrap();
        assert_eq!(published.exported_wal_file_id, winner.exported_wal_file_id);
        assert_eq!(published.segments.len(), winner.segments.len());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn retained_wal_gap_rebuilds_and_restart_keeps_manifest() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let prefix = "parquet-gap";
        let db = Arc::new(Db::open(prefix, objects.clone()).await.unwrap());
        db.put([2, b't'], b"t")
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        let key = rockbee_protocol::table::table_key("t", "a").unwrap();
        db.put(&key, br#"{"n":1}"#)
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        let worker = Worker::new(db.clone(), prefix.into(), objects.clone());
        worker.export_table("t").await.unwrap();
        let first = worker.load_manifest("t").await.unwrap().unwrap();
        db.put(&key, br#"{"n":2}"#)
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();
        let old =
            slatedb::WalReader::new(prefix, objects.clone()).get(first.exported_wal_file_id + 1);
        let metadata = old.metadata().await.unwrap();
        objects.delete(&metadata.metadata.location).await.unwrap();
        worker.export_table("t").await.unwrap();
        let rebuilt = worker.load_manifest("t").await.unwrap().unwrap();
        assert!(rebuilt.exported_wal_file_id > first.exported_wal_file_id);
        assert!(
            rebuilt
                .segments
                .iter()
                .all(|new| first.segments.iter().all(|old| old.path != new.path))
        );
        assert_eq!(
            latest_row(objects.as_ref(), &rebuilt).await,
            (Some(r#"{"n":2}"#.into()), false)
        );
        db.close().await.unwrap();
        drop(worker);
        drop(db);
        let reopened = Arc::new(Db::open(prefix, objects.clone()).await.unwrap());
        let worker = Worker::new(reopened.clone(), prefix.into(), objects);
        worker.export_table("t").await.unwrap();
        let restarted = worker.load_manifest("t").await.unwrap().unwrap();
        assert_eq!(restarted.writer_epoch, reopened.manifest().writer_epoch());
        assert!(restarted.exported_wal_file_id >= rebuilt.exported_wal_file_id);
        reopened.close().await.unwrap();
    }

    #[tokio::test]
    async fn newer_writer_fences_old_exporter_before_publication() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open("parquet-fence", objects.clone()).await.unwrap());
        db.put([2, b't'], b"t")
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        let worker = Worker::new(db.clone(), "parquet-fence".into(), objects.clone());
        worker.export_table("t").await.unwrap();
        let old = worker.load_manifest("t").await.unwrap().unwrap();
        let newer = Db::open("parquet-fence", objects).await.unwrap();
        assert!(
            worker
                .export_table("t")
                .await
                .unwrap_err()
                .to_string()
                .contains("fenced")
        );
        let current = worker.load_manifest("t").await.unwrap().unwrap();
        assert_eq!(current.writer_epoch, old.writer_epoch);
        assert_eq!(current.exported_wal_file_id, old.exported_wal_file_id);
        newer.close().await.unwrap();
        // The fenced database is already closed by SlateDB; its close may report fencing.
        let _ = db.close().await;
    }

    #[tokio::test]
    async fn exports_real_parquet_and_replays_updates_and_tombstones() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(Db::open("parquet-test", objects.clone()).await.unwrap());
        db.put([2, b't'], b"t")
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        db.put(
            rockbee_protocol::table::table_key("t", "a").unwrap(),
            br#"{"n":1}"#,
        )
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
        let worker = Worker::new(db.clone(), "parquet-test".into(), objects.clone());
        worker.export_table("t").await.unwrap();
        let first = worker.load_manifest("t").await.unwrap().unwrap();
        assert!(!first.segments.is_empty());
        let bytes = objects
            .get(&Path::from(first.segments[0].path.clone()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(&bytes[..4], b"PAR1");
        assert_eq!(&bytes[bytes.len() - 4..], b"PAR1");
        db.put(
            rockbee_protocol::table::table_key("t", "a").unwrap(),
            br#"{"n":2}"#,
        )
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
        db.delete(rockbee_protocol::table::table_key("t", "a").unwrap())
            .await
            .unwrap()
            .await_durable()
            .await
            .unwrap();
        worker.export_table("t").await.unwrap();
        let second = worker.load_manifest("t").await.unwrap().unwrap();
        assert!(second.exported_wal_file_id > first.exported_wal_file_id);
        assert!(second.segments.len() > first.segments.len());
        let dir = tempfile::tempdir().unwrap();
        let mut paths = Vec::new();
        for (index, segment) in second.segments.iter().enumerate() {
            let path = dir.path().join(format!("{index}.parquet"));
            let bytes = objects
                .get(&Path::from(segment.path.clone()))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            tokio::fs::write(&path, bytes).await.unwrap();
            paths.push(path.to_string_lossy().into_owned());
        }
        tokio::task::spawn_blocking(move || {
            let connection = duckdb::Connection::open_in_memory().unwrap();
            let files = paths.iter().map(|p| format!("'{}'", p.replace('\'', "''"))).collect::<Vec<_>>().join(",");
            let deleted: bool = connection.query_row(&format!("SELECT tombstone FROM read_parquet([{files}]) ORDER BY seq DESC, wal_file_id DESC, row_ordinal DESC LIMIT 1"), [], |row| row.get(0)).unwrap();
            assert!(deleted);
        }).await.unwrap();
        db.close().await.unwrap();
    }
}
