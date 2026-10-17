//! Read-only DuckDB SQL over immutable, manifest-published Parquet objects.
use crate::exporter::{Segment, load_manifest, table_path};
use crate::{QueryResult, Result};
use duckdb::{Config, Connection, InterruptHandle};
use rockbee_protocol::table::{MAX_QUERY_RESULT_BYTES, MAX_QUERY_ROWS, TableRequest};
use slatedb::object_store::{ObjectStore, ObjectStoreExt, path::Path};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;

const QUERY_TIMEOUT: Duration = Duration::from_secs(4);
const MAX_CACHE_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const MAX_CACHE_FILES: usize = 16_384;
const MAX_SEGMENT_BYTES: u64 = 32 * 1024 * 1024;

/// A reader's bounded, disposable Parquet cache and DuckDB execution admission.
///
/// SQL sees only this table's published generation. The cache never determines
/// which generation is authoritative: every query reloads the export manifest.
pub struct QueryReader {
    prefix: String,
    objects: Arc<dyn ObjectStore>,
    cache: OnceLock<Arc<Cache>>,
    workers: Arc<Semaphore>,
    timeout: Duration,
    memory_limit_mb: u32,
}

impl QueryReader {
    pub fn new(prefix: String, objects: Arc<dyn ObjectStore>) -> Self {
        Self {
            prefix,
            objects,
            cache: OnceLock::new(),
            workers: Arc::new(Semaphore::new(2)),
            timeout: QUERY_TIMEOUT,
            memory_limit_mb: 256,
        }
    }

    /// Override SQL admission, deadline and DuckDB memory while retaining the
    /// fixed result, disk-cache and spill bounds.
    ///
    /// # Errors
    /// Rejects zero/unrepresentable deadlines, memory outside 1..=16384 MB and
    /// concurrency outside 1..=128 before opening any files or storage.
    pub fn with_limits(
        prefix: String,
        objects: Arc<dyn ObjectStore>,
        timeout: Duration,
        memory_limit_mb: u32,
        max_concurrent: usize,
    ) -> Result<Self> {
        if timeout.is_zero() || std::time::Instant::now().checked_add(timeout).is_none() {
            return Err("query timeout must be positive and representable".into());
        }
        if !(1..=16_384).contains(&memory_limit_mb) {
            return Err("query memory_limit_mb must be in 1..=16384".into());
        }
        if !(1..=128).contains(&max_concurrent) {
            return Err("query max_concurrent must be in 1..=128".into());
        }
        Ok(Self {
            workers: Arc::new(Semaphore::new(max_concurrent)),
            timeout,
            memory_limit_mb,
            ..Self::new(prefix, objects)
        })
    }

    /// Execute a DuckDB SELECT. `registered` distinguishes an empty new table
    /// from a known table whose first export has not been published yet.
    ///
    /// # Errors
    /// Returns errors for invalid SQL, warming exports, resource limits, object
    /// download failures, timeout, or unavailable execution admission.
    pub async fn execute(&self, table: &str, sql: &str, registered: bool) -> Result<QueryResult> {
        TableRequest::Query {
            table: table.into(),
            sql: sql.into(),
        }
        .validate()?;
        let permit = Arc::clone(&self.workers)
            .try_acquire_owned()
            .map_err(|_| "reader SQL workers are busy")?;
        let control = Arc::new(Control::default());
        let _cancel = Cancel(Arc::clone(&control));
        let work = async {
            let manifest = load_manifest(self.objects.as_ref(), &self.prefix, table).await?;
            if registered && manifest.is_none() {
                return Err("table Parquet export is not ready; retry the read later".into());
            }
            let watermark = manifest.as_ref().map_or(0, |m| m.exported_wal_file_id);
            let cache = self.cache().await?;
            let mut files = Vec::new();
            if let Some(manifest) = manifest {
                let allowed_prefix = format!("{}/", table_path(&self.prefix, table));
                for segment in manifest.segments {
                    if !segment.path.starts_with(&allowed_prefix) || segment.path.contains("/../") {
                        return Err("manifest references an object outside this table".into());
                    }
                    files.push(cache.get(self.objects.as_ref(), segment).await?);
                }
            }
            let table = table.to_owned();
            let sql = sql.to_owned();
            let worker_control = Arc::clone(&control);
            let memory_limit_mb = self.memory_limit_mb;
            let job = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                let _finished = Finished(Arc::clone(&worker_control));
                run_sql(
                    &table,
                    &sql,
                    &files,
                    watermark,
                    memory_limit_mb,
                    &worker_control,
                )
            });
            // Keep interrupting after cancellation, including the small interval
            // between preparing a statement and starting execution. Admission is
            // owned by the blocking worker until it actually returns.
            let monitor = Arc::clone(&control);
            tokio::spawn(async move {
                while !monitor.done.load(Ordering::Acquire) {
                    if monitor.cancelled.load(Ordering::Acquire) {
                        monitor.interrupt();
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            });
            job.await?
        };
        tokio::time::timeout(self.timeout, work)
            .await
            .map_err(|_| "reader SQL query timed out")?
    }

    async fn cache(&self) -> Result<Arc<Cache>> {
        if let Some(cache) = self.cache.get() {
            return Ok(Arc::clone(cache));
        }
        let cache = Arc::new(tokio::task::spawn_blocking(Cache::new).await??);
        let _ = self.cache.set(Arc::clone(&cache));
        Ok(Arc::clone(
            self.cache
                .get()
                .ok_or("query cache initialization failed")?,
        ))
    }
}

#[derive(Default)]
struct Control {
    cancelled: AtomicBool,
    done: AtomicBool,
    handle: Mutex<Option<Arc<InterruptHandle>>>,
}

impl Control {
    fn interrupt(&self) {
        if let Ok(handle) = self.handle.lock()
            && let Some(handle) = handle.as_ref()
        {
            handle.interrupt();
        }
    }
}
struct Cancel(Arc<Control>);
impl Drop for Cancel {
    fn drop(&mut self) {
        self.0.cancelled.store(true, Ordering::Release);
        self.0.interrupt();
    }
}
struct Finished(Arc<Control>);
impl Drop for Finished {
    fn drop(&mut self) {
        self.0.done.store(true, Ordering::Release);
    }
}

struct CachedFile {
    path: tempfile::TempPath,
    bytes: u64,
    _directory: Arc<tempfile::TempDir>,
}
#[derive(Default)]
struct Index {
    files: HashMap<String, (Arc<CachedFile>, u64)>,
    bytes: u64,
    clock: u64,
}
struct Cache {
    directory: Arc<tempfile::TempDir>,
    index: Mutex<Index>,
}
impl Cache {
    fn new() -> Result<Self> {
        Ok(Self {
            directory: Arc::new(
                tempfile::Builder::new()
                    .prefix("rockbee-parquet-")
                    .tempdir()?,
            ),
            index: Mutex::new(Index::default()),
        })
    }
    async fn get(&self, objects: &dyn ObjectStore, segment: Segment) -> Result<Arc<CachedFile>> {
        if segment.bytes > MAX_SEGMENT_BYTES {
            return Err("Parquet segment exceeds 32 MiB cache object limit".into());
        }
        {
            let mut index = self.index.lock().map_err(|_| "query cache lock poisoned")?;
            index.clock = index.clock.saturating_add(1);
            let clock = index.clock;
            if let Some((file, used)) = index.files.get_mut(&segment.path) {
                if file.bytes != segment.bytes {
                    return Err("immutable Parquet object changed size".into());
                }
                *used = clock;
                return Ok(Arc::clone(file));
            }
        }
        let object = objects.get(&Path::parse(&segment.path)?).await?;
        if object.meta.size != segment.bytes {
            return Err("Parquet object size does not match its manifest".into());
        }
        let directory = Arc::clone(&self.directory);
        let (file, path) = tokio::task::spawn_blocking(move || {
            tempfile::NamedTempFile::new_in(directory.path())
                .map(tempfile::NamedTempFile::into_parts)
        })
        .await??;
        let mut file = tokio::fs::File::from_std(file);
        let mut stream = object.into_stream();
        let mut bytes = 0_u64;
        while let Some(chunk) = std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
            let chunk = chunk?;
            bytes = bytes
                .checked_add(u64::try_from(chunk.len())?)
                .ok_or("Parquet size overflow")?;
            if bytes > segment.bytes {
                return Err("Parquet download exceeds its manifest size".into());
            }
            file.write_all(&chunk).await?;
        }
        file.flush().await?;
        drop(file);
        if bytes != segment.bytes {
            return Err("incomplete Parquet download".into());
        }
        let cached = Arc::new(CachedFile {
            path,
            bytes,
            _directory: Arc::clone(&self.directory),
        });
        let mut index = self.index.lock().map_err(|_| "query cache lock poisoned")?;
        if let Some((existing, _)) = index.files.get(&segment.path) {
            return Ok(Arc::clone(existing));
        }
        while index.bytes.saturating_add(bytes) > MAX_CACHE_BYTES
            || index.files.len() >= MAX_CACHE_FILES
        {
            let oldest = index
                .files
                .iter()
                .filter(|(_, (file, _))| Arc::strong_count(file) == 1)
                .min_by_key(|(_, (_, used))| *used)
                .map(|(key, _)| key.clone());
            let Some(oldest) = oldest else {
                return Err("reader Parquet cache is full with active queries".into());
            };
            if let Some((file, _)) = index.files.remove(&oldest) {
                index.bytes = index.bytes.saturating_sub(file.bytes);
            }
        }
        index.clock = index.clock.saturating_add(1);
        let clock = index.clock;
        index.bytes = index.bytes.saturating_add(bytes);
        index
            .files
            .insert(segment.path, (Arc::clone(&cached), clock));
        Ok(cached)
    }
}

fn quoted(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn run_sql(
    table: &str,
    sql: &str,
    files: &[Arc<CachedFile>],
    watermark: u64,
    memory_limit_mb: u32,
    control: &Control,
) -> Result<QueryResult> {
    if control.cancelled.load(Ordering::Acquire) {
        return Err("SQL query cancelled".into());
    }
    let spill = tempfile::Builder::new()
        .prefix("rockbee-duckdb-")
        .tempdir()?;
    let config = Config::default()
        .max_memory(&format!("{memory_limit_mb}MB"))?
        .with("autoinstall_known_extensions", "false")?
        .with("autoload_known_extensions", "false")?
        .with("allow_persistent_secrets", "false")?;
    let conn = Connection::open_in_memory_with_flags(config)?;
    *control
        .handle
        .lock()
        .map_err(|_| "SQL interrupt lock poisoned")? = Some(conn.interrupt_handle());
    let paths: Vec<String> = files
        .iter()
        .map(|file| quoted(&file.path.to_string_lossy()))
        .collect();
    conn.execute_batch(&format!("SET temp_directory = {}; SET max_temp_directory_size = '1GB'; SET allowed_paths = [{}]; SET allowed_directories = [{}]; SET enable_external_access = false", quoted(&spill.path().to_string_lossy()), paths.join(","), quoted(&spill.path().to_string_lossy())))?;
    if files.is_empty() {
        conn.execute_batch(&format!(
            "CREATE TABLE \"{table}\" (id VARCHAR, value JSON)"
        ))?;
    } else {
        let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
        conn.execute_batch(&format!(
            "CREATE VIEW \"{table}\" AS SELECT id, CAST(value AS JSON) AS value FROM (
                SELECT *, row_number() OVER (PARTITION BY id ORDER BY seq DESC, wal_file_id DESC, row_ordinal DESC) AS _rockbee_rank
                FROM read_parquet([{}], union_by_name=true)
             ) WHERE _rockbee_rank = 1 AND NOT tombstone AND (expires_at_unix_ms IS NULL OR expires_at_unix_ms > {now})", paths.join(",")
        ))?;
    }
    conn.execute_batch("SET lock_configuration = true")?;
    crate::sql::validate(&conn, table, sql)?;
    if control.cancelled.load(Ordering::Acquire) {
        return Err("SQL query cancelled".into());
    }
    // DuckDB's query() accepts exactly one SELECT and treats SQL as data. Never
    // hand user SQL to Connection::prepare directly (it runs prior statements).
    let mut stmt = conn.prepare(&format!(
        "SELECT to_json(r) FROM query(?) AS r LIMIT {}",
        MAX_QUERY_ROWS + 1
    ))?;
    let mut result_rows = stmt.query([sql])?;
    let mut rows = Vec::new();
    let mut size = 2_usize;
    while let Some(row) = result_rows.next()? {
        if rows.len() == MAX_QUERY_ROWS {
            return Err("SQL result exceeds 1000 rows; add LIMIT or narrow the query".into());
        }
        let text: String = row.get(0)?;
        let separator = usize::from(!rows.is_empty());
        size = size
            .checked_add(text.len() + separator)
            .ok_or("SQL result size overflow")?;
        if size > MAX_QUERY_RESULT_BYTES {
            return Err("SQL result exceeds 512 KiB; select fewer columns or rows".into());
        }
        rows.push(serde_json::from_str(&text)?);
    }
    if serde_json::to_vec(&rows)?.len() > MAX_QUERY_RESULT_BYTES {
        return Err("SQL result exceeds 512 KiB".into());
    }
    Ok(QueryResult {
        rows,
        exported_wal_file_id: watermark,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use slatedb::object_store::memory::InMemory;

    #[test]
    fn selected_parquet_files_cannot_bypass_latest_tombstone_or_expiry_filter() -> Result<()> {
        let directory = Arc::new(tempfile::tempdir()?);
        let path = tempfile::NamedTempFile::new_in(directory.path())?.into_temp_path();
        let source = Connection::open_in_memory()?;
        source.execute_batch(&format!(
            "COPY (SELECT * FROM (VALUES
                ('deleted', '{{\"old\":1}}'::JSON, 1::UBIGINT, NULL::BIGINT, false, 1::UBIGINT, 0::UBIGINT),
                ('deleted', NULL::JSON, 2::UBIGINT, NULL::BIGINT, true, 2::UBIGINT, 0::UBIGINT),
                ('expired', '{{\"old\":1}}'::JSON, 1::UBIGINT, NULL::BIGINT, false, 1::UBIGINT, 1::UBIGINT),
                ('expired', '{{\"new\":1}}'::JSON, 2::UBIGINT, 1::BIGINT, false, 2::UBIGINT, 1::UBIGINT)
            ) t(id,value,seq,expires_at_unix_ms,tombstone,wal_file_id,row_ordinal)) TO {} (FORMAT PARQUET)", quoted(&path.to_string_lossy())
        ))?;
        let bytes = std::fs::metadata(&path)?.len();
        let files = vec![Arc::new(CachedFile {
            path,
            bytes,
            _directory: directory,
        })];
        let control = Control::default();
        assert!(
            run_sql(
                "products",
                "SELECT * FROM products",
                &files,
                2,
                256,
                &control
            )?
            .rows
            .is_empty()
        );
        let path = quoted(&files[0].path.to_string_lossy());
        for sql in [
            format!("SELECT * FROM read_parquet({path})"),
            format!("SELECT * FROM parquet_scan({path})"),
            format!("WITH x AS (SELECT * FROM read_parquet({path})) SELECT * FROM x"),
            format!("SELECT * FROM read_blob({path})"),
            format!("SELECT read_blob({path})"),
            format!("SELECT * FROM read_text({path})"),
            format!("SELECT * FROM {}", quoted(&files[0].path.to_string_lossy())),
            "SELECT * FROM query('SELECT * FROM products')".into(),
            "SELECT * FROM information_schema.tables".into(),
        ] {
            assert!(
                run_sql("products", &sql, &files, 2, 256, &control).is_err(),
                "source capability leaked through: {sql}"
            );
        }
        Ok(())
    }

    #[test]
    fn configurable_limits_reject_invalid_values() {
        for (timeout, memory, workers) in [
            (Duration::ZERO, 256, 2),
            (Duration::MAX, 256, 2),
            (QUERY_TIMEOUT, 0, 2),
            (QUERY_TIMEOUT, 16_385, 2),
            (QUERY_TIMEOUT, 256, 0),
            (QUERY_TIMEOUT, 256, 129),
        ] {
            assert!(
                QueryReader::with_limits(
                    "limits".into(),
                    Arc::new(InMemory::new()),
                    timeout,
                    memory,
                    workers
                )
                .is_err()
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn configured_memory_admission_and_deadline_reach_the_engine() -> Result<()> {
        let reader = QueryReader::with_limits(
            "limits".into(),
            Arc::new(InMemory::new()),
            Duration::from_secs(2),
            32,
            1,
        )?;
        assert_eq!(reader.workers.available_permits(), 1);
        let result = reader
            .execute(
                "products",
                "SELECT current_setting('memory_limit') AS memory",
                false,
            )
            .await?;
        assert_eq!(result.rows, vec![json!({"memory":"30.5 MiB"})]);
        let reader = QueryReader::with_limits(
            "short-query".into(),
            Arc::new(InMemory::new()),
            Duration::from_millis(50),
            32,
            1,
        )?;
        let error = reader.execute("products", "SELECT sum(sin(i::DOUBLE) * cos(j::DOUBLE)) AS n FROM range(1000000) t(i), range(1000000) u(j)", false).await.err().ok_or("expected configured timeout")?;
        assert!(error.to_string().contains("timed out"), "{error}");
        tokio::time::timeout(Duration::from_secs(3), async {
            while reader.workers.available_permits() != 1 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn empty_table_runs_real_sql_and_rejects_mutation_and_external_reads() -> Result<()> {
        let reader = QueryReader::new("empty-query".into(), Arc::new(InMemory::new()));
        let result = reader
            .execute("products", "SELECT count(*) AS n FROM products", false)
            .await?;
        assert_eq!(result.rows, vec![json!({"n":0})]);
        assert_eq!(result.exported_wal_file_id, 0);
        for sql in [
            "DROP TABLE products",
            "SELECT 1; SELECT 2",
            "SELECT * FROM read_csv('/etc/passwd')",
            "SELECT * FROM users",
        ] {
            assert!(
                reader.execute("products", sql, false).await.is_err(),
                "{sql}"
            );
        }
        assert!(
            reader
                .execute("products", "SELECT * FROM products", true)
                .await
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn results_are_bounded_without_silently_truncating_sql() -> Result<()> {
        let reader = QueryReader::new("bounded-query".into(), Arc::new(InMemory::new()));
        assert!(
            reader
                .execute("products", "SELECT i FROM range(1001) t(i)", false)
                .await
                .is_err()
        );
        let exact = reader
            .execute("products", "SELECT repeat('a', 524278) AS s", false)
            .await?;
        assert_eq!(
            serde_json::to_vec(&exact.rows)?.len(),
            MAX_QUERY_RESULT_BYTES
        );
        assert!(
            reader
                .execute("products", "SELECT repeat('a', 524279) AS s", false)
                .await
                .is_err()
        );
        assert!(
            reader
                .execute("products", "SELECT repeat('a', 524289) AS big", false)
                .await
                .is_err()
        );
        let result = reader
            .execute(
                "products",
                "WITH x AS (SELECT i FROM range(10) t(i)) SELECT sum(i) AS total FROM x",
                false,
            )
            .await?;
        assert_eq!(result.rows, vec![json!({"total":45})]);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancellation_interrupts_workers_and_returns_admission() -> Result<()> {
        let reader = Arc::new(QueryReader::new(
            "cancel-query".into(),
            Arc::new(InMemory::new()),
        ));
        const EXPENSIVE: &str = "SELECT sum(sin(i::DOUBLE) * cos(j::DOUBLE)) AS n FROM range(1000000) t(i), range(1000000) u(j)";
        let jobs: Vec<_> = (0..2)
            .map(|_| {
                let reader = reader.clone();
                tokio::spawn(async move { reader.execute("products", EXPENSIVE, false).await })
            })
            .collect();
        tokio::time::timeout(Duration::from_secs(2), async {
            while reader.workers.available_permits() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        let error = reader
            .execute("products", "SELECT 1", false)
            .await
            .err()
            .ok_or("expected SQL admission error")?;
        assert!(error.to_string().contains("busy"));
        // Give both native statements time to enter execution before dropping callers.
        tokio::time::sleep(Duration::from_millis(150)).await;
        for job in jobs {
            job.abort();
            let _ = job.await;
        }
        tokio::time::timeout(Duration::from_secs(3), async {
            while reader.workers.available_permits() != 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        assert_eq!(
            reader
                .execute("products", "SELECT 1 AS n", false)
                .await?
                .rows,
            vec![json!({"n":1})]
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deadline_interrupts_native_query_and_returns_admission() -> Result<()> {
        let reader = QueryReader::new("deadline-query".into(), Arc::new(InMemory::new()));
        let error = reader.execute("products", "SELECT sum(sin(i::DOUBLE) * cos(j::DOUBLE)) AS n FROM range(1000000) t(i), range(1000000) u(j)", false).await.err().ok_or("expected SQL timeout")?;
        assert!(error.to_string().contains("timed out"), "{error}");
        tokio::time::timeout(Duration::from_secs(3), async {
            while reader.workers.available_permits() != 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        Ok(())
    }
}
