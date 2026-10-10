use super::checkpoint::CursorStore;
use super::{CdcConfig, Event, event_from_row};
use rockbee_protocol::Result;
use slatedb::object_store::path::Path;
use slatedb::wal::{SlateDbWalReaderBuilder, WalError, WalReader};
use std::io::{self, Write};

/// Runs until interrupted, a retained-WAL gap is detected, or the optional file limit is met.
pub async fn run(config: CdcConfig) -> Result<()> {
    let objects = config.backend.connect()?;
    let reader = SlateDbWalReaderBuilder::new()
        .with_path(Path::from(config.prefix.as_str()))
        .with_object_store(objects)
        .build()?;
    let mut cursor = CursorStore::open(config.cursor, config.identity)?;
    let start = cursor
        .last_wal_file_id()
        .checked_add(1)
        .ok_or("CDC WAL cursor overflow")?;
    let mut iterator = reader.iterator((start..).into()).await.map_err(wal_error)?;
    let mut output = io::BufWriter::new(io::stdout().lock());
    let mut completed_files = 0_u64;
    loop {
        let rows = tokio::select! {
            result = iterator.next() => result.map_err(wal_error)?,
            result = tokio::signal::ctrl_c() => { result?; return Ok(()); }
        };
        let Some(rows) = rows else {
            return Ok(());
        };
        let advanced = process_rows(rows, &mut cursor, &mut output)?;
        if advanced > 0 {
            completed_files += advanced;
            if config
                .max_wal_files
                .is_some_and(|limit| completed_files >= limit)
            {
                return Ok(());
            }
        }
    }
}

fn process_rows(
    rows: slatedb::wal::WalRows,
    cursor: &mut CursorStore,
    output: &mut impl Write,
) -> Result<u64> {
    let previous = cursor.last_wal_file_id();
    emit_rows(rows.rows, rows.last_consumed_wal_file_id, output)?;
    output.flush()?;
    cursor.advance(rows.last_consumed_wal_file_id)?;
    Ok(rows.last_consumed_wal_file_id.saturating_sub(previous))
}

fn emit_rows(
    rows: Vec<slatedb::RowEntry>,
    wal_file_id: u64,
    output: &mut impl Write,
) -> Result<()> {
    for row in rows {
        if let Some(event) = event_from_row(row, wal_file_id)? {
            emit_event(&event, output)?;
        }
    }
    Ok(())
}
fn emit_event(event: &Event, output: &mut impl Write) -> Result<()> {
    serde_json::to_writer(&mut *output, event)?;
    output.write_all(b"\n")?;
    Ok(())
}
fn wal_error(error: WalError) -> Box<dyn std::error::Error + Send + Sync> {
    match error {
        WalError::WalTruncated(wal_id) => {
            format!("CDC WAL file {wal_id} was truncated; resnapshot is required").into()
        }
        error => error.to_string().into(),
    }
}

#[cfg(test)]
mod tests {
    use super::super::checkpoint::CursorStore;
    use super::*;
    use slatedb::{ValueDeletable, bytes::Bytes};
    #[test]
    fn failed_output_does_not_finish_batch() {
        struct Broken;
        impl Write for Broken {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                Err(io::Error::other("broken"))
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let row = slatedb::RowEntry {
            key: Bytes::from_static(&[0, b'x']),
            value: ValueDeletable::Value(Bytes::from("v")),
            seq: 1,
            create_ts: None,
            expire_ts: None,
        };
        let temp = tempfile::tempdir().unwrap();
        let mut cursor = CursorStore::open(temp.path().join("cursor"), "stream".into()).unwrap();
        let rows = slatedb::wal::WalRows {
            rows: vec![row],
            last_consumed_wal_file_id: 1,
        };
        assert!(process_rows(rows, &mut cursor, &mut Broken).is_err());
        assert_eq!(cursor.last_wal_file_id(), 0);
    }
    #[test]
    fn retained_wal_gap_requires_resnapshot() {
        assert!(
            wal_error(WalError::WalTruncated(9))
                .to_string()
                .contains("resnapshot")
        );
    }

    #[test]
    fn failed_flush_does_not_advance_cursor() {
        struct FlushFails;
        impl Write for FlushFails {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Err(io::Error::other("flush failed"))
            }
        }
        let temp = tempfile::tempdir().unwrap();
        let mut cursor = CursorStore::open(temp.path().join("cursor"), "stream".into()).unwrap();
        let rows = slatedb::wal::WalRows {
            rows: vec![],
            last_consumed_wal_file_id: 1,
        };
        assert!(process_rows(rows, &mut cursor, &mut FlushFails).is_err());
        assert_eq!(cursor.last_wal_file_id(), 0);
    }
}
