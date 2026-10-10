use rockbee_protocol::Result;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct Cursor {
    pub identity: String,
    pub last_wal_file_id: u64,
}

pub(super) struct CursorStore {
    path: PathBuf,
    cursor: Cursor,
    _lock: Lock,
}

impl CursorStore {
    pub(super) fn open(path: PathBuf, identity: String) -> Result<Self> {
        let lock = Lock::acquire(&path)?;
        let cursor = match fs::read(&path) {
            Ok(data) => {
                let cursor: Cursor = serde_json::from_slice(&data)?;
                if cursor.identity != identity {
                    return Err("CDC cursor belongs to a different backend/database path".into());
                }
                cursor
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Cursor {
                identity,
                last_wal_file_id: 0,
            },
            Err(error) => return Err(error.into()),
        };
        Ok(Self {
            path,
            cursor,
            _lock: lock,
        })
    }
    pub(super) fn last_wal_file_id(&self) -> u64 {
        self.cursor.last_wal_file_id
    }
    pub(super) fn advance(&mut self, wal_file_id: u64) -> Result<()> {
        if wal_file_id <= self.cursor.last_wal_file_id {
            return Ok(());
        }
        let mut next = self.cursor.clone();
        next.last_wal_file_id = wal_file_id;
        let data = serde_json::to_vec(&next)?;
        let temp = self.path.with_extension("json.tmp");
        {
            let mut file = File::create(&temp)?;
            file.write_all(&data)?;
            file.sync_data()?;
        }
        fs::rename(&temp, &self.path)?;
        let parent = self
            .path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        File::open(parent)?.sync_data()?;
        self.cursor = next;
        Ok(())
    }
}

struct Lock {
    _file: File,
}
impl Lock {
    fn acquire(cursor: &Path) -> Result<Self> {
        let path = cursor.with_extension("lock");
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        file.try_lock()
            .map_err(|e| format!("CDC cursor is already locked: {e}"))?;
        Ok(Self { _file: file })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn persists_identity_and_rejects_other_stream() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("cursor");
        let mut cursor = CursorStore::open(path.clone(), "one".into()).unwrap();
        cursor.advance(3).unwrap();
        drop(cursor);
        assert_eq!(
            CursorStore::open(path.clone(), "one".into())
                .unwrap()
                .last_wal_file_id(),
            3
        );
        assert!(CursorStore::open(path, "two".into()).is_err());
    }
    #[test]
    fn supports_relative_cursor_paths() {
        let mut reserved = tempfile::NamedTempFile::new_in(".").unwrap();
        serde_json::to_writer(
            reserved.as_file_mut(),
            &Cursor {
                identity: "one".into(),
                last_wal_file_id: 0,
            },
        )
        .unwrap();
        let path = PathBuf::from(reserved.path().file_name().unwrap());
        let lock = path.with_extension("lock");
        let mut cursor = CursorStore::open(path.clone(), "one".into()).unwrap();
        cursor.advance(1).unwrap();
        drop(cursor);
        assert_eq!(
            CursorStore::open(path.clone(), "one".into())
                .unwrap()
                .last_wal_file_id(),
            1
        );
        let _ = fs::remove_file(&lock);
    }
    #[test]
    fn failed_persistence_does_not_advance_memory_cursor() {
        let temp = tempfile::tempdir().unwrap();
        let mut cursor = CursorStore::open(temp.path().join("cursor"), "one".into()).unwrap();
        cursor.path = temp.path().join("missing").join("cursor");
        assert!(cursor.advance(1).is_err());
        assert_eq!(cursor.last_wal_file_id(), 0);
    }

    #[test]
    fn only_one_consumer_can_own_a_cursor() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("cursor");
        let first = CursorStore::open(path.clone(), "stream".into()).unwrap();
        assert!(CursorStore::open(path.clone(), "stream".into()).is_err());
        drop(first);
        assert!(CursorStore::open(path, "stream".into()).is_ok());
    }
}
