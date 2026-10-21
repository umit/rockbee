//! Stored encoding, key layout and bounded entry-batch layout. No I/O here.
//! Versioned bytes stored inside RocksDB; serialization only, no disk access.
pub(super) mod batch;
pub(super) mod keys;

use super::error;
use crate::types::StoreError;
use openraft::{ErrorSubject, ErrorVerb};
use serde::{Serialize, de::DeserializeOwned};

pub const STORAGE_FORMAT: u8 = 1;

pub(in crate::storage::rocksdb) fn encode<T: Serialize>(
    value: &T,
    subject: ErrorSubject<u64>,
) -> Result<Vec<u8>, StoreError> {
    postcard::to_stdvec(&(STORAGE_FORMAT, value)).map_err(|e| error(subject, ErrorVerb::Write, e))
}

pub(in crate::storage::rocksdb) fn decode<T: DeserializeOwned>(
    bytes: &[u8],
    subject: ErrorSubject<u64>,
) -> Result<T, StoreError> {
    let ((version, value), rest): ((u8, T), _) =
        postcard::take_from_bytes(bytes).map_err(|e| error(subject.clone(), ErrorVerb::Read, e))?;
    if version != STORAGE_FORMAT || !rest.is_empty() {
        return Err(error(
            subject,
            ErrorVerb::Read,
            "unsupported storage format or trailing bytes",
        ));
    }
    Ok(value)
}
