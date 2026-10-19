//! PUT, GET and DELETE payloads; validate before routing or dispatch.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{
    ProtocolError,
    validation::{validate_key, validate_value},
};

/// Opaque byte operations. Empty values are valid; empty keys are invalid.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum KvRequest {
    /// Replace the value stored under one opaque key.
    Put {
        /// Nonempty binary key, bounded by `MAX_KEY_BYTES`.
        key: Bytes,
        /// Binary value; empty values are valid.
        value: Bytes,
    },
    /// Look up a value; the response distinguishes missing from empty.
    Get {
        /// Nonempty binary lookup key.
        key: Bytes,
    },
    /// Remove the value associated with an opaque key.
    Delete {
        /// Nonempty binary key to remove.
        key: Bytes,
    },
}

impl KvRequest {
    /// Checks key and value bounds.
    ///
    /// # Errors
    /// Returns an error for empty/oversized keys or oversized values.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        match self {
            Self::Put { key, value } => {
                validate_key(key)?;
                validate_value(value)
            }
            Self::Get { key } | Self::Delete { key } => validate_key(key),
        }
    }
}
