//! Shared scalar and byte-field checks, without allocation or payload ownership.

use crate::{MAX_KEY_BYTES, MAX_VALUE_BYTES, ProtocolError, VERSION, limits::MAX_BUDGET_MS};

pub(crate) fn validate_version(version: u8) -> Result<(), ProtocolError> {
    if version != VERSION {
        return Err(ProtocolError::UnsupportedVersion(version));
    }
    Ok(())
}

pub(crate) fn validate_key(key: &[u8]) -> Result<(), ProtocolError> {
    if key.is_empty() || key.len() > MAX_KEY_BYTES {
        return Err(ProtocolError::InvalidKeyLength(key.len()));
    }
    Ok(())
}

pub(crate) fn validate_value(value: &[u8]) -> Result<(), ProtocolError> {
    if value.len() > MAX_VALUE_BYTES {
        return Err(ProtocolError::ValueTooLarge(value.len()));
    }
    Ok(())
}

pub(crate) fn validate_budget(budget_ms: u64) -> Result<(), ProtocolError> {
    if !(1..=MAX_BUDGET_MS).contains(&budget_ms) {
        return Err(ProtocolError::InvalidBudget(budget_ms));
    }
    Ok(())
}
