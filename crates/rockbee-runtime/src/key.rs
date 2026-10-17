/// Prefixes every nonempty opaque application key with one zero byte for SlateDB.
/// Keep this mapping unchanged so existing nonempty records remain accessible.
/// Reject empty keys before any storage operation, including direct Store calls.
pub fn to_storage_key(key: &[u8]) -> Result<Vec<u8>, &'static str> {
    if key.is_empty() {
        return Err("key must not be empty");
    }
    if key.len() > usize::from(u16::MAX) - 1 {
        return Err("key exceeds 65534 bytes");
    }
    let mut bytes = vec![0];
    bytes.extend_from_slice(key);
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_keys_are_rejected_and_nonempty_keys_keep_the_storage_prefix() {
        assert!(to_storage_key(b"").is_err());
        assert_eq!(to_storage_key(b"\0").unwrap(), [0, 0]);
        assert_eq!(to_storage_key(b"key").unwrap(), [0, b'k', b'e', b'y']);
        assert_ne!(
            to_storage_key(b"product:100").unwrap(),
            to_storage_key(b"product:101").unwrap()
        );
    }

    #[test]
    fn maximum_key_size_is_checked_in_utf8_bytes() {
        assert_eq!(to_storage_key(&vec![0; 65_534]).unwrap().len(), 65_535);
        assert!(to_storage_key(&vec![0; 65_535]).is_err());
    }
}
