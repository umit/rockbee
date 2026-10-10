#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::{RowEntry, ValueDeletable, bytes::Bytes};

    fn row(key: &[u8], value: ValueDeletable) -> RowEntry {
        RowEntry {
            key: Bytes::copy_from_slice(key),
            value,
            seq: 42,
            create_ts: None,
            expire_ts: None,
        }
    }

    #[test]
    fn strips_rockbee_prefix_and_preserves_empty_key() {
        let event = event_from_row(row(&[0], ValueDeletable::Value(Bytes::from("v"))), 7)
            .unwrap()
            .unwrap();
        assert_eq!(event.key, Vec::<u8>::new());
        assert_eq!(event.wal_file_id, 7);
        assert_eq!(event.seq, 42);
    }

    #[test]
    fn filters_private_key_and_encodes_delete() {
        assert!(
            event_from_row(row(&[1, b'x'], ValueDeletable::Tombstone), 7)
                .unwrap()
                .is_none()
        );
        let event = event_from_row(row(&[0, b'x'], ValueDeletable::Tombstone), 7)
            .unwrap()
            .unwrap();
        assert_eq!(event.op, "delete");
        assert_eq!(event.value, None);
    }

    #[test]
    fn rejects_merge_operands() {
        assert!(
            event_from_row(
                row(&[0, b'x'], ValueDeletable::Merge(Bytes::from("operand"))),
                7
            )
            .unwrap_err()
            .to_string()
            .contains("merge")
        );
    }

    #[test]
    fn emits_non_utf8_public_bytes_without_stopping() {
        let event = event_from_row(
            row(
                &[0, 0xff, 0x00],
                ValueDeletable::Value(Bytes::from_static(&[0x80, 0xff, 0x00])),
            ),
            7,
        )
        .unwrap()
        .unwrap();
        assert_eq!(event.key, vec![0xff, 0x00]);
        assert_eq!(event.value, Some(vec![0x80, 0xff, 0x00]));
    }

    #[test]
    fn serializes_public_bytes_as_json_arrays() {
        let event = event_from_row(
            row(
                &[0, 0xff],
                ValueDeletable::Value(Bytes::from_static(&[0x80])),
            ),
            7,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"op":"put","key":[255],"value":[128],"seq":42,"wal_file_id":7}"#
        );
    }

    #[test]
    fn serializes_delete_with_a_null_value() {
        let event = event_from_row(row(&[0, b'k'], ValueDeletable::Tombstone), 7)
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"op":"delete","key":[107],"value":null,"seq":42,"wal_file_id":7}"#
        );
    }
}
use rockbee_protocol::Result;
use serde::Serialize;
use slatedb::{RowEntry, ValueDeletable};

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct Event {
    pub op: &'static str,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub seq: u64,
    pub wal_file_id: u64,
    /// Original writer expiry; consumers must not restart the TTL on receipt.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at_unix_ms: Option<i64>,
}

/// Converts only Rockbee's public key namespace. Engine-private rows are skipped.
pub fn event_from_row(row: RowEntry, wal_file_id: u64) -> Result<Option<Event>> {
    let Some((&0, key)) = row.key.as_ref().split_first() else {
        return Ok(None);
    };
    let key = key.to_vec();
    let (op, value) = match row.value {
        ValueDeletable::Value(value) => ("put", Some(value.to_vec())),
        ValueDeletable::Tombstone => ("delete", None),
        ValueDeletable::Merge(_) => return Err("CDC cannot emit SlateDB merge operands".into()),
    };
    Ok(Some(Event {
        op,
        key,
        value,
        seq: row.seq,
        wal_file_id,
        expires_at_unix_ms: row.expire_ts,
    }))
}
