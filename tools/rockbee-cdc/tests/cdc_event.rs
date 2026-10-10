use rockbee_cdc::event_from_row;
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
fn emits_user_put_and_filters_private_rows() {
    let event = event_from_row(row(&[0], ValueDeletable::Value(Bytes::from("v"))), 7)
        .unwrap()
        .unwrap();
    assert_eq!(event.key, Vec::<u8>::new());
    assert_eq!(event.value.as_deref(), Some(&b"v"[..]));
    assert!(
        event_from_row(row(&[1, b'x'], ValueDeletable::Tombstone), 7)
            .unwrap()
            .is_none()
    );
}

#[test]
fn ttl_cdc_preserves_absolute_expiry_instead_of_restarting_duration() {
    let mut entry = row(&[0, b'k'], ValueDeletable::Value(Bytes::from("v")));
    entry.expire_ts = Some(1_791_526_000_123);
    let event = event_from_row(entry, 7).unwrap().unwrap();
    let json = serde_json::to_value(event).unwrap();
    assert_eq!(json["expires_at_unix_ms"], 1_791_526_000_123_i64);
}
