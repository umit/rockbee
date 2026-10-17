use rockbee_protocol::table::{QueryResult, TableRequest, table_key, table_prefix};
use rockbee_protocol::{
    ExpectedResponse, Request, RequestEnvelope, Response, ResponseEnvelope, WireMessage,
};
use serde_json::json;

#[test]
fn table_messages_roundtrip_and_classify_mutations() -> rockbee_protocol::Result<()> {
    let operations = [
        TableRequest::Put {
            table: "products".into(),
            id: "42".into(),
            value: br#"{"price":2500}"#.to_vec(),
            ttl_millis: 123,
        },
        TableRequest::Get {
            table: "products".into(),
            id: "42".into(),
        },
        TableRequest::Delete {
            table: "products".into(),
            id: "42".into(),
        },
        TableRequest::Query {
            table: "products".into(),
            sql: "SELECT count(*) FROM products".into(),
        },
    ];
    for (index, operation) in operations.into_iter().enumerate() {
        let request = Request::Table(operation);
        assert_eq!(request.is_mutation(), index == 0 || index == 2);
        let envelope = RequestEnvelope {
            request_id: 99,
            request: request.clone(),
        };
        envelope.validate()?;
        let bytes = prost::Message::encode_to_vec(&envelope.to_wire());
        assert_eq!(RequestEnvelope::decode(&bytes)?, envelope);
    }
    let envelope = ResponseEnvelope {
        request_id: 99,
        response: Response::Query {
            result: QueryResult {
                rows: vec![json!({"n":2,"missing":null})],
                exported_wal_file_id: 2,
            },
        },
    };
    let decoded = ResponseEnvelope::decode(&prost::Message::encode_to_vec(&envelope.to_wire()))?;
    assert_eq!(decoded, envelope);
    assert!(decoded.validate(ExpectedResponse::Query).is_ok());
    assert!(envelope.validate(ExpectedResponse::Value).is_err());
    Ok(())
}

#[test]
fn table_boundaries_are_unambiguous_and_validate_json_objects() -> rockbee_protocol::Result<()> {
    assert_eq!(table_key("products", "42")?, b"\x01\x08products42");
    assert_ne!(table_key("a", "bc")?, table_key("ab", "c")?);
    assert!(!table_key("products", "42")?.starts_with(&[0]));
    assert!(!table_prefix("product")?.starts_with(&table_prefix("products")?));
    for table in ["", "a.b", "1bad", "bad-name", "x\0y"] {
        assert!(table_prefix(table).is_err(), "{table:?}");
    }
    assert!(table_key("products", "").is_err());
    assert!(table_key("products", &"x".repeat(1025)).is_err());
    for value in [b"broken".as_slice(), b"[]", b"null", b"42"] {
        assert!(
            TableRequest::Put {
                table: "products".into(),
                id: "42".into(),
                value: value.to_vec(),
                ttl_millis: 0
            }
            .validate()
            .is_err()
        );
    }
    assert!(
        TableRequest::Query {
            table: "products".into(),
            sql: " ".into()
        }
        .validate()
        .is_err()
    );
    Ok(())
}

#[test]
fn query_result_bounds_apply_before_sending_and_after_decoding() -> rockbee_protocol::Result<()> {
    for rows in [
        vec![json!({}); 1001],
        vec![json!(42)],
        vec![json!({"s":"x".repeat(512 * 1024)})],
    ] {
        let envelope = ResponseEnvelope {
            request_id: u64::MAX,
            response: Response::Query {
                result: QueryResult {
                    rows,
                    exported_wal_file_id: 1,
                },
            },
        };
        assert!(WireMessage::validate(&envelope).is_err());
        assert!(
            ResponseEnvelope::decode(&prost::Message::encode_to_vec(&envelope.to_wire())).is_err()
        );
    }
    let max = ResponseEnvelope {
        request_id: u64::MAX,
        response: Response::Query {
            result: QueryResult {
                rows: vec![json!({}); 1000],
                exported_wal_file_id: u64::MAX,
            },
        },
    };
    WireMessage::validate(&max)?;
    assert_eq!(
        ResponseEnvelope::decode(&prost::Message::encode_to_vec(&max.to_wire()))?,
        max
    );
    Ok(())
}
