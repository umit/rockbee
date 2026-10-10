use prost::Message;
use rockbee_protocol::{
    MAX_FRAME, MAX_KEY_LEN, Request, RequestEnvelope, Response, ResponseEnvelope, encode_frame,
    read_frame, validate_put_with_ttl_bytes, validate_request_bytes, write_frame,
};
use std::io::{self, Cursor, Read};

#[derive(::prost::Message)]
struct LegacyEnvelope {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(uint64, tag = "2")]
    request_id: u64,
    #[prost(message, optional, tag = "10")]
    put: Option<LegacyPut>,
    #[prost(message, optional, tag = "11")]
    get: Option<LegacyKey>,
    #[prost(message, optional, tag = "12")]
    delete: Option<LegacyKey>,
}

#[derive(::prost::Message)]
struct LegacyPut {
    #[prost(bytes = "vec", tag = "1")]
    key: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    value: Vec<u8>,
}

#[derive(::prost::Message)]
struct LegacyKey {
    #[prost(bytes = "vec", tag = "1")]
    key: Vec<u8>,
}

#[test]
fn response_uses_binary_length_prefix_instead_of_json_lines() {
    let mut output = Vec::new();
    write_frame(&mut output, &Response::Ok).unwrap();
    assert_eq!(output, [0, 0, 0, 5, 0x08, 0x01, 0xa2, 0x01, 0]);
}

#[test]
fn rejected_error_frame_accounts_for_the_nonzero_enum_at_the_size_boundary() {
    let mut response = ResponseEnvelope {
        request_id: 1,
        response: Response::Error {
            message: "x".repeat(MAX_FRAME - 19),
            mutation_outcome: rockbee_protocol::MutationOutcome::Rejected,
        },
    };
    let frame = encode_frame(&response).unwrap();
    assert_eq!(frame.len(), MAX_FRAME);
    assert_eq!(ResponseEnvelope::decode(&frame[4..]).unwrap(), response);
    // MAX_FRAME includes the four-byte length prefix.
    if let Response::Error { message, .. } = &mut response.response {
        message.push('x');
    }
    assert!(encode_frame(&response).is_err());
}

#[test]
fn binary_put_matches_cross_language_golden_frame() {
    let request = RequestEnvelope {
        request_id: 7,
        request: Request::Put {
            key: vec![255, 0],
            value: vec![254, 10],
        },
    };
    let golden = [
        0, 0, 0, 14, 8, 1, 16, 7, 0x52, 8, 10, 2, 255, 0, 18, 2, 254, 10,
    ];
    assert_eq!(encode_frame(&request).unwrap(), golden);
    assert_eq!(RequestEnvelope::decode(&golden[4..]).unwrap(), request);
}

#[test]
fn binary_put_with_ttl_round_trips_as_a_distinct_request() {
    let request = RequestEnvelope {
        request_id: 7,
        request: Request::PutWithTtl {
            key: vec![255, 0],
            value: vec![254, 10],
            ttl_millis: 1,
        },
    };
    let frame = encode_frame(&request).unwrap();
    assert_eq!(RequestEnvelope::decode(&frame[4..]).unwrap(), request);
}

#[test]
fn legacy_protobuf_has_no_command_to_ack_for_a_ttl_put() {
    let request = RequestEnvelope {
        request_id: 7,
        request: Request::PutWithTtl {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
            ttl_millis: 1,
        },
    };
    let frame = encode_frame(&request).unwrap();
    // This models a peer compiled before tag 13 existed: protobuf skips the
    // unknown oneof field, leaving it without any command to execute or ACK.
    let legacy = LegacyEnvelope::decode(&frame[4..]).unwrap();
    assert_eq!((legacy.version, legacy.request_id), (1, 7));
    assert!(legacy.put.is_none());
    assert!(legacy.get.is_none());
    assert!(legacy.delete.is_none());
}

#[test]
fn missing_and_present_empty_values_have_distinct_wire_messages() {
    let missing = encode_frame(&Response::Value { value: None }).unwrap();
    let empty = encode_frame(&Response::Value {
        value: Some(Vec::new()),
    })
    .unwrap();
    assert_eq!(&missing[4..], &[8, 1, 0xaa, 1, 0]);
    assert_eq!(&empty[4..], &[8, 1, 0xaa, 1, 2, 10, 0]);
    assert_eq!(
        ResponseEnvelope::decode(&missing[4..]).unwrap().response,
        Response::Value { value: None }
    );
    assert_eq!(
        ResponseEnvelope::decode(&empty[4..]).unwrap().response,
        Response::Value {
            value: Some(Vec::new())
        }
    );
}

#[test]
fn envelopes_validate_version_presence_direction_and_response_kind() {
    for payload in [vec![], vec![8, 2, 0x5a, 0], vec![8, 1], vec![8, 1, 0x5a, 1]] {
        assert!(RequestEnvelope::decode(&payload).is_err());
    }
    let put = encode_frame(&Request::Put {
        key: vec![0],
        value: vec![255],
    })
    .unwrap();
    let ok = encode_frame(&Response::Ok).unwrap();
    assert!(ResponseEnvelope::decode(&put[4..]).is_err());
    assert!(RequestEnvelope::decode(&ok[4..]).is_err());
    assert!(
        ResponseEnvelope::decode(&ok[4..])
            .unwrap()
            .validate(rockbee_protocol::ExpectedResponse::Value)
            .is_err()
    );
    let value = encode_frame(&Response::Value { value: None }).unwrap();
    assert!(
        ResponseEnvelope::decode(&value[4..])
            .unwrap()
            .validate(rockbee_protocol::ExpectedResponse::Mutation)
            .is_err()
    );
    // Error's string field must be UTF-8 even though user data is opaque bytes.
    assert!(ResponseEnvelope::decode(&[8, 1, 0xb2, 1, 3, 10, 1, 255]).is_err());
}

#[test]
fn well_formed_unknown_fields_are_skipped() {
    let mut payload = encode_frame(&Request::Get { key: vec![255] }).unwrap()[4..].to_vec();
    payload.extend_from_slice(&[0xf8, 1, 42]); // Unknown field 31, varint 42.
    assert_eq!(
        RequestEnvelope::decode(&payload).unwrap().request,
        Request::Get { key: vec![255] }
    );
    payload.pop();
    assert!(RequestEnvelope::decode(&payload).is_err());
}

#[test]
fn reads_coalesced_frames_and_clean_eof_without_text_conversion() {
    let first = encode_frame(&Request::Get { key: vec![255, 10] }).unwrap();
    let second = encode_frame(&Request::Delete { key: vec![0] }).unwrap();
    let mut input = Cursor::new([first.clone(), second.clone()].concat());
    assert_eq!(read_frame(&mut input).unwrap().unwrap(), first[4..]);
    assert_eq!(read_frame(&mut input).unwrap().unwrap(), second[4..]);
    assert!(read_frame(&mut input).unwrap().is_none());
}

#[test]
fn reads_fragmented_headers_and_payloads() {
    struct Fragmented(Cursor<Vec<u8>>);
    impl Read for Fragmented {
        fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
            let len = output.len().min(1);
            self.0.read(&mut output[..len])
        }
    }
    let frame = encode_frame(&Request::Get { key: vec![255, 0] }).unwrap();
    let mut input = Fragmented(Cursor::new(frame.clone()));
    assert_eq!(read_frame(&mut input).unwrap().unwrap(), frame[4..]);
}

#[test]
fn rejects_partial_and_oversized_frames_before_reading_the_body() {
    let valid = encode_frame(&Response::Ok).unwrap();
    for end in 1..valid.len() {
        assert!(
            read_frame(&mut Cursor::new(&valid[..end])).is_err(),
            "prefix of length {end}"
        );
    }
    let mut input = Cursor::new([u32::MAX.to_be_bytes().as_slice(), b"untouched"].concat());
    assert_eq!(
        read_frame(&mut input).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(input.position(), 4);
}

#[test]
fn permits_exact_frame_limit_and_rejects_one_more_byte_without_output() {
    let exact = Response::Value {
        value: Some(vec![255; MAX_FRAME - 15]),
    };
    let frame = encode_frame(&exact).unwrap();
    assert_eq!(frame.len(), MAX_FRAME);
    let payload = read_frame(&mut Cursor::new(&frame)).unwrap().unwrap();
    assert_eq!(ResponseEnvelope::decode(&payload).unwrap().response, exact);
    let too_large = Response::Value {
        value: Some(vec![255; MAX_FRAME - 14]),
    };
    let mut output = Vec::new();
    assert!(write_frame(&mut output, &too_large).is_err());
    assert!(output.is_empty());
}

#[test]
fn request_byte_validation_rejects_oversized_key_or_payload_before_wire_cloning() {
    assert!(validate_request_bytes(&vec![0; MAX_KEY_LEN + 1], None).is_err());
    assert!(validate_request_bytes(b"k", Some(&vec![0; MAX_FRAME - 4])).is_err());
    let request = Request::Put {
        key: vec![0; MAX_KEY_LEN + 1],
        value: vec![1],
    };
    let mut output = Vec::new();
    assert!(write_frame(&mut output, &request).is_err());
    assert!(output.is_empty());
}

#[test]
fn ttl_preflight_counts_the_ttl_field_at_the_largest_request_id() {
    let value = vec![255; MAX_FRAME - 28];
    assert!(validate_request_bytes(b"k", Some(&value)).is_ok());
    assert!(validate_put_with_ttl_bytes(b"k", &value, 1).is_err());
}

#[test]
fn worst_case_positive_id_boundary_encodes_without_preflight_cloning() {
    // At this payload size the max-ID PUT envelope has 21 bytes of overhead,
    // plus three bytes for the one-byte key. Its value also fits a GET response.
    let request = RequestEnvelope {
        request_id: u64::MAX,
        request: Request::Put {
            key: b"k".to_vec(),
            value: vec![0; MAX_FRAME - 4 - 21 - 3],
        },
    };
    assert!(
        validate_request_bytes(
            b"k",
            Some(match &request.request {
                Request::Put { value, .. } => value,
                _ => unreachable!(),
            })
        )
        .is_ok()
    );
    assert_eq!(encode_frame(&request).unwrap().len(), MAX_FRAME);
}

#[test]
fn every_preflight_accepted_put_value_fits_a_get_response_at_any_id() {
    // A max-ID PUT with a one-byte key has 28 bytes of total overhead.
    assert!(validate_request_bytes(b"k", Some(&vec![255; MAX_FRAME - 28])).is_ok());
    assert!(validate_request_bytes(b"k", Some(&vec![255; MAX_FRAME - 27])).is_err());
    for len in [MAX_FRAME - 28, MAX_FRAME - 29] {
        let value = vec![255; len];
        assert!(validate_request_bytes(b"k", Some(&value)).is_ok());
        for request_id in [0, 1, 128, u64::MAX] {
            let response = ResponseEnvelope {
                request_id,
                response: Response::Value {
                    value: Some(value.clone()),
                },
            };
            let frame = encode_frame(&response).unwrap_or_else(|error| {
                panic!(
                    "accepted {len}-byte value cannot be returned for GET ID {request_id}: {error}"
                )
            });
            assert!(frame.len() <= MAX_FRAME);
        }
    }
    // Stored values still have their separate max-ID response boundary.
    let largest_value = vec![255; MAX_FRAME - 26];
    assert!(rockbee_protocol::validate_value_bytes(&largest_value).is_ok());
    assert!(rockbee_protocol::validate_value_bytes(&vec![255; MAX_FRAME - 25]).is_err());
    let frame = encode_frame(&ResponseEnvelope {
        request_id: u64::MAX,
        response: Response::Value {
            value: Some(largest_value),
        },
    })
    .unwrap();
    assert_eq!(frame.len(), MAX_FRAME);
}

#[test]
fn put_encoders_reject_a_value_that_cannot_be_returned_at_every_id() {
    let request = Request::Put {
        key: vec![0],
        value: vec![255; MAX_FRAME - 25],
    };
    assert!(validate_request_bytes(b"k", Some(&vec![255; MAX_FRAME - 25])).is_err());
    let mut output = Vec::new();
    assert!(write_frame(&mut output, &request).is_err());
    assert!(output.is_empty());
    for request_id in [0, 1, u64::MAX] {
        assert!(
            write_frame(
                &mut output,
                &RequestEnvelope {
                    request_id,
                    request: &request,
                }
            )
            .is_err()
        );
        assert!(output.is_empty());
    }
}
// An explicit no-side-effects rejection must survive a gateway decode/encode.
// Error.message = "no!", Error.mutation_outcome (tag 2) = REJECTED (1).
#[test]
fn rejection_certainty_survives_wire_decode_and_forwarding() {
    let payload = [
        0x08, 1, 0x10, 7, 0xb2, 1, 7, 0x0a, 3, b'n', b'o', b'!', 0x10, 1,
    ];
    let response = ResponseEnvelope::decode(&payload).unwrap();
    let forwarded = encode_frame(&response).unwrap();
    assert_eq!(&forwarded[4..], payload);
}

#[test]
fn absent_and_unrecognized_mutation_outcomes_never_prove_rejection() {
    use rockbee_protocol::{MutationOutcome, Response, ResponseEnvelope};
    for payload in [
        vec![0x08, 1, 0x10, 7, 0xb2, 1, 0],
        vec![0x08, 1, 0x10, 7, 0xb2, 1, 2, 0x10, 0],
        vec![0x08, 1, 0x10, 7, 0xb2, 1, 2, 0x10, 99],
    ] {
        assert!(matches!(
            ResponseEnvelope::decode(&payload).unwrap().response,
            Response::Error {
                mutation_outcome: MutationOutcome::Unknown,
                ..
            }
        ));
    }
}

#[test]
fn status_is_an_additive_binary_request_and_response_kind() -> rockbee_protocol::Result<()> {
    use rockbee_protocol::{
        ExpectedResponse, Request, RequestEnvelope, Response, ResponseEnvelope, ServiceRole,
        StatusSnapshot, encode_frame,
    };

    let request = RequestEnvelope {
        request_id: 7,
        request: Request::Status,
    };
    let request_frame = encode_frame(&request)?;
    // Envelope request oneof tag 14 is encoded as field key 0x72, then its empty message.
    assert_eq!(&request_frame[4..], &[0x08, 1, 0x10, 7, 0x72, 0]);
    assert_eq!(RequestEnvelope::decode(&request_frame[4..])?, request);

    let snapshot = StatusSnapshot {
        role: ServiceRole::Gateway,
        connections_current: 2,
        connections_accepted_total: 3,
        connections_rejected_total: 5,
        connections_failed_total: 7,
        requests_completed_total: 11,
        error_responses_total: 13,
    };
    let response = ResponseEnvelope {
        request_id: 7,
        response: Response::Status {
            snapshot: snapshot.clone(),
        },
    };
    let response_frame = encode_frame(&response)?;
    // Envelope response oneof tag 23 is encoded as the two-byte field key 0xba 0x01.
    assert!(
        response_frame[4..]
            .windows(2)
            .any(|window| window == [0xba, 0x01])
    );
    assert_eq!(
        ResponseEnvelope::decode(&response_frame[4..])?,
        ResponseEnvelope {
            request_id: 7,
            response: Response::Status {
                snapshot: snapshot.clone(),
            },
        }
    );
    assert_eq!(
        response.validate(ExpectedResponse::Status)?,
        Response::Status { snapshot }
    );
    Ok(())
}

#[test]
fn status_cannot_accept_crud_response_kinds() {
    use rockbee_protocol::{ExpectedResponse, Response, ResponseEnvelope};

    for response in [
        Response::Ok,
        Response::Value { value: None },
        Response::Value {
            value: Some(b"value".to_vec()),
        },
    ] {
        assert!(
            ResponseEnvelope {
                request_id: 1,
                response,
            }
            .validate(ExpectedResponse::Status)
            .is_err()
        );
    }
}

#[test]
fn byte_preflight_rejects_empty_keys_and_put_values() {
    assert!(validate_request_bytes(b"", None).is_err());
    assert!(validate_request_bytes(b"", Some(b"value")).is_err());
    assert!(validate_request_bytes(b"key", Some(b"")).is_err());
    assert!(validate_put_with_ttl_bytes(b"", b"value", 1).is_err());
    assert!(validate_put_with_ttl_bytes(b"key", b"", 1).is_err());
    assert!(rockbee_protocol::validate_value_bytes(b"").is_err());
    assert!(validate_request_bytes(&[0], Some(&[0, 255])).is_ok());
}

#[test]
fn request_encoders_reject_empty_inputs_without_output() {
    for request in [
        Request::Put {
            key: vec![],
            value: vec![1],
        },
        Request::Put {
            key: vec![1],
            value: vec![],
        },
        Request::PutWithTtl {
            key: vec![],
            value: vec![1],
            ttl_millis: 1,
        },
        Request::PutWithTtl {
            key: vec![1],
            value: vec![],
            ttl_millis: 1,
        },
        Request::Get { key: vec![] },
        Request::Delete { key: vec![] },
    ] {
        let mut output = Vec::new();
        assert!(write_frame(&mut output, &request).is_err());
        assert!(output.is_empty());
        for request_id in [0, 7, u64::MAX] {
            assert!(
                write_frame(
                    &mut output,
                    &RequestEnvelope {
                        request_id,
                        request: &request
                    }
                )
                .is_err()
            );
            assert!(output.is_empty());
        }
    }
}

#[test]
fn structurally_valid_empty_requests_keep_ids_for_correlated_rejections() {
    for request in [
        rockbee_protocol::wire::envelope::Message::Put(rockbee_protocol::wire::Put {
            key: vec![],
            value: vec![1],
        }),
        rockbee_protocol::wire::envelope::Message::Put(rockbee_protocol::wire::Put {
            key: vec![1],
            value: vec![],
        }),
        rockbee_protocol::wire::envelope::Message::Get(rockbee_protocol::wire::Get { key: vec![] }),
        rockbee_protocol::wire::envelope::Message::Delete(rockbee_protocol::wire::Delete {
            key: vec![],
        }),
    ] {
        let payload = rockbee_protocol::wire::Envelope {
            version: 1,
            request_id: 42,
            message: Some(request),
        }
        .encode_to_vec();
        assert_eq!(RequestEnvelope::decode(&payload).unwrap().request_id, 42);
    }
}
