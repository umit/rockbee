use bytes::Bytes;
use rockbee_raft_protocol::{
    ClusterId, Failure, FailureReason, GroupId, HASH_VERSION, KvRequest, KvResponse,
    MutationOutcome, NodeId, ReadFailure, RequestEnvelope, RequestId, ResponseEnvelope,
    ShardConfig, VERSION, decode_frame, encode_frame,
};
use serde::Serialize;

fn request(request: KvRequest) -> RequestEnvelope {
    RequestEnvelope::new(
        RequestId::new(7).expect("nonzero fixture ID"),
        1_000,
        request,
    )
    .expect("valid request fixture")
}

fn response(response: KvResponse) -> ResponseEnvelope {
    ResponseEnvelope {
        version: VERSION,
        request_id: RequestId::new(7).expect("nonzero fixture ID"),
        response,
    }
}

fn failure() -> Failure {
    Failure::new(FailureReason::Unavailable, "service unavailable")
        .expect("short diagnostic fixture")
}

#[test]
fn invalid_identifiers_cannot_be_constructed_or_deserialized() {
    assert!(RequestId::new(0).is_err());
    assert!(NodeId::new(0).is_err());
    assert!(ClusterId::new([0; 16]).is_err());
    assert!(decode_frame::<RequestId>(&encode_frame(&0_u64).unwrap()).is_err());
    assert!(decode_frame::<NodeId>(&encode_frame(&0_u64).unwrap()).is_err());
    assert!(decode_frame::<ClusterId>(&encode_frame(&[0_u8; 16]).unwrap()).is_err());
    let id = RequestId::new(u64::MAX).unwrap();
    assert_eq!(
        decode_frame::<RequestId>(&encode_frame(&id).unwrap())
            .unwrap()
            .get(),
        u64::MAX
    );
    assert_eq!(NodeId::new(1).unwrap().get(), 1);
    assert_eq!(ClusterId::new([1; 16]).unwrap().get(), [1; 16]);
    assert_eq!(GroupId::new(0).get(), 0);
    assert!(GroupId::new(0).is_metadata());
}

#[test]
fn shard_mapping_matches_independent_fixed_fnv_vectors() {
    // Published FNV-1a64 vectors: empty cbf29ce484222325, a af63dc4c8601ec8c,
    // hello a430d84680aabd0b, foobar 85944171f73967e8. Modulo 256 + 1.
    let config = ShardConfig::new(256, 0).unwrap();
    for (key, expected) in [(b"a".as_slice(), 141), (b"hello", 12), (b"foobar", 233)] {
        assert_eq!(config.group_for_key(key).unwrap(), GroupId::new(expected));
    }
    // Independently computed seeded/binary vectors, modulo 17 + 1.
    let config = ShardConfig::new(17, 42).unwrap();
    assert_eq!(config.group_for_key(b"hello").unwrap(), GroupId::new(11));
    assert_eq!(config.group_for_key(&[0, 255, 0]).unwrap(), GroupId::new(9));
    // Compatibility boundary: a nonzero maximum seed and the largest legal
    // opaque key must retain its v1 placement after implementation changes.
    let config = ShardConfig::new(256, u64::MAX).unwrap();
    assert_eq!(
        config.group_for_key(&vec![0xa5; 65_536]).unwrap(),
        GroupId::new(219)
    );
    assert!(config.group_for_key(b"").is_err());
    assert!(ShardConfig::new(0, 0).is_err());
    assert!(ShardConfig::new(257, 0).is_err());
    #[derive(Serialize)]
    struct Raw {
        version: u8,
        count: u16,
        seed: u64,
    }
    for raw in [
        Raw {
            version: HASH_VERSION + 1,
            count: 1,
            seed: 0,
        },
        Raw {
            version: HASH_VERSION,
            count: 0,
            seed: 0,
        },
    ] {
        assert!(decode_frame::<ShardConfig>(&encode_frame(&raw).unwrap()).is_err());
    }
}

#[test]
fn requests_enforce_exact_field_and_budget_boundaries() {
    for budget in [1, 60_000] {
        assert!(
            RequestEnvelope::new(
                RequestId::new(1).unwrap(),
                budget,
                KvRequest::Put {
                    key: Bytes::from(vec![0; 65_536]),
                    value: Bytes::from(vec![0; 1_048_576])
                }
            )
            .is_ok()
        );
    }
    for budget in [0, 60_001, u64::MAX] {
        assert!(
            RequestEnvelope::new(
                RequestId::new(1).unwrap(),
                budget,
                KvRequest::Get {
                    key: Bytes::from_static(b"k")
                }
            )
            .is_err()
        );
    }
    for req in [
        KvRequest::Get { key: Bytes::new() },
        KvRequest::Delete {
            key: Bytes::from(vec![1; 65_537]),
        },
        KvRequest::Put {
            key: Bytes::from_static(b"k"),
            value: Bytes::from(vec![0; 1_048_577]),
        },
    ] {
        assert!(RequestEnvelope::new(RequestId::new(1).unwrap(), 1, req).is_err());
    }
    let mut req = request(KvRequest::Get {
        key: Bytes::from_static(b"k"),
    });
    req.version = VERSION + 1;
    assert!(req.validate().is_err());
    req.version = VERSION;
    req.budget_ms = 0;
    let decoded: RequestEnvelope = decode_frame(&encode_frame(&req).unwrap()).unwrap();
    assert!(decoded.validate().is_err());
}

#[test]
fn opaque_bytes_and_empty_value_round_trip() {
    for req in [
        KvRequest::Put {
            key: Bytes::from_static(&[0, 255, 128]),
            value: Bytes::new(),
        },
        KvRequest::Put {
            key: Bytes::from_static(b"k"),
            value: Bytes::from_static(&[255, 0, 128]),
        },
        KvRequest::Delete {
            key: Bytes::from_static(&[0]),
        },
    ] {
        let req = request(req);
        let decoded: RequestEnvelope = decode_frame(&encode_frame(&req).unwrap()).unwrap();
        decoded.validate().unwrap();
        assert_eq!(decoded, req);
    }
}

#[test]
fn responses_correlate_ids_versions_and_operation_kinds() {
    let get = request(KvRequest::Get {
        key: Bytes::from_static(b"k"),
    });
    let put = request(KvRequest::Put {
        key: Bytes::from_static(b"k"),
        value: Bytes::new(),
    });
    let delete = request(KvRequest::Delete {
        key: Bytes::from_static(b"k"),
    });
    for rsp in [
        KvResponse::Value(None),
        KvResponse::Value(Some(Bytes::new())),
        KvResponse::Value(Some(Bytes::from(vec![1; 1_048_576]))),
        KvResponse::ReadError(ReadFailure::new(failure())),
    ] {
        let rsp = response(rsp);
        let decoded: ResponseEnvelope = decode_frame(&encode_frame(&rsp).unwrap()).unwrap();
        decoded.validate_for(&get).unwrap();
        assert!(decoded.validate_for(&put).is_err());
        assert!(decoded.validate_for(&delete).is_err());
        assert_eq!(decoded, rsp);
    }
    for outcome in [
        MutationOutcome::Committed,
        MutationOutcome::Rejected(failure()),
        MutationOutcome::Unknown(failure()),
    ] {
        let rsp = response(KvResponse::Mutation(outcome));
        let decoded: ResponseEnvelope = decode_frame(&encode_frame(&rsp).unwrap()).unwrap();
        decoded.validate_for(&put).unwrap();
        decoded.validate_for(&delete).unwrap();
        assert!(decoded.validate_for(&get).is_err());
        assert_eq!(decoded, rsp);
    }
    let local = response(KvResponse::Mutation(MutationOutcome::NotSent(failure())));
    let remote: ResponseEnvelope = decode_frame(&encode_frame(&local).unwrap()).unwrap();
    assert!(remote.validate_for(&put).is_err());
    let mut rsp = response(KvResponse::Value(None));
    rsp.request_id = RequestId::new(8).unwrap();
    assert!(rsp.validate_for(&get).is_err());
    rsp.request_id = get.request_id;
    rsp.version = VERSION + 1;
    assert!(rsp.validate_for(&get).is_err());
    assert!(
        response(KvResponse::Value(Some(Bytes::from(vec![0; 1_048_577]))))
            .validate_for(&get)
            .is_err()
    );
}

#[test]
fn failure_messages_are_bounded_by_utf8_bytes_even_after_decode() {
    assert!(Failure::new(FailureReason::Internal, "é".repeat(512)).is_ok());
    assert!(Failure::new(FailureReason::Internal, "é".repeat(513)).is_err());
    let put = request(KvRequest::Put {
        key: Bytes::from_static(b"k"),
        value: Bytes::new(),
    });
    let invalid = Failure {
        reason: FailureReason::Internal,
        message: "x".repeat(1025),
    };
    let rsp = response(KvResponse::Mutation(MutationOutcome::Unknown(invalid)));
    let decoded: ResponseEnvelope = decode_frame(&encode_frame(&rsp).unwrap()).unwrap();
    assert!(decoded.validate_for(&put).is_err());
}

#[test]
fn frame_codec_rejects_bad_lengths_truncation_and_trailing_data() {
    assert!(decode_frame::<u8>(&[]).is_err());
    assert!(decode_frame::<u8>(&[0, 0, 0, 0]).is_err());
    assert!(decode_frame::<u8>(&[0, 32, 0, 1]).is_err());
    assert!(decode_frame::<u8>(&[0, 0, 0, 2, 1]).is_err());
    assert!(decode_frame::<u8>(&[0, 0, 0, 1, 1, 2]).is_err());
    assert!(decode_frame::<u8>(&[0, 0, 0, 2, 1, 2]).is_err());
    let frame = encode_frame(&42_u8).unwrap();
    assert_eq!(frame, [0, 0, 0, 1, 42]);
    assert_eq!(decode_frame::<u8>(&frame).unwrap(), 42);
}

#[test]
fn frame_serialization_has_a_hard_payload_bound() {
    assert!(encode_frame(&42_u8).unwrap().capacity() <= 64);
    let maximum = Bytes::from(vec![7; 2_097_149]); // three-byte postcard length prefix
    let frame = encode_frame(&maximum).unwrap();
    assert_eq!(frame.len(), 2_097_156);
    assert_eq!(decode_frame::<Bytes>(&frame).unwrap(), maximum);
    assert!(encode_frame(&Bytes::from(vec![7; 2_097_150])).is_err());
    struct Unbounded;
    impl Serialize for Unbounded {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            use serde::ser::SerializeSeq;
            let mut seq = serializer.serialize_seq(Some(usize::MAX))?;
            loop {
                seq.serialize_element(&1_u8)?;
            }
        }
    }
    assert!(encode_frame(&Unbounded).is_err());
}

#[test]
fn tiny_frames_cannot_request_huge_collection_allocations() {
    // usize::MAX unsigned LEB128 on 64-bit hosts, followed by no elements.
    let mut frame = vec![0, 0, 0, 10];
    frame.extend_from_slice(&[255, 255, 255, 255, 255, 255, 255, 255, 255, 1]);
    assert!(decode_frame::<Bytes>(&frame).is_err());
    assert!(decode_frame::<String>(&frame).is_err());
    // Actual request: version=1, id=7, budget=1, Get variant=1, then malicious key length.
    let mut request_payload = vec![1, 7, 1, 1];
    request_payload.extend_from_slice(&frame[4..]);
    let mut request_frame = vec![0, 0, 0, 14];
    request_frame.extend_from_slice(&request_payload);
    assert!(decode_frame::<RequestEnvelope>(&request_frame).is_err());
    // Actual response: version=1, id=7, ReadError variant=2,
    // Unavailable reason=4, then malicious String message length.
    let mut response_frame = vec![0, 0, 0, 14, 1, 7, 2, 4];
    response_frame.extend_from_slice(&frame[4..]);
    assert!(decode_frame::<ResponseEnvelope>(&response_frame).is_err());
}
