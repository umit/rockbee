//! Fixed wire contracts, derived from the v1 field order and postcard encoding.

use bytes::Bytes;
use rockbee_raft_protocol::{
    Failure, FailureReason, KvRequest, KvResponse, MutationOutcome, RequestEnvelope, RequestId,
    ResponseEnvelope, VERSION, decode_frame, encode_frame,
};

fn request(operation: KvRequest) -> RequestEnvelope {
    RequestEnvelope::new(
        RequestId::new(7).expect("nonzero fixture ID"),
        1_000,
        operation,
    )
    .expect("valid request fixture")
}

fn check_response(response: KvResponse, request: &RequestEnvelope, expected: &[u8]) {
    let envelope = ResponseEnvelope {
        version: VERSION,
        request_id: request.request_id,
        response,
    };
    assert_eq!(
        encode_frame(&envelope).expect("encode response fixture"),
        expected
    );
    let decoded: ResponseEnvelope = decode_frame(expected).expect("decode response fixture");
    assert_eq!(decoded, envelope);
    decoded
        .validate_for(request)
        .expect("valid response fixture");
}

#[test]
fn put_request_keeps_v1_wire_bytes() {
    let envelope = request(KvRequest::Put {
        key: Bytes::from_static(&[0, 0xff]),
        value: Bytes::from_static(&[0x80, 0]),
    });
    // Header=11; version=1, ID=7, budget=1000 (LEB128 e8 07), PUT=0;
    // each opaque field has a one-byte length followed by its exact bytes.
    let expected = [0, 0, 0, 11, 1, 7, 0xe8, 7, 0, 2, 0, 0xff, 2, 0x80, 0];
    assert_eq!(
        encode_frame(&envelope).expect("encode PUT fixture"),
        expected
    );
    let decoded: RequestEnvelope = decode_frame(&expected).expect("decode PUT fixture");
    assert_eq!(decoded, envelope);
    decoded.validate().expect("valid PUT fixture");
}

#[test]
fn committed_response_keeps_v1_wire_bytes() {
    let request = request(KvRequest::Delete {
        key: Bytes::from_static(b"k"),
    });
    // Header=4; version=1, ID=7, Mutation=1, Committed=0.
    check_response(
        KvResponse::Mutation(MutationOutcome::Committed),
        &request,
        &[0, 0, 0, 4, 1, 7, 1, 0],
    );
}

#[test]
fn value_response_keeps_v1_wire_bytes() {
    let request = request(KvRequest::Get {
        key: Bytes::from_static(b"k"),
    });
    // Header=7; version=1, ID=7, Value=0, Some=1, length=2, opaque bytes.
    check_response(
        KvResponse::Value(Some(Bytes::from_static(&[0, 0xff]))),
        &request,
        &[0, 0, 0, 7, 1, 7, 0, 1, 2, 0, 0xff],
    );
}

#[test]
fn unknown_response_keeps_v1_wire_bytes() {
    let request = request(KvRequest::Delete {
        key: Bytes::from_static(b"k"),
    });
    // Header=8; version=1, ID=7, Mutation=1, Unknown=3,
    // Unavailable=4, UTF-8 message length=2, message="no".
    check_response(
        KvResponse::Mutation(MutationOutcome::Unknown(
            Failure::new(FailureReason::Unavailable, "no").expect("short diagnostic fixture"),
        )),
        &request,
        &[0, 0, 0, 8, 1, 7, 1, 3, 4, 2, b'n', b'o'],
    );
}
