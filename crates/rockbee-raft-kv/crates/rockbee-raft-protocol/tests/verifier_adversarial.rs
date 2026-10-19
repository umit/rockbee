use bytes::Bytes;
use rockbee_raft_protocol::{
    KvRequest, MAX_KEY_BYTES, MAX_VALUE_BYTES, RequestEnvelope, RequestId, ResponseEnvelope,
    decode_frame, encode_frame,
};

fn frame(payload: &[u8]) -> Vec<u8> {
    let length = u32::try_from(payload.len()).expect("small verifier fixture");
    let mut bytes = length.to_be_bytes().to_vec();
    bytes.extend_from_slice(payload);
    bytes
}

#[test]
fn maximum_put_survives_actual_wire_encoding_and_validation() {
    let request = RequestEnvelope::new(
        RequestId::new(u64::MAX).expect("nonzero ID"),
        60_000,
        KvRequest::Put {
            key: Bytes::from(vec![0xff; MAX_KEY_BYTES]),
            value: Bytes::from(vec![0x80; MAX_VALUE_BYTES]),
        },
    )
    .expect("maximum legal request");
    let encoded = encode_frame(&request).expect("maximum request fits frame");
    let decoded: RequestEnvelope = decode_frame(&encoded).expect("decode maximum request");
    decoded
        .validate()
        .expect("maximum decoded fields stay valid");
    assert_eq!(decoded, request);

    // Each removed byte truncates an actually present value, while the adjusted
    // outer header remains valid. The inner Bytes length must still reject it.
    for removed in [1, 128, MAX_VALUE_BYTES] {
        let truncated = frame(&encoded[4..encoded.len() - removed]);
        assert!(decode_frame::<RequestEnvelope>(&truncated).is_err());
    }
}

#[test]
fn hostile_lengths_and_unknown_variants_in_supported_envelopes_are_rejected() {
    // Unsigned LEB128 u64::MAX: tiny input claiming an unavailable field body.
    let huge_length = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1];
    // Request fields: version=1, id=7, budget=1, PUT=0, key="k".
    let mut put_value = vec![1, 7, 1, 0, 1, b'k'];
    put_value.extend_from_slice(&huge_length);
    assert!(decode_frame::<RequestEnvelope>(&frame(&put_value)).is_err());

    for prefix in [
        vec![1, 7, 0, 1],    // Value(Some(Bytes)).
        vec![1, 7, 1, 3, 4], // Mutation(Unknown(Unavailable, String)).
        vec![1, 7, 1, 2, 4], // Mutation(Rejected(Unavailable, String)).
    ] {
        let mut payload = prefix;
        payload.extend_from_slice(&huge_length);
        assert!(decode_frame::<ResponseEnvelope>(&frame(&payload)).is_err());
    }

    // Unknown operation/response/failure tags cannot become a supported variant.
    assert!(decode_frame::<RequestEnvelope>(&frame(&[1, 7, 1, 3])).is_err());
    assert!(decode_frame::<ResponseEnvelope>(&frame(&[1, 7, 3])).is_err());
    assert!(decode_frame::<ResponseEnvelope>(&frame(&[1, 7, 2, 10])).is_err());
}
