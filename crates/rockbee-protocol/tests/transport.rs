use rockbee_protocol::transport::{FrameReader, read_frame, write_frame};
use rockbee_protocol::{MAX_FRAME, Request, Response, ResponseEnvelope, encode_frame};
use std::time::Duration;
use tokio::io::{AsyncWriteExt, duplex};

#[tokio::test]
async fn async_transport_reads_coalesced_binary_frames_and_clean_eof() {
    let (mut sender, receiver) = duplex(128);
    let first = encode_frame(&Request::Get { key: vec![255, 10] }).unwrap();
    let second = encode_frame(&Request::Delete { key: vec![0] }).unwrap();
    sender
        .write_all(&[first.clone(), second.clone()].concat())
        .await
        .unwrap();
    drop(sender);
    let mut reader = FrameReader::new(receiver);
    assert_eq!(read_frame(&mut reader).await.unwrap().unwrap(), first[4..]);
    assert_eq!(read_frame(&mut reader).await.unwrap().unwrap(), second[4..]);
    assert!(read_frame(&mut reader).await.unwrap().is_none());
}

#[tokio::test]
async fn canceled_read_preserves_partial_header_and_payload_on_same_reader() {
    for split in [2, 6] {
        let (mut sender, receiver) = duplex(128);
        let packet = encode_frame(&Response::Value {
            value: Some(vec![255, 0, 10]),
        })
        .unwrap();
        let mut reader = FrameReader::new(receiver);
        sender.write_all(&packet[..split]).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), read_frame(&mut reader))
                .await
                .is_err()
        );
        sender.write_all(&packet[split..]).await.unwrap();
        assert_eq!(read_frame(&mut reader).await.unwrap().unwrap(), packet[4..]);
    }
}

#[tokio::test]
async fn async_transport_rejects_incomplete_header_body_and_oversized_length() {
    let valid = encode_frame(&Response::Ok).unwrap();
    for data in [
        valid[..2].to_vec(),
        valid[..6].to_vec(),
        u32::MAX.to_be_bytes().to_vec(),
    ] {
        let (mut sender, receiver) = duplex(128);
        sender.write_all(&data).await.unwrap();
        drop(sender);
        assert!(read_frame(&mut FrameReader::new(receiver)).await.is_err());
    }
}

#[tokio::test]
async fn async_transport_writes_binary_values_and_rejects_oversized_output() {
    let (mut sender, receiver) = duplex(128);
    let response = Response::Value {
        value: Some(vec![255, 0, 10]),
    };
    write_frame(&mut sender, &response).await.unwrap();
    let mut reader = FrameReader::new(receiver);
    let payload = read_frame(&mut reader).await.unwrap().unwrap();
    assert_eq!(
        ResponseEnvelope::decode(&payload).unwrap().response,
        response
    );
    assert!(
        write_frame(
            &mut sender,
            &Response::Value {
                value: Some(vec![0; MAX_FRAME])
            }
        )
        .await
        .is_err()
    );
}
