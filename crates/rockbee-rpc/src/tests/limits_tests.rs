use super::*;
use rockbee_protocol::encode_frame;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;

#[tokio::test]
async fn listener_rejects_the_257th_live_connection_without_executing_it() -> Result<()> {
    let rpc = RpcServer::bind("127.0.0.1:0").await?;
    let address = rpc.local_addr()?;
    let calls = Arc::new(AtomicUsize::new(0));
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(rpc.run_until(
        {
            let calls = calls.clone();
            move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(Response::Value { value: None }) }
            }
        },
        async {
            stopped.await?;
            Ok(())
        },
    ));
    let frame = encode_frame(&RequestEnvelope {
        request_id: 1,
        request: Request::Get { key: b"k".to_vec() },
    })?;
    let mut sockets = Vec::new();
    for _ in 0..256 {
        let stream = TcpStream::connect(address).await?;
        let (input, mut output) = stream.into_split();
        output.write_all(&frame).await?;
        let mut input = FrameReader::new(input);
        let reply = timeout(Duration::from_secs(1), read_frame(&mut input))
            .await??
            .unwrap();
        assert_eq!(ResponseEnvelope::decode(&reply)?.request_id, 1);
        sockets.push((input, output));
    }
    let mut overflow = TcpStream::connect(address).await?;
    let closed = timeout(Duration::from_millis(300), overflow.read_u8()).await;
    stop.send(()).unwrap();
    server.await??;
    drop(sockets);
    assert!(
        matches!(closed, Ok(Err(ref error)) if matches!(error.kind(),
        std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset)),
        "the listener retained a connection beyond its process-wide count limit: {closed:?}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 256);
    Ok(())
}
