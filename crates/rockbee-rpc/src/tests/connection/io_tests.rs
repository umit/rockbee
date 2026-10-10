use super::*;
use rockbee_protocol::encode_frame;
use rockbee_protocol::transport::FrameReader;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;

fn config() -> RpcConfig {
    RpcConfig::new(1, Duration::from_millis(100), Duration::from_millis(80)).unwrap()
}

fn frame(id: u64) -> Vec<u8> {
    encode_frame(&RequestEnvelope {
        request_id: id,
        request: Request::Get { key: b"k".to_vec() },
    })
    .unwrap()
}

#[tokio::test]
async fn client_reconnects_after_idle_timeout_releases_its_slot() -> Result<()> {
    let rpc = RpcServer::bind_with_config("127.0.0.1:0", config()).await?;
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
    let client = rockbee_client::Client::connect(&address.to_string()).await?;
    assert_eq!(client.get(b"before-idle").await?, None);
    tokio::time::sleep(Duration::from_millis(160)).await;
    // With a one-socket cap, another accepted GET proves that the first
    // connection was retired, rather than merely left idle and usable.
    let stream = TcpStream::connect(address).await?;
    let (mut input, mut output) = stream.into_split();
    output.write_all(&frame(1)).await?;
    let reply = timeout(
        Duration::from_secs(1),
        read_frame(&mut FrameReader::new(&mut input)),
    )
    .await??
    .ok_or("idle connection still occupied the only socket slot")?;
    assert_eq!(
        ResponseEnvelope::decode(&reply)?.response,
        Response::Value { value: None }
    );
    assert!(
        matches!(timeout(Duration::from_secs(1), input.read_u8()).await,
        Ok(Err(e)) if matches!(e.kind(), std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset))
    );
    assert_eq!(client.get(b"after-idle").await?, None);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    stop.send(()).unwrap();
    server.await??;
    Ok(())
}

#[tokio::test]
async fn partial_frames_use_one_absolute_budget_in_both_protocol_modes() -> Result<()> {
    for id in [0, 1] {
        let rpc = RpcServer::bind_with_config(
            "127.0.0.1:0",
            RpcConfig::new(1, Duration::from_millis(400), Duration::from_millis(80))?,
        )
        .await?;
        let address = rpc.local_addr()?;
        let calls = Arc::new(AtomicUsize::new(0));
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(rpc.run_until(
            {
                let calls = calls.clone();
                move |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    async { Ok(Response::Ok) }
                }
            },
            async {
                stopped.await?;
                Ok(())
            },
        ));
        let stream = TcpStream::connect(address).await?;
        let (mut input, mut output) = stream.into_split();
        let first = frame(id);
        output.write_all(&first).await?;
        let mut framed = FrameReader::new(&mut input);
        assert!(
            timeout(Duration::from_secs(1), read_frame(&mut framed))
                .await??
                .is_some()
        );
        drop(framed);
        let packet = frame(if id == 0 { 0 } else { 2 });
        output.write_all(&packet[..2]).await?;
        tokio::time::sleep(Duration::from_millis(150)).await;
        output.write_all(&packet[2..4]).await?;
        tokio::time::sleep(Duration::from_millis(150)).await;
        output.write_all(&packet[4..6]).await?;
        assert!(
            matches!(timeout(Duration::from_millis(220), input.read_u8()).await, Ok(Err(e)) if matches!(e.kind(), std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset))
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "id {id} partial frame reached handler"
        );
        stop.send(()).unwrap();
        server.await??;
    }
    Ok(())
}

#[tokio::test]
async fn write_budget_is_not_a_handler_budget() -> Result<()> {
    for id in [0, 1] {
        let rpc = RpcServer::bind_with_config(
            "127.0.0.1:0",
            RpcConfig::new(1, Duration::from_millis(250), Duration::from_millis(80))?,
        )
        .await?;
        let address = rpc.local_addr()?;
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(rpc.run_until(
            move |_| async {
                tokio::time::sleep(Duration::from_millis(130)).await;
                Ok(Response::Ok)
            },
            async {
                stopped.await?;
                Ok(())
            },
        ));
        let stream = TcpStream::connect(address).await?;
        let (input, mut output) = stream.into_split();
        output.write_all(&frame(id)).await?;
        let mut input = FrameReader::new(input);
        assert!(
            timeout(Duration::from_secs(1), read_frame(&mut input))
                .await??
                .is_some()
        );
        stop.send(()).unwrap();
        server.await??;
    }
    Ok(())
}

#[tokio::test]
async fn stalled_duplex_write_times_out_without_leaving_a_reusable_frame() -> Result<()> {
    let (mut writer, mut reader) = tokio::io::duplex(1);
    let response = Response::Value {
        value: Some(vec![7; 4096]),
    };
    assert!(matches!(
        timeout(
            Duration::from_secs(1),
            write_with_timeout(&mut writer, &response, Duration::from_millis(80))
        )
        .await,
        Ok(Err(_))
    ));
    let mut bytes = vec![0; 16];
    let count = timeout(Duration::from_secs(1), reader.read(&mut bytes)).await??;
    assert!(count > 0, "test must observe blocked partial output");
    assert!(
        timeout(
            Duration::from_millis(80),
            read_frame(&mut FrameReader::new(reader))
        )
        .await
        .is_err(),
        "partial canceled write must not decode as a valid next frame"
    );
    Ok(())
}
