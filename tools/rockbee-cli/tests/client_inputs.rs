//! Empty CLI inputs fail without sending a CRUD request to a real TCP peer.
use rockbee_client::Result;
use std::io::Read;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::time::{sleep, timeout};

struct OwnedChild(Child);

impl Drop for OwnedChild {
    fn drop(&mut self) {
        if matches!(self.0.try_wait(), Ok(Some(_))) {
            return;
        }
        let _ = self.0.kill();
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while !matches!(self.0.try_wait(), Ok(Some(_))) && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

#[tokio::test]
async fn empty_cli_inputs_send_no_request_bytes() -> Result<()> {
    let cases: &[&[&str]] = &[
        &["put", "", "value"],
        &["put", "", "value", "--ttl-ms", "1000"],
        &["get", ""],
        &["delete", ""],
        &["put", "key", ""],
        &["put", "key", "", "--ttl-ms", "1000"],
    ];

    for args in cases {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?.to_string();
        let mut command = Command::new(env!("CARGO_BIN_EXE_rockbee-client"));
        command
            .args(["--addr", address.as_str()])
            .args(*args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        let mut child = OwnedChild(command.spawn()?);
        let status = timeout(Duration::from_secs(10), async {
            loop {
                if let Some(status) = child.0.try_wait()? {
                    return std::io::Result::Ok(status);
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await??;
        assert!(!status.success(), "CLI accepted {args:?}");
        let mut error = Vec::new();
        child
            .0
            .stderr
            .take()
            .expect("stderr configured as pipe")
            .read_to_end(&mut error)?;
        let diagnostic = String::from_utf8_lossy(&error);
        assert!(
            diagnostic.contains("empty"),
            "unexpected CLI error for {args:?}: {diagnostic}"
        );

        // The CLI may validate before connecting in the future. If it connects,
        // the child has exited, so only EOF is acceptable, never request bytes.
        while let Ok(accepted) = timeout(Duration::from_millis(100), listener.accept()).await {
            let (mut stream, _) = accepted?;
            let mut byte = [0_u8; 1];
            let received = timeout(Duration::from_secs(1), stream.read(&mut byte)).await??;
            assert_eq!(received, 0, "CLI sent request bytes for {args:?}");
        }
    }
    Ok(())
}
