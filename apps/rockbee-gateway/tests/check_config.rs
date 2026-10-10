use rockbee_protocol::Result;
use serde_json::Value;
use std::io::Read;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct OwnedProcess(Child);

impl Drop for OwnedProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn check_config_prints_effective_settings_without_binding_or_connecting() -> Result<()> {
    // Occupying the requested listener makes an accidental bind fail. Backend
    // listeners record any accidental connections without relying on DNS/network access.
    let gateway = TcpListener::bind("127.0.0.1:0")?;
    let writer = TcpListener::bind("127.0.0.1:0")?;
    let reader = TcpListener::bind("127.0.0.1:0")?;
    writer.set_nonblocking(true)?;
    reader.set_nonblocking(true)?;
    let address = gateway.local_addr()?.to_string();
    let writer_address = writer.local_addr()?.to_string();
    let reader_address = reader.local_addr()?.to_string();
    let mut process = OwnedProcess(
        Command::new(env!("CARGO_BIN_EXE_rockbee-gateway"))
            .args([
                "--check-config",
                "--addr",
                &address,
                "--writer",
                &writer_address,
                "--reader",
                &reader_address,
                "--upstream-max-in-flight",
                "64",
                "--upstream-max-pending",
                "32",
                "--rpc-max-connections",
                "17",
                "--rpc-global-max-in-flight",
                "8",
                "--rpc-read-timeout-ms",
                "2000",
                "--rpc-write-timeout-ms",
                "3000",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?,
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = process.0.try_wait()? {
            break status;
        }
        if Instant::now() >= deadline {
            return Err("gateway --check-config did not exit".into());
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let mut stdout = String::new();
    let mut stderr = String::new();
    process
        .0
        .stdout
        .take()
        .ok_or("missing stdout")?
        .read_to_string(&mut stdout)?;
    process
        .0
        .stderr
        .take()
        .ok_or("missing stderr")?
        .read_to_string(&mut stderr)?;
    assert!(status.success(), "{status}: {stderr}");
    let effective: Value = serde_json::from_str(&stdout)?;
    assert_eq!(
        effective,
        serde_json::json!({
            "role": "gateway", "addr": address, "writer": writer_address,
            "readers": [reader_address], "check_config": true,
            "upstream": { "max_in_flight": 64, "max_pending": 32 },
            "rpc": {
                "max_connections": 17, "global_max_in_flight": 8,
                "read_timeout_ms": 2000, "write_timeout_ms": 3000,
            },
        })
    );
    for backend in [&writer, &reader] {
        assert_eq!(
            backend.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }
    Ok(())
}
