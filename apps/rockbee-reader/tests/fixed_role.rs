use std::process::Command;

fn binary() -> Command {
    Command::new(env!("CARGO_BIN_EXE_rockbee-reader"))
}

struct OwnedChild(std::process::Child);

impl OwnedChild {
    fn new(child: std::process::Child) -> Self {
        Self(child)
    }

    fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        self.0.try_wait()
    }

    fn stop(&mut self) -> std::io::Result<()> {
        match self.0.try_wait()? {
            Some(_) => Ok(()),
            None => {
                let _ = self.0.kill();
                self.0.wait().map(|_| ())
            }
        }
    }
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

#[test]
fn help_names_the_fixed_binary() {
    let output = binary().arg("--help").output().unwrap();
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("Usage: rockbee-reader"));
}

#[test]
fn malformed_args_name_the_fixed_binary() {
    let output = binary().arg("--unknown").output().unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("Usage: rockbee-reader"));
}

#[test]
fn check_config_has_the_fixed_role_and_default_address() {
    let output = binary().arg("--check-config").output().unwrap();
    assert!(output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["role"], "reader");
    assert_eq!(value["addr"], "127.0.0.1:7102");
}

#[test]
fn conflicting_cli_role_fails_before_any_storage_open() {
    let temp = tempfile::tempdir().unwrap();
    let output = binary()
        .args(["--role", "writer", "--backend", "file", "--directory"])
        .arg(temp.path())
        .arg("--check-config")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(!temp.path().join("rockbee").exists());
}

#[test]
fn conflicting_toml_role_fails_even_when_cli_claims_the_expected_role() {
    let temp = tempfile::tempdir().unwrap();
    let config = temp.path().join("node.toml");
    std::fs::write(
        &config,
        "role = \"writer\"\nbackend = \"file\"\ndirectory = \"objects\"\n",
    )
    .unwrap();
    let output = binary()
        .args(["--config"])
        .arg(&config)
        .args(["--role", "reader", "--check-config"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(!temp.path().join("objects").exists());
}

#[tokio::test]
async fn invalid_reader_process_cannot_fence_live_writer_and_valid_reader_rejects_mutations()
-> rockbee_test_support::Result<()> {
    use rockbee_client::RockBeeClient;
    use rockbee_test_support::backend::Backend;
    use rockbee_test_support::storage::Role;
    use rockbee_test_support::{Server, Store};
    use std::process::Stdio;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::{Instant, sleep};

    let temp = tempfile::tempdir()?;
    let prefix = "fixed-reader-process";
    let objects = Backend::File {
        directory: temp.path().join("objects"),
    }
    .connect()?;
    let writer_store = Store::open(Role::Writer, prefix, Arc::clone(&objects)).await?;
    let writer_server = Server::bind("127.0.0.1:0", writer_store).await?;
    let writer_address = writer_server.local_addr()?.to_string();
    let (stop_writer, writer_stopped) = tokio::sync::oneshot::channel();
    let writer_task = tokio::spawn(writer_server.run_until(async move {
        let _ = writer_stopped.await;
        Ok(())
    }));

    let wrong = temp.path().join("wrong.toml");
    std::fs::write(
        &wrong,
        format!(
            "role = \"writer\"\nbackend = \"file\"\ndirectory = \"{}\"\nprefix = \"{prefix}\"\n",
            temp.path().join("objects").display()
        ),
    )?;
    let mut rejected = OwnedChild::new(
        binary()
            .args(["--config"])
            .arg(&wrong)
            .stderr(Stdio::null())
            .spawn()?,
    );
    // This is a role-safety test, not a process-startup benchmark. A freshly
    // linked macOS binary took 1.9s to start under parallel launches, leaving
    // the former 2s bound vulnerable to loader and scheduler variation.
    let reject_deadline = Instant::now() + Duration::from_secs(10);
    let rejection = loop {
        match rejected.try_wait()? {
            Some(status) => break status,
            None if Instant::now() < reject_deadline => sleep(Duration::from_millis(10)).await,
            None => return Err("mismatched reader config did not exit within ten seconds".into()),
        }
    };
    assert!(!rejection.success());

    let writer = RockBeeClient::connect(&writer_address).await?;
    writer.put(b"still-writer", b"durable", None).await?;

    let reader_address = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        listener.local_addr()?.to_string()
    };
    let mut reader = OwnedChild::new(
        binary()
            .args([
                "--addr",
                &reader_address,
                "--backend",
                "file",
                "--directory",
            ])
            .arg(temp.path().join("objects"))
            .args(["--prefix", prefix])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    let reader_client = loop {
        match RockBeeClient::connect(&reader_address).await {
            Ok(client) => break client,
            Err(error) if Instant::now() < deadline => {
                let _ = error;
                sleep(Duration::from_millis(25)).await;
            }
            Err(error) => {
                return Err(error);
            }
        }
    };
    let observed = loop {
        if reader_client.get(b"still-writer").await?.as_deref() == Some(&b"durable"[..]) {
            break true;
        }
        if Instant::now() >= deadline {
            break false;
        }
        sleep(Duration::from_millis(25)).await;
    };
    assert!(observed, "reader did not converge within test budget");
    let put_error = reader_client
        .put(b"denied", b"value", None)
        .await
        .expect_err("reader PUT must be rejected");
    assert!(put_error.to_string().contains("read-only"), "{put_error}");
    let delete_error = reader_client
        .delete(b"still-writer")
        .await
        .expect_err("reader DELETE must be rejected");
    assert!(
        delete_error.to_string().contains("read-only"),
        "{delete_error}"
    );
    assert_eq!(
        writer.get(b"still-writer").await?.as_deref(),
        Some(&b"durable"[..])
    );
    assert_eq!(writer.get(b"denied").await?, None);

    reader.stop()?;
    stop_writer
        .send(())
        .map_err(|_| "writer stop receiver dropped")?;
    writer_task.await??;
    Ok(())
}

#[test]
fn query_check_config_resolves_limits_without_creating_storage() {
    let temp = tempfile::tempdir().unwrap();
    let directory = temp.path().join("must-not-open");
    let config = temp.path().join("query.toml");
    let body = format!(
        "backend = 'file'\ndirectory = '{}'\n[query]\ntimeout_ms = 1250\nmemory_limit_mb = 512\nmax_concurrent = 4\n",
        directory.display()
    );
    std::fs::write(&config, &body).unwrap();
    let output = binary()
        .arg("--config")
        .arg(&config)
        .arg("--check-config")
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let effective: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        effective["query"],
        serde_json::json!({"timeout_ms":1250,"memory_limit_mb":512,"max_concurrent":4})
    );
    assert!(!directory.exists());
    std::fs::write(&config, body.replace("timeout_ms = 1250", "timeout_ms = 0")).unwrap();
    let rejected = binary()
        .arg("--config")
        .arg(&config)
        .arg("--check-config")
        .output()
        .unwrap();
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("query.timeout_ms"));
    assert!(!directory.exists());
}
