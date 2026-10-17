use rockbee_client::RockBeeClient;
use rockbee_test_support::Result;
use serde_json::Value;
use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct OwnedServer(Child);

impl Drop for OwnedServer {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn command(config: &std::path::Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_rockbee-writer"));
    command.arg("--config").arg(config).stdin(Stdio::null());
    command
}

fn wait_for_exit(child: &mut Child) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    while child.try_wait()?.is_none() {
        if Instant::now() >= deadline {
            return Err("config command did not exit within ten seconds".into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    Ok(())
}

#[test]
fn check_config_exits_without_binding_or_creating_the_storage_directory() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let objects = directory.path().join("must-not-be-created");
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let config = directory.path().join("server.toml");
    std::fs::write(
        &config,
        format!(
            "role = 'writer'\naddr = '{}'\nbackend = 'file'\ndirectory = '{}'\n\
             [memory]\nblock_cache_bytes = 8388608\nmetadata_cache_bytes = 2097152\n\
             block_cache_shards = 2\nmetadata_cache_shards = 1\n",
            listener.local_addr()?,
            objects.display()
        ),
    )?;
    let mut child = OwnedServer(
        command(&config)
            .arg("--check-config")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?,
    );
    wait_for_exit(&mut child.0)?;
    let output = child.0.try_wait()?.ok_or("missing exit status")?;
    let mut stdout = String::new();
    std::io::Read::read_to_string(
        &mut child.0.stdout.take().ok_or("missing stdout")?,
        &mut stdout,
    )?;
    assert!(output.success(), "check-config failed: {output}, {stdout}");
    let effective: Value = serde_json::from_str(&stdout)?;
    assert_eq!(effective["memory"]["block_cache_bytes"], 8_388_608);
    assert_eq!(effective["memory"]["metadata_cache_shards"], 1);
    assert!(!objects.exists(), "check-config opened file storage");
    Ok(())
}

#[tokio::test]
async fn invalid_programmatic_memory_config_fails_before_binding_or_opening_storage() -> Result<()>
{
    let directory = tempfile::tempdir()?;
    let objects = directory.path().join("must-not-be-created");
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let mut config = rockbee_test_support::config::Config::parse([
        "--backend".to_owned(),
        "file".to_owned(),
        "--directory".to_owned(),
        objects.display().to_string(),
        "--addr".to_owned(),
        listener.local_addr()?.to_string(),
    ])?
    .ok_or("missing config")?;
    config.memory.block_cache_bytes = 0;
    let error = match rockbee_test_support::Server::from_config(config).await {
        Ok(_) => return Err("invalid config opened a server".into()),
        Err(error) => error,
    };
    assert!(error.to_string().contains("block_cache_bytes"), "{error}");
    assert!(!objects.exists());
    Ok(())
}

#[tokio::test]
async fn configured_server_announces_budgets_and_serves_binary_crud() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let config = directory.path().join("server.toml");
    std::fs::write(
        &config,
        format!(
            "role = 'writer'\naddr = '127.0.0.1:0'\nbackend = 'file'\ndirectory = '{}'\n\
             [memory]\nblock_cache_bytes = 8388608\nmetadata_cache_bytes = 2097152\n\
             writer_max_unflushed_bytes = 134217728\nblock_cache_shards = 2\nmetadata_cache_shards = 1\n",
            directory.path().join("objects").display()
        ),
    )?;
    let mut child = OwnedServer(
        command(&config)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?,
    );
    let stdout = child.0.stdout.take().ok_or("missing stdout")?;
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let mut line = String::new();
        let read = BufReader::new(stdout).read_line(&mut line).map(|_| line);
        let _ = tx.send(read);
    });
    let line = rx.recv_timeout(Duration::from_secs(10))??;
    let ready: Value = serde_json::from_str(&line)?;
    assert_eq!(ready["event"], "ready");
    assert_eq!(ready["memory"]["writer_max_unflushed_bytes"], 134_217_728);
    assert_eq!(ready["memory"]["block_cache_bytes"], 8_388_608);
    let client = RockBeeClient::connect(ready["addr"].as_str().ok_or("missing address")?).await?;
    let key = b"\x00memory\xff";
    let value = b"\x00opaque\xfe";
    client.put(key, value, None).await?;
    assert_eq!(client.get(key).await?, Some(value.to_vec()));
    client.delete(key).await?;
    assert_eq!(client.get(key).await?, None);
    Ok(())
}
