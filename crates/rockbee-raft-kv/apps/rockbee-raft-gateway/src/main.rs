//! Thin process boundary for the Rockbee Raft gateway library.

use std::process::ExitCode;

use rockbee_raft_gateway::GatewayRuntime;

mod cli;

#[tokio::main]
async fn main() -> ExitCode {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_micros()
        .init();
    let config = match cli::arguments(std::env::args().skip(1)) {
        Ok(Some(config)) => config,
        Ok(None) => {
            println!("{}", cli::USAGE);
            return ExitCode::SUCCESS;
        }
        Err(error) => {
            eprintln!("{error}\n\n{}", cli::USAGE);
            return ExitCode::FAILURE;
        }
    };
    // Install signal handlers before opening listeners or contacting seeds, so
    // every owned runtime resource has one orderly shutdown path.
    let mut shutdown = match Shutdown::install() {
        Ok(shutdown) => shutdown,
        Err(error) => {
            log::error!("cannot install shutdown signals: {error}");
            return ExitCode::FAILURE;
        }
    };
    let mut gateway = match GatewayRuntime::start(config).await {
        Ok(gateway) => gateway,
        Err(error) => {
            log::error!("gateway startup failed: {error}");
            return ExitCode::FAILURE;
        }
    };
    let shutdown_error = shutdown.wait().await.err();
    log::info!("gateway shutdown requested");
    gateway.close().await;
    if let Some(error) = shutdown_error {
        log::error!("shutdown signal failed: {error}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

#[cfg(unix)]
struct Shutdown {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl Shutdown {
    fn install() -> Result<Self, std::io::Error> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
        })
    }

    async fn wait(&mut self) -> Result<(), std::io::Error> {
        tokio::select! {
            _ = self.interrupt.recv() => Ok(()),
            _ = self.terminate.recv() => Ok(()),
        }
    }
}

#[cfg(not(unix))]
struct Shutdown;

#[cfg(not(unix))]
impl Shutdown {
    fn install() -> Result<Self, std::io::Error> {
        Ok(Self)
    }

    async fn wait(&mut self) -> Result<(), std::io::Error> {
        tokio::signal::ctrl_c().await
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::panic, clippy::unwrap_used)]

    use std::io::Write;

    use rockbee_raft_protocol::ClusterId;

    use super::*;

    const VALID_CONFIG: &str = r#"
cluster_id = "01010101010101010101010101010101"
listen = "127.0.0.1:9100"

[[seeds]]
node_id = 1
address = "127.0.0.1:8101"

[[seeds]]
node_id = 2
address = "127.0.0.1:8102"
"#;

    fn temporary_config(contents: &str) -> tempfile::NamedTempFile {
        let mut file = tempfile::Builder::new()
            .prefix("rockbee-raft-gateway-config-")
            .suffix(".toml")
            .tempfile()
            .expect("create temporary config");
        file.write_all(contents.as_bytes())
            .expect("write temporary config");
        file
    }

    fn config_arguments(path: &std::path::Path) -> std::vec::IntoIter<String> {
        vec!["--config".into(), path.display().to_string()].into_iter()
    }

    fn assert_config_error(contents: &str, expected: &str) {
        let file = temporary_config(contents);
        let error =
            cli::arguments(config_arguments(file.path())).expect_err("config must be rejected");
        assert!(error.contains(&file.path().display().to_string()));
        assert!(error.contains(expected), "unexpected error: {error}");
    }

    #[test]
    fn parses_a_valid_toml_config_file() {
        let file = temporary_config(VALID_CONFIG);
        let config = cli::arguments(config_arguments(file.path()))
            .expect("valid config")
            .expect("start request");

        assert_eq!(config.cluster_id, ClusterId::new([1; 16]).unwrap());
        assert_eq!(config.listen_address, "127.0.0.1:9100".parse().unwrap());
        assert_eq!(config.expected_shard_config.count(), 4);
        assert_eq!(config.expected_shard_config.seed(), 0);
        assert_eq!(config.seeds.len(), 2);
        assert_eq!(config.seeds[0].node_id.get(), 1);
        assert_eq!(config.seeds[1].address, "127.0.0.1:8102".parse().unwrap());
    }

    #[test]
    fn rejects_invalid_or_missing_toml_identity() {
        assert_config_error(
            VALID_CONFIG
                .replace("01010101010101010101010101010101", "not-a-cluster-id")
                .as_str(),
            "cluster_id",
        );
        assert_config_error(
            VALID_CONFIG
                .replacen("cluster_id = \"01010101010101010101010101010101\"\n", "", 1)
                .as_str(),
            "cluster_id",
        );
    }

    #[test]
    fn rejects_unknown_and_duplicate_toml_fields() {
        assert_config_error(format!("extra = true\n{VALID_CONFIG}").as_str(), "unknown");
        assert_config_error(
            VALID_CONFIG
                .replacen(
                    "address = \"127.0.0.1:8101\"",
                    "address = \"127.0.0.1:8101\"\nextra = true",
                    1,
                )
                .as_str(),
            "unknown",
        );
        assert_config_error(
            VALID_CONFIG
                .replacen(
                    "listen = \"127.0.0.1:9100\"",
                    "listen = \"127.0.0.1:9100\"\nlisten = \"127.0.0.1:9200\"",
                    1,
                )
                .as_str(),
            "duplicate",
        );
    }

    #[test]
    fn rejects_config_mixed_with_runtime_flags_and_missing_files() {
        let file = temporary_config(VALID_CONFIG);
        for values in [
            vec![
                "--config".to_owned(),
                file.path().display().to_string(),
                "--listen".to_owned(),
                "127.0.0.1:9200".to_owned(),
            ],
            vec![
                "--listen".to_owned(),
                "127.0.0.1:9200".to_owned(),
                "--config".to_owned(),
                file.path().display().to_string(),
            ],
        ] {
            let mixed = cli::arguments(values.into_iter())
                .expect_err("config and runtime arguments conflict");
            assert!(mixed.contains("--config"));
            assert!(mixed.contains("--listen"));
        }
        let repeated = cli::arguments(
            [
                "--config".to_owned(),
                file.path().display().to_string(),
                "--config".to_owned(),
                file.path().display().to_string(),
            ]
            .into_iter(),
        )
        .expect_err("repeated config must be rejected");
        assert!(repeated.contains("only once"));

        let missing = tempfile::Builder::new()
            .prefix("rockbee-raft-gateway-missing-config-")
            .suffix(".toml")
            .tempfile()
            .expect("reserve missing config path")
            .into_temp_path();
        let missing_path = missing.to_path_buf();
        missing.close().expect("remove reserved config path");
        let error = cli::arguments(config_arguments(&missing_path))
            .expect_err("missing file must be rejected");
        assert!(error.contains(&missing_path.display().to_string()));
    }

    #[test]
    fn rejects_toml_configs_that_fail_gateway_validation() {
        assert_config_error(
            VALID_CONFIG
                .replace("127.0.0.1:9100", "0.0.0.0:9100")
                .as_str(),
            "gateway listener address",
        );
        assert_config_error(
            VALID_CONFIG
                .replacen("node_id = 2", "node_id = 1", 1)
                .as_str(),
            "unique seed",
        );
        assert_config_error(
            "cluster_id = \"01010101010101010101010101010101\"\nlisten = \"127.0.0.1:9100\"\nseeds = []\n",
            "unique seed",
        );
    }

    #[test]
    fn keeps_inline_arguments_and_the_shipped_gateway_example_usable() {
        let inline = cli::arguments(
            [
                "--cluster-id",
                "01010101010101010101010101010101",
                "--listen",
                "127.0.0.1:9100",
                "--seed",
                "1@127.0.0.1:8101",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .expect("valid inline arguments")
        .expect("start request");
        assert_eq!(inline.seeds[0].node_id.get(), 1);

        let example =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../configs/gateway.toml");
        let config = cli::arguments(config_arguments(&example))
            .expect("valid shipped config")
            .expect("start request");
        assert_eq!(config.seeds.len(), 3);
        assert_eq!(config.seeds[2].address.port(), 8103);
    }

    #[test]
    fn inline_server_overrides_configure_gateway_admission_limits() {
        let config = cli::arguments(
            [
                "--cluster-id",
                "01010101010101010101010101010101",
                "--listen",
                "127.0.0.1:9100",
                "--seed",
                "1@127.0.0.1:8101",
                "--server-request-mib",
                "512",
                "--server-response-mib",
                "256",
                "--max-connections",
                "96",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .expect("valid inline arguments")
        .expect("start request");

        assert_eq!(config.server.request_bytes, 512 * 1024 * 1024);
        assert_eq!(config.server.response_bytes, 256 * 1024 * 1024);
        assert_eq!(config.server.max_connections, 96);
    }

    #[test]
    fn inline_server_overrides_reject_malformed_values() {
        for (flag, value, expected) in [
            (
                "--server-request-mib",
                "large",
                "--server-request-mib must be an unsigned MiB value",
            ),
            (
                "--server-response-mib",
                &usize::MAX.to_string(),
                "--server-response-mib MiB value overflows byte budget",
            ),
            (
                "--max-connections",
                "0",
                "server count limits must be nonzero",
            ),
        ] {
            let error = cli::arguments(
                [
                    "--cluster-id".to_owned(),
                    "01010101010101010101010101010101".to_owned(),
                    "--listen".to_owned(),
                    "127.0.0.1:9100".to_owned(),
                    "--seed".to_owned(),
                    "1@127.0.0.1:8101".to_owned(),
                    flag.to_owned(),
                    value.to_owned(),
                ]
                .into_iter(),
            )
            .expect_err("malformed server override must be rejected");
            assert!(error.contains(expected), "unexpected error: {error}");
        }
    }

    #[test]
    fn omits_toml_source_contents_from_parse_errors() {
        let file = temporary_config("sentinel-secret = [");
        let error = cli::arguments(config_arguments(file.path()))
            .expect_err("malformed config must be rejected");
        assert!(error.contains(&file.path().display().to_string()));
        assert!(
            !error.contains("sentinel-secret"),
            "error leaked config source: {error}"
        );
    }
}
