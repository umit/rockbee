#![cfg_attr(test, allow(clippy::unwrap_used))]

use rockbee_node::backend::Backend;
use rockbee_node::config::Config;
use rockbee_node::storage::Role;
use std::fs;
use std::time::Duration;

fn parse(args: &[String]) -> rockbee_node::Result<Option<Config>> {
    Config::parse(args.iter().cloned())
}

fn config_path(contents: &str) -> tempfile::NamedTempFile {
    let file = tempfile::NamedTempFile::new().unwrap();
    fs::write(file.path(), contents).unwrap();
    file
}

#[test]
fn config_file_is_merged_before_explicit_cli_flags_regardless_of_flag_order() {
    let file = config_path(
        r#"
role = "reader"
addr = "127.0.0.1:7010"
prefix = "from-file"
backend = "localstack"
bucket = "file-bucket"
endpoint = "http://localhost:4566"

[rpc]
max_connections = 17
global_max_in_flight = 19
read_timeout_ms = 1234
write_timeout_ms = 5678

[memory]
block_cache_bytes = 8192
metadata_cache_bytes = 4096
block_cache_shards = 1
metadata_cache_shards = 1
reader_max_memtable_bytes = 2048
writer_max_unflushed_bytes = 134217728

        [reader]
        poll_interval_ms = 125
"#,
    );
    let args = vec![
        "--addr".to_owned(),
        "127.0.0.1:7020".to_owned(),
        "--config".to_owned(),
        file.path().display().to_string(),
        "--prefix".to_owned(),
        "from-cli".to_owned(),
    ];

    let config = parse(&args).unwrap().unwrap();
    assert_eq!(config.role, Role::Reader);
    assert_eq!(config.address, "127.0.0.1:7020");
    assert_eq!(config.prefix, "from-cli");
    assert!(
        matches!(config.backend, Backend::LocalStack { ref bucket, ref endpoint }
        if bucket == "file-bucket" && endpoint == "http://localhost:4566")
    );
    assert_eq!(config.rpc.max_connections(), 17);
    assert_eq!(config.rpc.global_max_in_flight(), 19);
    assert_eq!(config.memory.block_cache_bytes, 8192);
    assert_eq!(config.memory.metadata_cache_bytes, 4096);
    assert_eq!(config.memory.reader_max_memtable_bytes, 2048);
    assert_eq!(config.memory.writer_max_unflushed_bytes, 134217728);
    assert_eq!(
        config.reader.manifest_poll_interval(),
        Duration::from_millis(125)
    );
}

#[test]
fn config_file_rejects_unknown_tables_empty_strings_and_invalid_static_limits() {
    for contents in [
        "unknown = 1",
        "[rpc]\nunknown = 1",
        "[memory]\nunknown = 1",
        "prefix = ''",
        "directory = ''",
        "aws_secret_access_key = 'not-accepted'",
        "[rpc]\nmax_connections = 0",
        "[memory]\nblock_cache_bytes = 0",
        "[memory]\nwriter_max_unflushed_bytes = 67108864",
        "[reader]\npoll_interval_ms = 0",
        "[reader]\npoll_interval_ms = 300001",
        "[reader]\nunknown = 1",
    ] {
        let file = config_path(contents);
        let args = vec!["--config".to_owned(), file.path().display().to_string()];
        assert!(parse(&args).is_err(), "{contents:?}");
    }
}

#[test]
fn backend_changed_on_cli_rejects_incompatible_file_values() {
    let file = config_path(
        r#"
backend = "localstack"
endpoint = "http://localhost:4566"
"#,
    );
    let args = vec![
        "--config".to_owned(),
        file.path().display().to_string(),
        "--backend".to_owned(),
        "s3".to_owned(),
    ];
    let error = match parse(&args) {
        Err(error) => error,
        Ok(_) => panic!("incompatible config file value was accepted"),
    };
    assert!(error.to_string().contains("--endpoint"));
}

#[test]
fn check_config_is_cli_only_and_effective_json_contains_resolved_values() {
    let file = config_path(
        r#"
backend = "file"
directory = "./example-objects"

[memory]
block_cache_shards = "auto"
metadata_cache_shards = "auto"
"#,
    );
    let args = vec![
        "--check-config".to_owned(),
        "--config".to_owned(),
        file.path().display().to_string(),
    ];
    let config = parse(&args).unwrap().unwrap();
    assert!(config.check_config);
    let effective = config.effective_json();
    assert_eq!(effective["role"], "writer");
    assert_eq!(effective["addr"], "127.0.0.1:7000");
    assert_eq!(effective["prefix"], "rockbee");
    assert_eq!(effective["backend"]["kind"], "file");
    assert_eq!(effective["backend"]["directory"], "./example-objects");
    assert_eq!(effective["rpc"]["max_connections"], 256);
    assert_eq!(effective["rpc"]["global_max_in_flight"], 128);
    assert!(effective["memory"]["block_cache_shards"].is_u64());
    assert!(effective["memory"]["metadata_cache_shards"].is_u64());
    assert!(effective.get("reader").is_none());

    let duplicate = vec!["--check-config".to_owned(), "--check-config".to_owned()];
    assert!(parse(&duplicate).is_err());
    let duplicate_config = vec![
        "--config".to_owned(),
        file.path().display().to_string(),
        "--config".to_owned(),
        file.path().display().to_string(),
    ];
    assert!(parse(&duplicate_config).is_err());
}

#[test]
fn reader_poll_interval_is_resolved_and_writer_rejects_it() {
    let file = config_path(
        r#"
role = "reader"

[reader]
poll_interval_ms = 125
"#,
    );
    let args = vec!["--config".to_owned(), file.path().display().to_string()];
    let config = parse(&args).unwrap().unwrap();
    assert_eq!(
        config.reader.manifest_poll_interval(),
        Duration::from_millis(125)
    );
    assert_eq!(config.effective_json()["reader"]["poll_interval_ms"], 125);

    let boundary_file = config_path("role = 'reader'\n[reader]\npoll_interval_ms = 300000");
    let boundary_args = vec![
        "--config".to_owned(),
        boundary_file.path().display().to_string(),
    ];
    let boundary = parse(&boundary_args).unwrap().unwrap();
    assert_eq!(
        boundary.reader.manifest_poll_interval(),
        Duration::from_secs(300)
    );

    let writer_file = config_path("role = 'writer'\n[reader]\npoll_interval_ms = 125");
    let writer_args = vec![
        "--config".to_owned(),
        writer_file.path().display().to_string(),
    ];
    let error = match parse(&writer_args) {
        Err(error) => error,
        Ok(_) => panic!("writer unexpectedly accepted [reader] configuration"),
    };
    assert!(
        error
            .to_string()
            .contains("only supported by the reader role")
    );
}
