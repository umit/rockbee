//! Configuration-file boundary tests: parse real files without opening storage.
#![allow(clippy::panic, clippy::unwrap_used)]

use super::super::*;

const BOOTSTRAP: &str = r#"
mode = "bootstrap"
cluster_id = "01010101010101010101010101010101"
node_id = 1
data_dir = "storage/node-1"
shards = 4
hash_seed = 0

[[nodes]]
node_id = 1
peer_address = "127.0.0.1:7101"
data_address = "127.0.0.1:8101"
[[nodes]]
node_id = 2
peer_address = "127.0.0.1:7102"
data_address = "127.0.0.1:8102"
[[nodes]]
node_id = 3
peer_address = "127.0.0.1:7103"
data_address = "127.0.0.1:8103"
"#;

const JOIN: &str = r#"
mode = "join"
cluster_id = "01010101010101010101010101010101"
data_dir = "storage/node-4"
shards = 4
hash_seed = 0
startup_timeout_ms = 12000

[local_node]
node_id = 4
peer_address = "127.0.0.1:7104"
data_address = "127.0.0.1:8104"
[[seeds]]
node_id = 1
address = "127.0.0.1:8101"
"#;

fn write_config(contents: &str) -> (tempfile::TempDir, PathBuf) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("node.toml");
    std::fs::write(&path, contents).unwrap();
    (root, path)
}

fn read_arguments(path: &std::path::Path, extra: &[&str]) -> Result<Arguments, String> {
    parse(
        ["--config".to_owned(), path.to_string_lossy().into_owned()]
            .into_iter()
            .chain(extra.iter().map(|value| (*value).to_owned())),
    )?
    .ok_or_else(|| "expected startup arguments".into())
}

#[test]
fn toml_bootstrap_uses_file_relative_storage_and_explicit_open_intent() {
    let (root, path) = write_config(BOOTSTRAP);
    for (flag, creating) in [("--create", true), ("--open", false)] {
        let parsed = read_arguments(&path, &[flag, "--initialize"]).unwrap();
        assert!(parsed.initialize);
        let Startup::Bootstrap(config) = parsed.config else {
            panic!("expected bootstrap settings");
        };
        assert_eq!(config.node_id.get(), 1);
        assert_eq!(config.data_directory, root.path().join("storage/node-1"));
        assert_eq!(config.directory.placements.len(), 5);
        assert_eq!(config.directory.nodes[2].data_address.port(), 8103);
        assert_eq!(matches!(config.open_mode, OpenMode::CreateNew), creating);
    }
    assert!(!root.path().join("storage").exists());
    assert!(read_arguments(&path, &[]).is_err());
    assert!(read_arguments(&path, &["--create", "--open"]).is_err());
}

#[test]
fn toml_join_keeps_seeds_separate_from_voters_and_cannot_initialize() {
    let (root, path) = write_config(JOIN);
    let parsed = read_arguments(&path, &["--open"]).unwrap();
    assert!(!parsed.initialize);
    let Startup::Join(config) = parsed.config else {
        panic!("expected join settings");
    };
    assert_eq!(config.local_endpoint.node_id.get(), 4);
    assert_eq!(config.seeds[0].node_id.get(), 1);
    assert_eq!(config.seeds[0].address.port(), 8101);
    assert_eq!(config.startup_timeout, Duration::from_secs(12));
    assert_eq!(config.data_directory, root.path().join("storage/node-4"));
    assert!(read_arguments(&path, &["--create", "--initialize"]).is_err());
}

#[test]
fn toml_rejects_unknown_fields_and_lifecycle_settings_in_file() {
    for invalid in [
        format!("shrad_count = 8\n{BOOTSTRAP}"),
        format!("create = true\n{BOOTSTRAP}"),
        format!("initialize = true\n{BOOTSTRAP}"),
        format!("{BOOTSTRAP}\npeer_adress = '127.0.0.1:7103'"),
        format!("{JOIN}\naddres = '127.0.0.1:8101'"),
        BOOTSTRAP.replace("mode = \"bootstrap\"", "mode = \"bootstarp\""),
    ] {
        let (_root, path) = write_config(&invalid);
        let error = read_arguments(&path, &["--create"]).err().unwrap();
        assert!(error.contains(&path.display().to_string()), "{error}");
    }
}

#[test]
fn toml_cannot_override_identity_or_addresses_with_cli_flags_in_either_order() {
    let (_root, path) = write_config(BOOTSTRAP);
    for flags in [
        vec!["--node-id", "2"],
        vec!["--shards", "8"],
        vec!["--join"],
    ] {
        assert!(read_arguments(&path, &[vec!["--create"], flags.clone()].concat()).is_err());
        let args = flags.into_iter().map(str::to_owned).chain([
            "--config".to_owned(),
            path.to_string_lossy().into_owned(),
            "--create".to_owned(),
        ]);
        assert!(parse(args).is_err());
    }
    assert!(read_arguments(&path, &["--create", "--config", path.to_str().unwrap()]).is_err());
}

#[test]
fn toml_uses_existing_identity_membership_and_storage_validation() {
    for invalid in [
        BOOTSTRAP.replace("node_id = 1\ndata_dir", "node_id = 4\ndata_dir"),
        BOOTSTRAP.replace("data_dir = \"storage/node-1\"", "data_dir = \"\""),
        BOOTSTRAP.replace("node_id = 3", "node_id = 2"),
        BOOTSTRAP.replace("shards = 4", "shards = 0"),
        BOOTSTRAP.replace("01010101010101010101010101010101", "wrong-cluster"),
        JOIN.replace("startup_timeout_ms = 12000", "startup_timeout_ms = 0"),
    ] {
        let (_root, path) = write_config(&invalid);
        assert!(read_arguments(&path, &["--create"]).is_err());
    }
    let (_root, path) = write_config(&BOOTSTRAP.replacen("node_id = 1", "node_id = 2", 1));
    assert!(read_arguments(&path, &["--create", "--initialize"]).is_err());
}

#[test]
fn toml_reports_missing_and_malformed_files_with_their_path() {
    let (root, path) = write_config("broken = [");
    for path in [path, root.path().join("missing.toml")] {
        let error = read_arguments(&path, &["--create"]).err().unwrap();
        assert!(error.contains(&path.display().to_string()), "{error}");
    }
}

#[test]
fn toml_shipped_node_examples_parse_without_creating_their_databases() {
    let configs = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../configs");
    for id in 1..=3 {
        let parsed = read_arguments(&configs.join(format!("node-{id}.toml")), &["--open"]).unwrap();
        let Startup::Bootstrap(config) = parsed.config else {
            panic!("expected bootstrap example");
        };
        assert_eq!(config.node_id.get(), id);
        assert_eq!(
            config.directory.cluster_id,
            ClusterId::new([1; 16]).unwrap()
        );
        assert_eq!(config.directory.nodes.len(), 3);
    }
    assert!(matches!(
        read_arguments(&configs.join("node-4-join.toml"), &["--open"])
            .unwrap()
            .config,
        Startup::Join(_)
    ));
}

#[test]
fn toml_defaults_and_absolute_storage_paths_preserve_startup_semantics() {
    let directory = tempfile::tempdir().unwrap();
    let absolute = directory.path().join("existing-database");
    let contents = JOIN
        .replace("shards = 4\n", "")
        .replace("hash_seed = 0\n", "")
        .replace("startup_timeout_ms = 12000\n", "")
        .replace("storage/node-4", absolute.to_str().unwrap());
    let (_root, path) = write_config(&contents);
    let Startup::Join(config) = read_arguments(&path, &["--open"]).unwrap().config else {
        panic!("expected join settings");
    };
    assert_eq!(config.data_directory, absolute);
    assert_eq!(config.shard_config.count(), 4);
    assert_eq!(config.shard_config.seed(), 0);
    assert_eq!(config.startup_timeout, Duration::from_secs(10));
    assert!(!absolute.exists());
}

#[test]
fn toml_parse_error_does_not_dump_the_source_line() {
    let (_root, path) = write_config("mode = [ # private-config-comment-sentinel\n");
    let error = read_arguments(&path, &["--create"]).err().unwrap();
    assert!(error.contains(&path.display().to_string()));
    assert!(!error.contains("private-config-comment-sentinel"));
}

#[test]
fn inline_directory_named_config_flag_is_still_a_value() {
    let parsed = parse(
        [
            "--node-id",
            "1",
            "--cluster-id",
            "01010101010101010101010101010101",
            "--data-dir",
            "--config",
            "--create",
            "--node",
            "1@127.0.0.1:7101@127.0.0.1:8101",
            "--node",
            "2@127.0.0.1:7102@127.0.0.1:8102",
            "--node",
            "3@127.0.0.1:7103@127.0.0.1:8103",
        ]
        .into_iter()
        .map(str::to_owned),
    )
    .unwrap()
    .unwrap();
    let Startup::Bootstrap(config) = parsed.config else {
        panic!("expected inline bootstrap settings");
    };
    assert_eq!(config.data_directory, PathBuf::from("--config"));
}
