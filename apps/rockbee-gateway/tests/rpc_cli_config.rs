use rockbee_gateway::config::Config as GatewayConfig;
use std::time::Duration;

fn parse(args: &[&str]) -> rockbee_node::Result<Option<GatewayConfig>> {
    GatewayConfig::parse(args.iter().map(|arg| (*arg).to_owned()))
}

fn parse_owned(args: Vec<String>) -> rockbee_node::Result<Option<GatewayConfig>> {
    GatewayConfig::parse(args)
}

fn base() -> Vec<&'static str> {
    vec!["--writer", "127.0.0.1:7000", "--reader", "127.0.0.1:7001"]
}

#[test]
fn gateway_rpc_flags_default_parse_and_validate_once() {
    let default = parse(&base()).unwrap().unwrap();
    assert_eq!(default.address, "127.0.0.1:7100");
    assert_eq!(default.readers, ["127.0.0.1:7001"]);
    assert_eq!(default.rpc.max_connections(), 256);
    assert_eq!(default.rpc.global_max_in_flight(), 128);
    assert_eq!(default.rpc.read_timeout(), Duration::from_secs(30));
    assert_eq!(default.rpc.write_timeout(), Duration::from_secs(5));
    assert!(!default.check_config);

    let mut global_only = base();
    global_only.extend(["--rpc-global-max-in-flight", "1024"]);
    let global_only = parse(&global_only).unwrap().unwrap();
    assert_eq!(global_only.rpc.max_connections(), 256);
    assert_eq!(global_only.rpc.global_max_in_flight(), 1024);

    let mut selected = base();
    selected.extend([
        "--rpc-max-connections",
        "19",
        "--rpc-global-max-in-flight",
        "1024",
        "--rpc-read-timeout-ms",
        "2000",
        "--rpc-write-timeout-ms",
        "3000",
    ]);
    let selected = parse(&selected).unwrap().unwrap();
    assert_eq!(selected.rpc.max_connections(), 19);
    assert_eq!(selected.rpc.global_max_in_flight(), 1024);
    assert_eq!(selected.rpc.read_timeout(), Duration::from_millis(2000));
    assert_eq!(selected.rpc.write_timeout(), Duration::from_millis(3000));

    for suffix in [
        vec!["--rpc-max-connections", "0"],
        vec!["--rpc-read-timeout-ms", "0"],
        vec!["--rpc-write-timeout-ms", "0"],
        vec!["--rpc-max-connections", "bad"],
        vec!["--rpc-read-timeout-ms", "bad"],
        vec!["--rpc-write-timeout-ms", "bad"],
        vec!["--rpc-max-connections", "1", "--rpc-max-connections", "2"],
        vec!["--rpc-read-timeout-ms", "1", "--rpc-read-timeout-ms", "2"],
        vec!["--rpc-write-timeout-ms", "1", "--rpc-write-timeout-ms", "2"],
        vec!["--rpc-max-connections"],
        vec!["--rpc-read-timeout-ms"],
        vec!["--rpc-write-timeout-ms"],
    ] {
        let mut args = base();
        args.extend(suffix);
        assert!(parse(&args).is_err(), "{args:?}");
    }
}

#[test]
fn gateway_addr_is_singleton_while_distinct_readers_repeat() {
    for suffix in [
        vec!["--addr", "127.0.0.1:7100", "--addr", "127.0.0.1:7100"],
        vec!["--addr", "127.0.0.1:7100", "--addr", "127.0.0.1:7102"],
    ] {
        let mut args = base();
        args.extend(suffix);
        assert!(parse(&args).is_err(), "{args:?}");
    }

    let config = parse(&[
        "--addr",
        "127.0.0.1:7110",
        "--writer",
        "127.0.0.1:7000",
        "--reader",
        "127.0.0.1:7001",
        "--reader",
        "127.0.0.1:7002",
    ])
    .unwrap()
    .unwrap();
    assert_eq!(config.address, "127.0.0.1:7110");
    assert_eq!(config.readers, ["127.0.0.1:7001", "127.0.0.1:7002"]);
}

#[test]
fn gateway_rpc_global_max_in_flight_rejects_invalid_values() {
    let semaphore_overflow = (tokio::sync::Semaphore::MAX_PERMITS + 1).to_string();
    let usize_overflow = format!("{}0", usize::MAX);
    let mut invalid = vec![
        vec!["--rpc-global-max-in-flight".to_owned(), "0".to_owned()],
        vec!["--rpc-global-max-in-flight".to_owned(), "-1".to_owned()],
        vec!["--rpc-global-max-in-flight".to_owned(), "nope".to_owned()],
        vec!["--rpc-global-max-in-flight".to_owned()],
        vec![
            "--rpc-global-max-in-flight".to_owned(),
            "1".to_owned(),
            "--rpc-global-max-in-flight".to_owned(),
            "2".to_owned(),
        ],
    ];
    invalid.push(vec![
        "--rpc-global-max-in-flight".to_owned(),
        usize_overflow,
    ]);
    invalid.push(vec![
        "--rpc-global-max-in-flight".to_owned(),
        semaphore_overflow,
    ]);

    for suffix in invalid {
        let mut args: Vec<String> = base().into_iter().map(str::to_owned).collect();
        args.extend(suffix);
        assert!(GatewayConfig::parse(args.clone()).is_err(), "{args:?}");
    }
    assert!(rockbee_gateway::config::USAGE.contains("--rpc-global-max-in-flight"));
}

#[test]
fn gateway_check_config_keeps_validation_and_rejects_duplicate_switches() {
    for suffix in [
        vec!["--check-config", "--check-config"],
        vec!["--check-config", "--upstream-max-in-flight", "129"],
        vec!["--check-config", "--upstream-max-pending", "0"],
        vec!["--check-config", "--rpc-write-timeout-ms", "0"],
        vec!["--check-config", "--addr", "missing-port"],
        vec!["--check-config", "--addr", ":7100"],
        vec!["--check-config", "--addr", "localhost:65536"],
        vec!["--check-config", "--addr", "http://localhost:7100"],
        vec!["--check-config", "--addr", "::1:7100"],
        vec!["--check-config", "--unknown", "value"],
    ] {
        let mut args = base();
        args.extend(suffix);
        assert!(parse(&args).is_err(), "{args:?}");
    }
    assert!(parse(&["--check-config"]).is_err());
    assert!(
        parse(&[
            "--check-config",
            "--writer",
            "localhost:7101",
            "--reader",
            "localhost:7101",
        ])
        .is_err()
    );

    let config = parse(&[
        "--check-config",
        "--addr",
        "[::1]:0",
        "--writer",
        "writer.invalid:7101",
        "--reader",
        "[::1]:7102",
    ])
    .unwrap()
    .unwrap();
    assert!(config.check_config);
    assert_eq!(config.address, "[::1]:0");
}

#[test]
fn gateway_toml_supplies_a_complete_topology_and_limits() -> rockbee_node::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("gateway.toml");
    std::fs::write(
        &path,
        r#"
addr = "127.0.0.1:7110"
writer = "writer.example:7101"
readers = ["reader-a.example:7102", "reader-b.example:7103"]

[upstream]
max_in_flight = 64
max_pending = 32

[rpc]
max_connections = 17
global_max_in_flight = 8
read_timeout_ms = 2000
write_timeout_ms = 3000
"#,
    )?;

    let config = parse_owned(vec![
        "--config".to_owned(),
        path.display().to_string(),
        "--check-config".to_owned(),
    ])?
    .expect("not help");
    assert!(config.check_config);
    assert_eq!(config.address, "127.0.0.1:7110");
    assert_eq!(config.writer, "writer.example:7101");
    assert_eq!(
        config.readers,
        ["reader-a.example:7102", "reader-b.example:7103"]
    );
    assert_eq!(config.upstream.max_in_flight(), 64);
    assert_eq!(config.upstream.max_pending(), 32);
    assert_eq!(config.rpc.max_connections(), 17);
    assert_eq!(config.rpc.global_max_in_flight(), 8);
    assert_eq!(config.rpc.read_timeout(), Duration::from_millis(2000));
    assert_eq!(config.rpc.write_timeout(), Duration::from_millis(3000));
    Ok(())
}

#[test]
fn gateway_cli_overrides_toml_and_replaces_reader_list() -> rockbee_node::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("gateway.toml");
    std::fs::write(
        &path,
        r#"
addr = "127.0.0.1:7110"
writer = "writer.example:7101"
readers = ["reader-a.example:7102", "reader-b.example:7103"]

[upstream]
max_in_flight = 64
max_pending = 32

[rpc]
max_connections = 17
global_max_in_flight = 8
read_timeout_ms = 2000
write_timeout_ms = 3000
"#,
    )?;

    let config = parse_owned(vec![
        "--config".to_owned(),
        path.display().to_string(),
        "--addr".to_owned(),
        "127.0.0.1:7120".to_owned(),
        "--writer".to_owned(),
        "writer-override.example:7101".to_owned(),
        "--reader".to_owned(),
        "reader-override.example:7102".to_owned(),
        "--upstream-max-pending".to_owned(),
        "16".to_owned(),
        "--rpc-write-timeout-ms".to_owned(),
        "4000".to_owned(),
    ])?
    .expect("not help");
    assert_eq!(config.address, "127.0.0.1:7120");
    assert_eq!(config.writer, "writer-override.example:7101");
    assert_eq!(config.readers, ["reader-override.example:7102"]);
    assert_eq!(config.upstream.max_in_flight(), 64);
    assert_eq!(config.upstream.max_pending(), 16);
    assert_eq!(config.rpc.max_connections(), 17);
    assert_eq!(config.rpc.write_timeout(), Duration::from_millis(4000));
    Ok(())
}

#[test]
fn gateway_rejects_unknown_toml_fields_at_every_level() -> rockbee_node::Result<()> {
    let directory = tempfile::tempdir()?;
    for (name, contents) in [
        (
            "top-level",
            "writer = \"writer.example:7101\"\nreaders = [\"reader.example:7102\"]\nextra = true\n",
        ),
        (
            "upstream",
            "writer = \"writer.example:7101\"\nreaders = [\"reader.example:7102\"]\n[upstream]\nunknown = 1\n",
        ),
        (
            "rpc",
            "writer = \"writer.example:7101\"\nreaders = [\"reader.example:7102\"]\n[rpc]\nunknown = 1\n",
        ),
    ] {
        let path = directory.path().join(format!("{name}.toml"));
        std::fs::write(&path, contents)?;
        let result = parse_owned(vec!["--config".to_owned(), path.display().to_string()]);
        assert!(result.is_err(), "{name} unknown field was accepted");
    }
    Ok(())
}

#[test]
fn gateway_rejects_unreadable_malformed_and_invalid_effective_toml() -> rockbee_node::Result<()> {
    let directory = tempfile::tempdir()?;
    let missing = directory.path().join("missing.toml");
    assert!(parse_owned(vec!["--config".to_owned(), missing.display().to_string()]).is_err());

    for (name, contents) in [
        ("malformed", "writer = [\n"),
        (
            "invalid-upstream",
            "writer = \"writer.example:7101\"\nreaders = [\"reader.example:7102\"]\n[upstream]\nmax_in_flight = 0\n",
        ),
        (
            "invalid-rpc",
            "writer = \"writer.example:7101\"\nreaders = [\"reader.example:7102\"]\n[rpc]\nread_timeout_ms = 0\n",
        ),
        (
            "invalid-readers",
            "writer = \"writer.example:7101\"\nreaders = [\"reader.example:7102\", \"reader.example:7102\"]\n",
        ),
    ] {
        let path = directory.path().join(format!("{name}.toml"));
        std::fs::write(&path, contents)?;
        let result = parse_owned(vec!["--config".to_owned(), path.display().to_string()]);
        assert!(result.is_err(), "{name} TOML was accepted");
    }
    Ok(())
}
