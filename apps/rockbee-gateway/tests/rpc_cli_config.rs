use rockbee_gateway::config::Config as GatewayConfig;
use std::time::Duration;

fn parse(args: &[&str]) -> rockbee_node::Result<Option<GatewayConfig>> {
    GatewayConfig::parse(args.iter().map(|arg| (*arg).to_owned()))
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
