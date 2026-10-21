#![allow(clippy::panic, clippy::unwrap_used)]
use super::*;

fn arguments() -> Vec<String> {
    [
        "--node-id",
        "1",
        "--cluster-id",
        "01010101010101010101010101010101",
        "--data-dir",
        "/tmp/example-db",
        "--create",
        "--node",
        "3@127.0.0.1:7103@127.0.0.1:8103",
        "--node",
        "1@127.0.0.1:7101@127.0.0.1:8101",
        "--node",
        "2@127.0.0.1:7102@127.0.0.1:8102",
        "--initialize",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

#[test]
fn creates_canonical_initial_directory_and_requires_explicit_open_intent() {
    let parsed = parse(arguments().into_iter())
        .expect("valid arguments")
        .expect("start request");
    assert!(parsed.initialize);
    let Startup::Bootstrap(config) = parsed.config else {
        panic!("expected bootstrap configuration")
    };
    assert_eq!(config.directory.nodes[0].node_id.get(), 1);
    assert_eq!(config.directory.placements.len(), 5);
    assert!(matches!(config.open_mode, OpenMode::CreateNew));
    let missing = arguments().into_iter().filter(|value| value != "--create");
    assert!(parse(missing).is_err());
    let mut conflicting = arguments();
    conflicting.push("--open".into());
    assert!(parse(conflicting.into_iter()).is_err());
}

#[test]
fn rejects_unknown_flags_missing_values_and_non_ascii_cluster_ids_without_panic() {
    assert!(parse(["--unknown".into()].into_iter()).is_err());
    assert!(parse(["--node-id".into()].into_iter()).is_err());
    let mut unicode = arguments();
    unicode[3] = "é".repeat(16);
    assert!(parse(unicode.into_iter()).is_err());
    assert!(
        parse(["--help".into()].into_iter())
            .expect("help")
            .is_none()
    );
}
fn join_arguments() -> Vec<String> {
    [
        "--join",
        "--local-node",
        "1@127.0.0.1:7101@127.0.0.1:8101",
        "--cluster-id",
        "01010101010101010101010101010101",
        "--data-dir",
        "/tmp/join-example-db",
        "--create",
        "--seed",
        "10@127.0.0.1:8110",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

#[test]
fn explicit_join_parses_one_local_identity_and_bounded_backend_seeds() {
    let parsed = parse(join_arguments().into_iter()).unwrap().unwrap();
    let Startup::Join(config) = parsed.config else {
        panic!("expected join configuration")
    };
    assert_eq!(config.local_endpoint.node_id.get(), 1);
    assert_eq!(config.seeds[0].node_id.get(), 10);
    assert!(!parsed.initialize);
    for conflicting in [
        vec!["--initialize"],
        vec!["--node", "2@127.0.0.1:7102@127.0.0.1:8102"],
        vec!["--node-id", "1"],
        vec!["--open"],
        vec!["--startup-timeout-ms", "0"],
    ] {
        let mut values = join_arguments();
        values.extend(conflicting.into_iter().map(str::to_owned));
        assert!(parse(values.into_iter()).is_err());
    }
    for flag in ["--local-node", "--seed"] {
        let values = join_arguments();
        let index = values.iter().position(|value| value == flag).unwrap();
        let values = values
            .into_iter()
            .enumerate()
            .filter_map(|(position, value)| {
                if position == index || position == index + 1 {
                    None
                } else {
                    Some(value)
                }
            });
        assert!(parse(values).is_err());
    }
}

#[test]
fn server_budget_flags_override_bootstrap_and_join_admission_budgets() {
    let mut bootstrap = arguments();
    bootstrap.extend([
        "--server-request-mib".into(),
        "512".into(),
        "--server-response-mib".into(),
        "256".into(),
    ]);
    let parsed = parse(bootstrap.into_iter())
        .expect("valid bootstrap")
        .expect("startup");
    let Startup::Bootstrap(config) = parsed.config else {
        panic!("expected bootstrap configuration")
    };
    assert_eq!(config.server.request_bytes, 512 * 1024 * 1024);
    assert_eq!(config.server.response_bytes, 256 * 1024 * 1024);

    let mut join = join_arguments();
    join.extend([
        "--server-request-mib".into(),
        "512".into(),
        "--server-response-mib".into(),
        "256".into(),
    ]);
    let parsed = parse(join.into_iter())
        .expect("valid join")
        .expect("startup");
    let Startup::Join(config) = parsed.config else {
        panic!("expected join configuration")
    };
    assert_eq!(config.server.request_bytes, 512 * 1024 * 1024);
    assert_eq!(config.server.response_bytes, 256 * 1024 * 1024);
}

#[test]
fn server_budget_flags_reject_non_numeric_and_overflowing_mib_values() {
    let mut non_numeric = arguments();
    non_numeric.extend(["--server-request-mib".into(), "large".into()]);
    assert_eq!(
        error(parse(non_numeric.into_iter())),
        "--server-request-mib must be an unsigned MiB value"
    );

    let mut overflowing = arguments();
    overflowing.extend(["--server-response-mib".into(), usize::MAX.to_string()]);
    assert_eq!(
        error(parse(overflowing.into_iter())),
        "--server-response-mib MiB value overflows byte budget"
    );
}

fn error(result: Result<Option<Arguments>, String>) -> String {
    match result {
        Ok(_) => panic!("expected parser error"),
        Err(error) => error,
    }
}

mod toml;
