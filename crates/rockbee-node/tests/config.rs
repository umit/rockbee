use rockbee_node::backend::Backend;
use rockbee_node::config::{Config, USAGE};
use rockbee_node::storage::Role;
use std::time::Duration;

fn parse(args: &[&str]) -> rockbee_node::Result<Option<Config>> {
    Config::parse(args.iter().map(|arg| (*arg).to_owned()))
}

#[test]
fn reader_and_file_backend_are_selected_explicitly() {
    let config = parse(&[
        "--role",
        "reader",
        "--backend",
        "file",
        "--directory",
        "./objects",
    ])
    .unwrap()
    .unwrap();
    assert_eq!(config.role, Role::Reader);
    assert!(matches!(config.backend, Backend::File { .. }));
}

#[test]
fn invalid_flags_roles_backends_and_missing_values_fail() {
    for args in [
        vec!["--role", "other"],
        vec!["--backend", "other"],
        vec!["--prefix", ""],
        vec!["--bucket", ""],
        vec!["--addr"],
        vec!["--unknown"],
    ] {
        assert!(parse(&args).is_err());
    }
    assert!(parse(&["--help"]).unwrap().is_none());
}

#[test]
fn free_string_flags_reject_another_long_option_as_their_value() {
    for args in [
        vec!["--prefix", "--backend"],
        vec!["--bucket", "--backend"],
        vec!["--endpoint", "--backend"],
        vec!["--addr", "--backend"],
        vec!["--backend", "file", "--directory", "--role"],
    ] {
        let error = match parse(&args) {
            Err(error) => error,
            Ok(_) => panic!("long option was accepted as a value: {args:?}"),
        };
        assert_eq!(error.to_string(), USAGE, "{args:?}");
    }

    let prefix = parse(&["--prefix", "tenant--blue"]).unwrap().unwrap();
    assert_eq!(prefix.prefix, "tenant--blue");
    let directory = parse(&["--backend", "file", "--directory", "./--objects"])
        .unwrap()
        .unwrap();
    assert!(matches!(directory.backend, Backend::File { ref directory }
        if directory == std::path::Path::new("./--objects")));
}

#[test]
fn server_singleton_flags_reject_same_and_different_duplicate_values() {
    for args in [
        vec!["--addr", "127.0.0.1:7100", "--addr", "127.0.0.1:7100"],
        vec!["--addr", "127.0.0.1:7100", "--addr", "127.0.0.1:7101"],
        vec!["--role", "writer", "--role", "writer"],
        vec!["--role", "writer", "--role", "reader"],
        vec!["--backend", "localstack", "--backend", "localstack"],
        vec!["--backend", "localstack", "--backend", "file"],
        vec!["--prefix", "first", "--prefix", "first"],
        vec!["--prefix", "first", "--prefix", "second"],
        vec!["--bucket", "first", "--bucket", "first"],
        vec!["--bucket", "first", "--bucket", "second"],
        vec!["--endpoint", "http://first", "--endpoint", "http://first"],
        vec!["--endpoint", "http://first", "--endpoint", "http://second"],
        vec![
            "--backend",
            "file",
            "--directory",
            "first",
            "--directory",
            "first",
        ],
        vec![
            "--backend",
            "file",
            "--directory",
            "first",
            "--directory",
            "second",
        ],
    ] {
        let error = match parse(&args) {
            Err(error) => error,
            Ok(_) => panic!("duplicate singleton flag was accepted: {args:?}"),
        };
        assert_eq!(error.to_string(), USAGE, "{args:?}");
    }
}

#[test]
fn server_rpc_flags_default_parse_and_validate_once() {
    let default = parse(&[]).unwrap().unwrap();
    assert_eq!(default.address, rockbee_node::DEFAULT_ADDR);
    assert_eq!(default.role, Role::Writer);
    assert_eq!(default.prefix, "rockbee");
    assert_eq!(default.rpc.max_connections(), 256);
    assert_eq!(default.rpc.global_max_in_flight(), 128);
    assert_eq!(default.rpc.read_timeout(), Duration::from_secs(30));
    assert_eq!(default.rpc.write_timeout(), Duration::from_secs(5));

    let global_only = parse(&["--rpc-global-max-in-flight", "1024"])
        .unwrap()
        .unwrap();
    assert_eq!(global_only.rpc.max_connections(), 256);
    assert_eq!(global_only.rpc.global_max_in_flight(), 1024);

    let configured = parse(&[
        "--rpc-max-connections",
        "17",
        "--rpc-global-max-in-flight",
        "1024",
        "--rpc-read-timeout-ms",
        "1234",
        "--rpc-write-timeout-ms",
        "5678",
    ])
    .unwrap()
    .unwrap();
    assert_eq!(configured.rpc.max_connections(), 17);
    assert_eq!(configured.rpc.global_max_in_flight(), 1024);
    assert_eq!(configured.rpc.read_timeout(), Duration::from_millis(1234));
    assert_eq!(configured.rpc.write_timeout(), Duration::from_millis(5678));

    for args in [
        vec!["--rpc-max-connections", "0"],
        vec!["--rpc-read-timeout-ms", "0"],
        vec!["--rpc-write-timeout-ms", "0"],
        vec!["--rpc-max-connections", "nope"],
        vec!["--rpc-read-timeout-ms", "nope"],
        vec!["--rpc-write-timeout-ms", "nope"],
        vec!["--rpc-max-connections", "1", "--rpc-max-connections", "2"],
        vec!["--rpc-read-timeout-ms", "1", "--rpc-read-timeout-ms", "2"],
        vec!["--rpc-write-timeout-ms", "1", "--rpc-write-timeout-ms", "2"],
        vec!["--rpc-max-connections"],
        vec!["--rpc-read-timeout-ms"],
        vec!["--rpc-write-timeout-ms"],
    ] {
        assert!(parse(&args).is_err(), "{args:?}");
    }
}

#[test]
fn server_rpc_global_max_in_flight_rejects_invalid_values() {
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

    for args in invalid {
        assert!(Config::parse(args.clone()).is_err(), "{args:?}");
    }
    assert!(USAGE.contains("--rpc-global-max-in-flight"));
}

#[test]
fn backend_specific_options_reject_incompatible_explicit_values() {
    for (args, rejected_flag) in [
        (vec!["--directory", "./objects"], "--directory"),
        (
            vec!["--backend", "localstack", "--directory", "./objects"],
            "--directory",
        ),
        (
            vec!["--backend", "s3", "--endpoint", "http://127.0.0.1:4566"],
            "--endpoint",
        ),
        (
            vec!["--backend", "s3", "--directory", "./objects"],
            "--directory",
        ),
        (
            vec!["--backend", "file", "--bucket", "rockbee-test"],
            "--bucket",
        ),
        (
            vec!["--backend", "file", "--endpoint", "http://127.0.0.1:4566"],
            "--endpoint",
        ),
    ] {
        let error = match parse(&args) {
            Err(error) => error,
            Ok(_) => panic!("incompatible storage option was accepted: {args:?}"),
        };
        assert!(
            error.to_string().contains(rejected_flag),
            "{args:?}: {error}"
        );
    }
}

#[test]
fn backend_specific_options_keep_default_and_valid_orders() {
    let default = parse(&[]).unwrap().unwrap();
    assert!(
        matches!(default.backend, Backend::LocalStack { ref bucket, ref endpoint }
        if bucket == "rockbee-test" && endpoint == "http://127.0.0.1:4566")
    );

    let local_before = parse(&[
        "--bucket",
        "before",
        "--backend",
        "localstack",
        "--endpoint",
        "http://before",
    ])
    .unwrap()
    .unwrap();
    assert!(
        matches!(local_before.backend, Backend::LocalStack { ref bucket, ref endpoint }
        if bucket == "before" && endpoint == "http://before")
    );
    let local_after = parse(&[
        "--backend",
        "localstack",
        "--bucket",
        "after",
        "--endpoint",
        "http://after",
    ])
    .unwrap()
    .unwrap();
    assert!(
        matches!(local_after.backend, Backend::LocalStack { ref bucket, ref endpoint }
        if bucket == "after" && endpoint == "http://after")
    );

    let s3_before = parse(&["--bucket", "before", "--backend", "s3"])
        .unwrap()
        .unwrap();
    assert!(matches!(s3_before.backend, Backend::S3 { ref bucket } if bucket == "before"));
    let s3_after = parse(&["--backend", "s3", "--bucket", "after"])
        .unwrap()
        .unwrap();
    assert!(matches!(s3_after.backend, Backend::S3 { ref bucket } if bucket == "after"));

    let file_before = parse(&["--directory", "before", "--backend", "file"])
        .unwrap()
        .unwrap();
    assert!(
        matches!(file_before.backend, Backend::File { ref directory } if directory == std::path::Path::new("before"))
    );
    let file_after = parse(&["--backend", "file", "--directory", "after"])
        .unwrap()
        .unwrap();
    assert!(
        matches!(file_after.backend, Backend::File { ref directory } if directory == std::path::Path::new("after"))
    );
}

#[test]
fn incompatible_option_is_rejected_before_an_occupied_listener_or_s3_connect() {
    use std::net::TcpListener;
    use std::process::{Command, Stdio};
    use std::thread;
    use std::time::{Duration, Instant};

    let occupied = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = occupied.local_addr().unwrap().to_string();
    let run = |args: &[&str]| {
        let mut child = Command::new(env!("CARGO_BIN_EXE_rockbee-server"))
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if child.try_wait().unwrap().is_some() {
                break child.wait_with_output().unwrap();
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let output = child.wait_with_output().unwrap();
                panic!("server did not reject invalid options within five seconds: {output:?}");
            }
            thread::sleep(Duration::from_millis(10));
        }
    };

    let incompatible = run(&[
        "--addr",
        &address,
        "--backend",
        "s3",
        "--endpoint",
        "http://127.0.0.1:4566",
    ]);
    assert!(!incompatible.status.success());
    let stderr = String::from_utf8_lossy(&incompatible.stderr);
    assert!(stderr.contains("--endpoint"), "stderr={stderr:?}");
    assert!(!stderr.contains("AddrInUse"), "stderr={stderr:?}");
    assert!(!String::from_utf8_lossy(&incompatible.stdout).contains("\"event\":\"ready\""));

    let missing_prefix_value = run(&["--addr", &address, "--prefix", "--backend"]);
    assert!(!missing_prefix_value.status.success());
    let stderr = String::from_utf8_lossy(&missing_prefix_value.stderr);
    assert!(stderr.contains("Usage:"), "stderr={stderr:?}");
    assert!(!stderr.contains("AddrInUse"), "stderr={stderr:?}");
    assert!(!String::from_utf8_lossy(&missing_prefix_value.stdout).contains("\"event\":\"ready\""));

    let duplicate_prefix = run(&[
        "--addr", &address, "--prefix", "first", "--prefix", "second",
    ]);
    assert!(!duplicate_prefix.status.success());
    let stderr = String::from_utf8_lossy(&duplicate_prefix.stderr);
    assert!(stderr.contains("Usage:"), "stderr={stderr:?}");
    assert!(!stderr.contains("AddrInUse"), "stderr={stderr:?}");
    assert!(!String::from_utf8_lossy(&duplicate_prefix.stdout).contains("\"event\":\"ready\""));
}

#[test]
fn fixed_role_defaults_are_distinct_and_explicit_legacy_address_is_preserved() {
    let writer = Config::parse_for_role(Role::Writer, std::iter::empty())
        .unwrap()
        .unwrap();
    let reader = Config::parse_for_role(Role::Reader, std::iter::empty())
        .unwrap()
        .unwrap();
    assert_eq!(writer.address, "127.0.0.1:7101");
    assert_eq!(reader.address, "127.0.0.1:7102");
    let explicit = Config::parse_for_role(
        Role::Reader,
        ["--addr".to_owned(), "127.0.0.1:7000".to_owned()],
    )
    .unwrap()
    .unwrap();
    assert_eq!(explicit.address, "127.0.0.1:7000");
}

#[test]
fn fixed_role_rejects_each_conflicting_role_source() {
    assert!(Config::parse_for_role(Role::Reader, ["--role".into(), "writer".into()]).is_err());
    let path = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(path.path(), "role = \"writer\"\n").unwrap();
    assert!(
        Config::parse_for_role(
            Role::Reader,
            [
                "--config".into(),
                path.path().display().to_string(),
                "--role".into(),
                "reader".into()
            ],
        )
        .is_err()
    );
}
