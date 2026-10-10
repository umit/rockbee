use std::process::Command;

fn binary() -> Command {
    Command::new(env!("CARGO_BIN_EXE_rockbee-writer"))
}

#[test]
fn help_names_the_fixed_binary() {
    let output = binary().arg("--help").output().unwrap();
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("Usage: rockbee-writer"));
}

#[test]
fn malformed_args_name_the_fixed_binary() {
    let output = binary().arg("--unknown").output().unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("Usage: rockbee-writer"));
}

#[test]
fn check_config_has_the_fixed_role_and_default_address() {
    let output = binary().arg("--check-config").output().unwrap();
    assert!(output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["role"], "writer");
    assert_eq!(value["addr"], "127.0.0.1:7101");
}

#[test]
fn conflicting_cli_role_fails_before_any_storage_open() {
    let temp = tempfile::tempdir().unwrap();
    let output = binary()
        .args(["--role", "reader", "--backend", "file", "--directory"])
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
        "role = \"reader\"\nbackend = \"file\"\ndirectory = \"objects\"\n",
    )
    .unwrap();
    let output = binary()
        .args(["--config"])
        .arg(&config)
        .args(["--role", "writer", "--check-config"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(!temp.path().join("objects").exists());
}
