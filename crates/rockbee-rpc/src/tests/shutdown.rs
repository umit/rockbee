//! Signal registration is process-global, so each case owns a fresh subprocess.
#![cfg_attr(test, allow(clippy::unwrap_used))]

use super::wait_for_shutdown;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const CHILD_SIGNAL: &str = "ROCKBEE_SHUTDOWN_TEST_CHILD_SIGNAL";
const CHILD_RESULT: &str = "ROCKBEE_SHUTDOWN_TEST_CHILD_RESULT";

struct OwnedChild(Child);

impl Drop for OwnedChild {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}

fn test_name(case: &str) -> String {
    let module = module_path!();
    let (_, module) = module.split_once("::").unwrap_or(("", module));
    format!("{module}::{case}")
}

fn check_signal_at_ready(signal: &str, test_name: &str) {
    if std::env::var(CHILD_SIGNAL).ok().as_deref() == Some(signal) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            tokio::time::timeout(
                Duration::from_secs(3),
                wait_for_shutdown(|| {
                    // Signal this isolated child while still inside the ready callback.
                    // This widens the scheduling window deterministically, without sleeps
                    // or sending a signal to the parent test runner.
                    assert!(
                        Command::new("kill")
                            .args([signal, &std::process::id().to_string()])
                            .status()
                            .unwrap()
                            .success()
                    );
                }),
            )
            .await
            .expect("registered signal was not delivered")
            .expect("shutdown listener failed");
        });
        std::fs::write(std::env::var_os(CHILD_RESULT).unwrap(), signal).unwrap();
        return;
    }

    let directory = tempfile::tempdir().unwrap();
    let result_path = directory.path().join("handled-signal");
    let mut child = OwnedChild(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test_name, "--nocapture"])
            .env(CHILD_SIGNAL, signal)
            .env(CHILD_RESULT, &result_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            assert!(
                status.success(),
                "{signal} at ready bypassed shutdown: {status}"
            );
            break;
        }
        assert!(Instant::now() < deadline, "signal test child did not exit");
        std::thread::sleep(Duration::from_millis(10));
    }
    // Also detects a stale --exact name accidentally selecting zero child tests.
    assert_eq!(std::fs::read_to_string(result_path).unwrap(), signal);
}

#[test]
fn sigterm_is_registered_before_ready() {
    check_signal_at_ready("-TERM", &test_name("sigterm_is_registered_before_ready"));
}

#[test]
fn sigint_is_registered_before_ready() {
    check_signal_at_ready("-INT", &test_name("sigint_is_registered_before_ready"));
}
