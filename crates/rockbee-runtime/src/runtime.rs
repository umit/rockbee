//! Shared process bootstrap for the fixed-role applications.

use crate::{Result, Role, Server, Storage, config};
use std::io::{IsTerminal, Write};
use std::time::Instant;

/// Runs a writer or reader executable whose storage role is fixed by its package.
/// Installs the process-wide tracing subscriber after help/config validation.
/// Embedding applications should configure logging themselves and use [`Server`].
pub async fn run_for_role<S: Storage>(
    role: Role,
    args: impl IntoIterator<Item = String>,
) -> Result<()> {
    let Some(config) = config::Config::parse_for_role(role, args)? else {
        println!("{}", config::usage_for_role(role));
        return Ok(());
    };
    run_config::<S>(config).await
}

async fn run_config<S: Storage>(config: config::Config) -> Result<()> {
    if config.check_config {
        println!(
            "{}",
            serde_json::to_string_pretty(&config.effective_json())?
        );
        return Ok(());
    }
    init_logging()?;
    let started = Instant::now();
    let rpc = config.rpc;
    let memory = config.memory.clone();
    let (role, consistency) = match config.role {
        Role::Writer => ("writer", "writer-current"),
        Role::Reader => ("reader", "reader-eventual"),
    };
    tracing::info!(role, addr = %config.address, "service starting");
    // Keep machine-readable stdout intact, including when a supervisor captures it.
    if std::io::stderr().is_terminal() {
        let _ = writeln!(
            std::io::stderr().lock(),
            "\n   \\ /\n  (o o)  ROCKBEE\n /|===|\\ {role}\n   ===\n"
        );
    }
    let mut address = config.address.clone();
    let mut ready = false;
    let result = async {
        let server = Server::<S>::from_config(config).await?;
        address = server.local_addr()?.to_string();
        server
            .run_with_ready(|| {
                ready = true;
                event(&serde_json::json!({
                    "event": "ready", "addr": address, "role": role,
                    "consistency": consistency,
                    "rpc_max_connections": rpc.max_connections(),
                    "rpc_global_max_in_flight": rpc.global_max_in_flight(),
                    "rpc_read_timeout_ms": rpc.read_timeout().as_millis() as u64,
                    "rpc_write_timeout_ms": rpc.write_timeout().as_millis() as u64,
                    "memory": memory, "startup_ms": started.elapsed().as_secs_f64() * 1000.0,
                }));
                tracing::info!(role, addr = %address, "service ready");
            })
            .await
    }
    .await;
    let mut stopped = serde_json::json!({
        "event": if result.is_ok() { "stopped" } else { "failed" },
        "role": role,
        "addr": address,
        "uptime_ms": started.elapsed().as_millis(),
        "phase": if ready { "serving" } else { "startup" },
    });
    match &result {
        Ok(()) => {
            stopped["reason"] = "shutdown_signal".into();
            tracing::info!(role, addr = %address,
                uptime_ms = started.elapsed().as_secs_f64() * 1000.0,
                reason = "shutdown_signal", "service stopped");
        }
        Err(error) => {
            stopped["error"] = error.to_string().into();
            tracing::error!(role, addr = %address,
                phase = if ready { "serving" } else { "startup" },
                uptime_ms = started.elapsed().as_secs_f64() * 1000.0,
                error = %error, "service failed");
        }
    }
    // Server::run has already drained RPC owners and attempted to close storage.
    event(&stopped);
    result
}

fn event(value: &serde_json::Value) {
    // A supervisor may close its readiness pipe. Logging must not turn a clean
    // shutdown into a panic or replace the original service error.
    let _ = writeln!(std::io::stdout().lock(), "{value}");
}

// Only the executable entry points call this. Store/Server and the public client
// leave subscriber ownership to their embedding application.
fn init_logging() -> Result<()> {
    let filter = match std::env::var("RUST_LOG") {
        Ok(value) => tracing_subscriber::EnvFilter::try_new(value)
            .map_err(|error| format!("invalid RUST_LOG: {error}"))?,
        Err(std::env::VarError::NotPresent) => {
            tracing_subscriber::EnvFilter::new("warn,rockbee=info")
        }
        Err(error) => return Err(format!("invalid RUST_LOG: {error}").into()),
    };
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .compact()
        .try_init()?;
    Ok(())
}
