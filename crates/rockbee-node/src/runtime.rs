//! Shared process bootstrap for the fixed-role apps and legacy server.

use crate::{Result, Server, config, storage::Role};
use std::time::Instant;

/// Runs a writer or reader executable whose storage role is fixed by its package.
pub async fn run_for_role(role: Role, args: impl IntoIterator<Item = String>) -> Result<()> {
    let Some(config) = config::Config::parse_for_role(role, args)? else {
        println!("{}", config::usage_for_role(role));
        return Ok(());
    };
    run_config(config).await
}

/// Temporary compatibility runner for the legacy `rockbee-server --role …` binary.
pub async fn run_compat(args: impl IntoIterator<Item = String>) -> Result<()> {
    let Some(config) = config::Config::parse(args)? else {
        println!("{}", config::USAGE);
        return Ok(());
    };
    run_config(config).await
}

async fn run_config(config: config::Config) -> Result<()> {
    if config.check_config {
        println!(
            "{}",
            serde_json::to_string_pretty(&config.effective_json())?
        );
        return Ok(());
    }
    let started = Instant::now();
    let rpc = config.rpc;
    let role = config.role;
    let memory = config.memory.clone();
    let server = Server::from_config(config).await?;
    let (role, consistency) = match role {
        Role::Writer => ("writer", "writer-current"),
        Role::Reader => ("reader", "reader-eventual"),
    };
    let address = server.local_addr()?;
    server
        .run_with_ready(|| {
            println!(
                "{}",
                serde_json::json!({
                    "event": "ready", "addr": address.to_string(), "role": role,
                    "consistency": consistency,
                    "rpc_max_connections": rpc.max_connections(),
                    "rpc_global_max_in_flight": rpc.global_max_in_flight(),
                    "rpc_read_timeout_ms": rpc.read_timeout().as_millis() as u64,
                    "rpc_write_timeout_ms": rpc.write_timeout().as_millis() as u64,
                    "memory": memory, "startup_ms": started.elapsed().as_secs_f64() * 1000.0,
                })
            )
        })
        .await
}
