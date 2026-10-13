use rockbee_gateway::{
    Gateway,
    config::{Config, USAGE},
};
use rockbee_protocol::Result;
use std::io::{IsTerminal, Write};
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<()> {
    let Some(config) = Config::parse(std::env::args().skip(1))? else {
        println!("{USAGE}");
        return Ok(());
    };
    if config.check_config {
        println!(
            "{}",
            serde_json::to_string_pretty(&config.effective_json())?
        );
        return Ok(());
    }
    init_logging()?;
    let started = Instant::now();
    let reader_count = config.readers.len();
    let rpc = config.rpc;
    tracing::info!(role = "gateway", addr = %config.address, reader_count, "service starting");
    if std::io::stderr().is_terminal() {
        let _ = writeln!(
            std::io::stderr().lock(),
            "\n   \\ /\n  (o o)  ROCKBEE\n /|===|\\ gateway\n   ===\n"
        );
    }
    let mut address = config.address.clone();
    let mut ready = false;
    let result = async {
        let gateway = Gateway::bind_with_config(
            &config.address,
            config.writer,
            config.readers,
            config.upstream,
            rpc,
        )
        .await?;
        address = gateway.local_addr()?.to_string();
        gateway
            .run_with_ready(|| {
                ready = true;
                event(&serde_json::json!({
                    "event": "ready", "role": "gateway", "addr": address,
                    "consistency": "reader-eventual", "reader_count": reader_count,
                    "rpc_max_connections": rpc.max_connections(),
                    "rpc_global_max_in_flight": rpc.global_max_in_flight(),
                    "rpc_read_timeout_ms": rpc.read_timeout().as_millis() as u64,
                    "rpc_write_timeout_ms": rpc.write_timeout().as_millis() as u64,
                    "startup_ms": started.elapsed().as_secs_f64() * 1000.0,
                }));
                tracing::info!(role = "gateway", addr = %address, "service ready");
            })
            .await
    }
    .await;
    let mut stopped = serde_json::json!({
        "event": if result.is_ok() { "stopped" } else { "failed" },
        "role": "gateway",
        "addr": address,
        "uptime_ms": started.elapsed().as_millis(),
        "phase": if ready { "serving" } else { "startup" },
    });
    match &result {
        Ok(()) => {
            stopped["reason"] = "shutdown_signal".into();
            tracing::info!(role = "gateway", addr = %address,
                uptime_ms = started.elapsed().as_secs_f64() * 1000.0,
                reason = "shutdown_signal", "service stopped");
        }
        Err(error) => {
            stopped["error"] = error.to_string().into();
            tracing::error!(role = "gateway", addr = %address,
                phase = if ready { "serving" } else { "startup" },
                uptime_ms = started.elapsed().as_secs_f64() * 1000.0,
                error = %error, "service failed");
        }
    }
    event(&stopped);
    result
}

fn event(value: &serde_json::Value) {
    // Logging must not panic if a supervisor closes the readiness pipe.
    let _ = writeln!(std::io::stdout().lock(), "{value}");
}

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
