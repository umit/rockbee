use rockbee_gateway::{
    Gateway,
    config::{Config, USAGE},
};
use rockbee_protocol::Result;
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
    let started = Instant::now();
    let reader_count = config.readers.len();
    let rpc = config.rpc;
    let gateway = Gateway::bind_with_config(
        &config.address,
        config.writer,
        config.readers,
        config.upstream,
        rpc,
    )
    .await?;
    let address = gateway.local_addr()?;
    gateway
        .run_with_ready(|| {
            println!(
                "{}",
                serde_json::json!({
                    "event": "ready", "role": "gateway", "addr": address.to_string(),
                    "consistency": "reader-eventual", "reader_count": reader_count,
                    "rpc_max_connections": rpc.max_connections(),
                    "rpc_global_max_in_flight": rpc.global_max_in_flight(),
                    "rpc_read_timeout_ms": rpc.read_timeout().as_millis() as u64,
                    "rpc_write_timeout_ms": rpc.write_timeout().as_millis() as u64,
                    "startup_ms": started.elapsed().as_secs_f64() * 1000.0,
                })
            );
        })
        .await
}
