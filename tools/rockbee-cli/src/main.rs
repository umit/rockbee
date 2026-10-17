use rockbee_client::{Result, RockBeeClient};
use rockbee_protocol::Response;
mod status;
mod table;

const DEFAULT_ADDR: &str = "127.0.0.1:7100";
const USAGE: &str = "Usage: rockbee-client [--addr HOST:PORT] put KEY VALUE [--ttl-ms N] | get KEY | delete KEY | table-put TABLE JSON [--id ID] [--ttl-ms N] | table-get TABLE ID | table-delete TABLE ID | table-query TABLE SQL | status [--expect-role gateway|writer|reader] [--timeout-ms N] (TTL 0 means no expiry)";

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() == 1 && (args[0] == "--help" || args[0] == "-h") {
        println!("{USAGE}\nDefault address: {DEFAULT_ADDR}");
        return Ok(());
    }
    let (address, command) = if args.first().map(String::as_str) == Some("--addr") {
        (args.get(1).ok_or(USAGE)?.as_str(), &args[2..])
    } else {
        (DEFAULT_ADDR, args.as_slice())
    };

    if command.first().map(String::as_str) == Some("status") {
        return status::run(address, &command[1..]).await;
    }
    if command.first().is_some_and(|op| op.starts_with("table-")) {
        let output = table::run(address, command).await?;
        println!("{}", serde_json::to_string(&output)?);
        return Ok(());
    }

    let response = match command {
        [op, key, value, flag, ttl] if op == "put" && flag == "--ttl-ms" => {
            let ttl = std::time::Duration::from_millis(ttl.parse()?);
            RockBeeClient::connect(address)
                .await?
                .put(key.as_bytes(), value.as_bytes(), Some(ttl))
                .await?;
            Response::Ok
        }
        [op, key, value] if op == "put" => {
            RockBeeClient::connect(address)
                .await?
                .put(key.as_bytes(), value.as_bytes(), None)
                .await?;
            Response::Ok
        }
        [op, key] if op == "get" => {
            let value = RockBeeClient::connect(address)
                .await?
                .get(key.as_bytes())
                .await?;
            Response::Value { value }
        }
        [op, key] if op == "delete" => {
            RockBeeClient::connect(address)
                .await?
                .delete(key.as_bytes())
                .await?;
            Response::Ok
        }
        _ => return Err(USAGE.into()),
    };
    println!("{}", serde_json::to_string(&response)?);
    Ok(())
}
