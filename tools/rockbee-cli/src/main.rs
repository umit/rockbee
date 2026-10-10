use rockbee_client::{Client, Result};
use rockbee_protocol::Response;
const DEFAULT_ADDR: &str = "127.0.0.1:7100";

#[tokio::main]
async fn main() -> Result<()> {
    let usage = "Usage: rockbee-client [--addr HOST:PORT] put KEY VALUE [--ttl-ms N] | get KEY | delete KEY (TTL 0 means no expiry)";
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() == 1 && (args[0] == "--help" || args[0] == "-h") {
        println!("{usage}\nDefault address: {DEFAULT_ADDR}");
        return Ok(());
    }
    let (address, command) = if args.first().map(String::as_str) == Some("--addr") {
        (args.get(1).ok_or(usage)?.as_str(), &args[2..])
    } else {
        (DEFAULT_ADDR, args.as_slice())
    };

    let response = match command {
        [op, key, value, flag, ttl] if op == "put" && flag == "--ttl-ms" => {
            let ttl = std::time::Duration::from_millis(ttl.parse()?);
            Client::connect(address)
                .await?
                .put(key.as_bytes(), value.as_bytes(), Some(ttl))
                .await?;
            Response::Ok
        }
        [op, key, value] if op == "put" => {
            Client::connect(address)
                .await?
                .put(key.as_bytes(), value.as_bytes(), None)
                .await?;
            Response::Ok
        }
        [op, key] if op == "get" => {
            let value = Client::connect(address).await?.get(key.as_bytes()).await?;
            Response::Value { value }
        }
        [op, key] if op == "delete" => {
            Client::connect(address)
                .await?
                .delete(key.as_bytes())
                .await?;
            Response::Ok
        }
        _ => return Err(usage.into()),
    };
    println!("{}", serde_json::to_string(&response)?);
    Ok(())
}
