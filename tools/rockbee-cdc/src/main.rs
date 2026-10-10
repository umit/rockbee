use rockbee_cdc::{CDC_USAGE, CdcConfig, run};
use rockbee_protocol::Result;
#[tokio::main]
async fn main() -> Result<()> {
    let Some(config) = CdcConfig::parse(std::env::args().skip(1))? else {
        println!("{CDC_USAGE}");
        return Ok(());
    };
    run(config).await
}
