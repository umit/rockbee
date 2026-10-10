mod config;
mod probe;
mod run;

pub(crate) type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const USAGE: &str = "Usage: rockbee-local probe --gateway IP:PORT --writer IP:PORT --reader IP:PORT [--reader IP:PORT ...] | rockbee-local run [--bin-dir DIR] [--writer-config PATH] [--reader-config PATH] [--endpoint URL] [--bucket NAME]";

pub(crate) async fn main(args: impl IntoIterator<Item = String>) -> Result<()> {
    let mut args = args.into_iter();
    let Some(command) = args.next() else {
        return Err(USAGE.into());
    };
    match command.as_str() {
        "probe" => probe::run(config::ProbeConfig::parse(args)?).await,
        "run" => run::run(config::RunConfig::parse(args)?).await,
        "--help" | "-h" | "help" => {
            println!("{USAGE}");
            Ok(())
        }
        _ => Err(USAGE.into()),
    }
}
