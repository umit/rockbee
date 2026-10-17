#[tokio::main]
async fn main() -> rockbee_runtime::Result<()> {
    rockbee_reader::run(std::env::args().skip(1)).await
}
