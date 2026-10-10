#[tokio::main]
async fn main() -> rockbee_node::Result<()> {
    rockbee_node::run_compat(std::env::args().skip(1)).await
}
