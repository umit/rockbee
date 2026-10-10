#[tokio::main]
async fn main() -> rockbee_node::Result<()> {
    rockbee_node::writer::run(std::env::args().skip(1)).await
}
