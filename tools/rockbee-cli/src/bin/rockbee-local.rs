#[path = "../local/mod.rs"]
mod local;

#[tokio::main]
async fn main() -> local::Result<()> {
    local::main(std::env::args().skip(1)).await
}
