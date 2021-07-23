#[tokio::main]
async fn main() {
    pancake::frontend::http::main().await;
}
