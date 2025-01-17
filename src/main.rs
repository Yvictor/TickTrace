mod message;
mod server;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    let server = server::QuoteServer::new();
    server.run("127.0.0.1:5678").await?;

    Ok(())
}
