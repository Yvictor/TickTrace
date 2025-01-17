mod message;
mod server;
mod processor;
mod consumer;

use anyhow::Result;
use flume::bounded;
use tracing::error;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    // Create channels
    let (server, raw_receiver) = server::QuoteServer::new(128*128);
    let (quote_sender, quote_receiver) = bounded(128*128);

    // Start processor
    let processor = processor::QuoteProcessor::new(raw_receiver, quote_sender);
    tokio::spawn(async move {
        if let Err(e) = processor.run().await {
            error!("Processor error: {}", e);
        }
    });

    // Start consumer
    let mut consumer = consumer::QuoteConsumer::new(
        quote_receiver,
        1024, // batch size
        "data".to_string(), // output directory
    )?;
    tokio::spawn(async move {
        if let Err(e) = consumer.run().await {
            error!("Consumer error: {}", e);
        }
    });

    // Start server
    server.run("127.0.0.1:5678").await?;

    Ok(())
}
