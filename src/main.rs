mod message;
mod server;
mod processor;
mod consumer;

use anyhow::Result;
use clap::Parser;
use flume::bounded;
use std::path::PathBuf;
use tracing::error;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Server listen address
    #[arg(short, long, default_value = "127.0.0.1:5678")]
    addr: String,

    /// Output directory for quote data
    #[arg(short, long, default_value = "data")]
    output_dir: PathBuf,

    /// Batch size for writing quotes
    #[arg(short, long, default_value = "1024")]
    batch_size: usize,

    /// Channel capacity
    #[arg(short, long, default_value = "16384")]
    capacity: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    // Create output directory if it doesn't exist
    std::fs::create_dir_all(&args.output_dir)?;

    // Create channels
    let (server, raw_receiver) = server::QuoteServer::new(args.capacity);
    let (quote_sender, quote_receiver) = bounded(args.capacity);

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
        args.batch_size,
        args.output_dir.to_str().unwrap().to_string(),
    )?;
    tokio::spawn(async move {
        if let Err(e) = consumer.run().await {
            error!("Consumer error: {}", e);
        }
    });

    // Start server
    server.run(&args.addr).await?;

    Ok(())
}
