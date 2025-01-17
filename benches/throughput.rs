use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use flume::bounded;
use rust_decimal_macros::dec;
use std::time::Duration;
use tick_trace::{
    message::QuoteData,
    processor::QuoteProcessor,
    server::QuoteServer,
    consumer::QuoteConsumer,
};
use compact_str::CompactString;
use tempfile::tempdir;
use tokio::net::TcpStream;
use tokio::io::AsyncWriteExt;

fn create_test_quote() -> QuoteData {
    QuoteData {
        code: CompactString::from("2330"),
        datetime: CompactString::from("2024-03-20 10:30:00"),
        open: dec!(100.00),
        target_kind_price: dec!(101.00),
        trade_bid_vol_sum: 500,
        trade_ask_vol_sum: 600,
        avg_price: dec!(100.50),
        close: dec!(101.00),
        high: dec!(101.50),
        low: dec!(99.50),
        amount: dec!(50000.00),
        amount_sum: dec!(150000.00),
        volume: 500,
        vol_sum: 1500,
        tick_type: 1,
        diff_type: 1,
        diff_price: dec!(1.00),
        diff_rate: dec!(1.0),
        simtrade: 0,
    }
}

async fn benchmark_pipeline(num_messages: usize) {
    let temp_dir = tempdir().unwrap();
    let output_dir = temp_dir.path().to_str().unwrap().to_string();
    
    // Create output directory
    std::fs::create_dir_all(&output_dir).unwrap();
    
    // Setup pipeline
    let (server, raw_receiver) = QuoteServer::new(16384);
    let (quote_sender, quote_receiver) = bounded(16384);
    let processor = QuoteProcessor::new(raw_receiver, quote_sender);
    let mut consumer = QuoteConsumer::new(quote_receiver, 1024, output_dir).unwrap();

    // Start components
    let server_handle = tokio::spawn({
        let server = server.clone();
        async move {
            server.run("127.0.0.1:5679").await.unwrap();
        }
    });

    let processor_handle = tokio::spawn(async move {
        processor.run().await.unwrap();
    });

    let consumer_handle = tokio::spawn(async move {
        consumer.run().await.unwrap();
    });

    // Wait for server to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect and send test data
    let mut stream = TcpStream::connect("127.0.0.1:5679").await.unwrap();
    let quote = create_test_quote();
    let encoded = rmp_serde::to_vec(&quote).unwrap();
    
    let topic = "quotes";
    for _ in 0..num_messages {
        stream.write_all(&[topic.len() as u8]).await.unwrap();
        stream.write_all(topic.as_bytes()).await.unwrap();
        stream.write_all(&[encoded.len() as u8]).await.unwrap();
        stream.write_all(&encoded).await.unwrap();
    }

    // Cleanup
    drop(stream);
    server_handle.abort();
    processor_handle.abort();
    consumer_handle.abort();
}

fn criterion_benchmark(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("throughput");
    for size in [1000, 10000, 100000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_function(format!("pipeline_{}", size), |b| {
            b.to_async(&rt).iter(|| benchmark_pipeline(*size));
        });
    }
    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .measurement_time(Duration::from_secs(20));
    targets = criterion_benchmark
);
criterion_main!(benches); 