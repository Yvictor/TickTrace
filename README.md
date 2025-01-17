# TickTrace

A high-performance market data collection service built with Rust and Tokio.

## Features

- High-throughput TCP socket server for real-time quote data collection
- Supports up to 100,000 messages per second
- Message format: [length][topic][length][data(msgpack)]
- In-memory queue for efficient data handling
- Parquet file storage for data persistence
- Optimized socket buffer settings for high-performance data ingestion

## Technical Stack

- Rust
- Tokio async runtime
- Apache Arrow/Parquet
- MessagePack serialization
- Lock-free queuing system
- TonboDB

## Project Structure