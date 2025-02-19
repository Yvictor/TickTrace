use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, BufReader};
use anyhow::Result;
use tracing::error;
use flume::{Sender, Receiver};

const BUFFER_SIZE: usize = 1024 * 1024; // 1MB buffer

#[derive(Clone)]
pub struct QuoteServer {
    sender: Sender<Vec<u8>>, // Raw message queue
}

impl QuoteServer {
    pub fn new(capacity: usize) -> (Self, Receiver<Vec<u8>>) {
        let (sender, receiver) = flume::bounded(capacity);
        (Self { sender }, receiver)
    }

    pub async fn run(&self, addr: &str) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        println!("Server listening on {}", addr);

        loop {
            let (socket, _) = listener.accept().await?;
            self.configure_socket(&socket)?;
            
            let sender = self.sender.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket, sender).await {
                    error!("Connection error: {}", e);
                }
            });
        }
    }

    fn configure_socket(&self, socket: &TcpStream) -> Result<()> {
        use std::os::unix::io::AsRawFd;
        
        let fd = socket.as_raw_fd();
        unsafe {
            // Set receive buffer size
            let buf_size = 8 * 1024 * 1024; // 8MB
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                &buf_size as *const _ as *const libc::c_void,
                std::mem::size_of_val(&buf_size) as libc::socklen_t,
            );
        }
        Ok(())
    }

    pub fn is_full(&self) -> bool {
        self.sender.is_full()
    }

    pub fn len(&self) -> usize {
        self.sender.len()
    }
}

async fn handle_connection(
    socket: TcpStream,
    sender: Sender<Vec<u8>>,
) -> Result<()> {
    let mut reader = BufReader::with_capacity(BUFFER_SIZE, socket);
    let mut topic_len_buf = [0u8; 1];
    let mut data_len_buf = [0u8; 1];

    loop {
        reader.read_exact(&mut topic_len_buf).await?;
        let topic_len = topic_len_buf[0];
        let mut topic = vec![0u8; topic_len as usize];
        reader.read_exact(&mut topic).await?;

        reader.read_exact(&mut data_len_buf).await?;
        let data_len = data_len_buf[0];
        let mut data = vec![0u8; data_len as usize];
        reader.read_exact(&mut data).await?;
        
        if let Err(e) = sender.send_async(data).await {
            error!("Failed to queue message: {}", e);
            continue;
        }
    }
} 

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpStream;
    use tokio::io::AsyncWriteExt;
    use rust_decimal_macros::dec;
    use std::time::Duration;
    use compact_str::CompactString;
    use crate::message::QuoFOPv2;
    use flume::bounded;
    use crate::processor::QuoteProcessor;

    #[tokio::test]
    async fn test_server_connection_and_data_handling() {
        // Start server
        let (server, receiver) = QuoteServer::new(1000);
        let server_addr = "127.0.0.1:8899";
        
        let server_handle = tokio::spawn({
            let server = server.clone();
            async move {
                server.run(server_addr).await.unwrap();
            }
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create test data
        let quote = QuoFOPv2 {
            code: CompactString::from("TEST"),
            date: CompactString::from("2024-03-20"),
            time: CompactString::from("10:30:00"),
            target_kind_price: dec!(101.00),
            open: dec!(100.00),
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
            trade_bid_vol_sum: 500,
            trade_ask_vol_sum: 600,
            trade_bid_cnt: 100,
            trade_ask_cnt: 120,
            bid_price: [dec!(101.00); 5],
            bid_volume: [500; 5],
            diff_bid_vol: [0; 5],
            ask_price: [dec!(101.00); 5],
            ask_volume: [500; 5],
            diff_ask_vol: [0; 5],
            first_derived_bid_price: dec!(101.00),
            first_derived_ask_price: dec!(101.00),
            first_derived_bid_volume: 500,
            first_derived_ask_volume: 500,
            simtrade: 0,
        };

        // Connect and send data
        let mut stream = TcpStream::connect(server_addr).await.unwrap();
        
        // Send topic first
        let topic = "quotes";
        stream.write_all(&[topic.len() as u8]).await.unwrap();
        stream.write_all(topic.as_bytes()).await.unwrap();

        // Then send MessagePack data
        let encoded = rmp_serde::to_vec(&quote).unwrap();
        stream.write_all(&[encoded.len() as u8]).await.unwrap();
        stream.write_all(&encoded).await.unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify data was received and queued
        let received = receiver.recv_async().await.unwrap();
        let decoded = rmp_serde::from_slice::<QuoFOPv2>(&received).unwrap();
        assert_eq!(decoded.code, "TEST");
        assert_eq!(decoded.open, dec!(100.00));
        assert_eq!(decoded.close, dec!(101.00));
        
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_invalid_data_handling() {
        let (server, raw_receiver) = QuoteServer::new(1000);
        let (quote_sender, quote_receiver) = bounded(1000);
        let server_addr = "127.0.0.1:8900";
        
        // Start server
        let server_handle = tokio::spawn({
            let server = server.clone();
            async move {
                server.run(server_addr).await.unwrap();
            }
        });

        // Start processor
        let processor = QuoteProcessor::new(raw_receiver, quote_sender);
        let processor_handle = tokio::spawn(async move {
            processor.run().await.unwrap();
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Connect and send invalid data
        let mut stream = TcpStream::connect(server_addr).await.unwrap();
        
        // Send topic
        let topic = "quotes";
        stream.write_all(&[topic.len() as u8]).await.unwrap();
        stream.write_all(topic.as_bytes()).await.unwrap();

        // Send invalid MessagePack data
        let invalid_data = vec![1, 2, 3, 4];
        stream.write_all(&[invalid_data.len() as u8]).await.unwrap();
        stream.write_all(&invalid_data).await.unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify no valid quotes were processed
        assert!(quote_receiver.is_empty());
        
        server_handle.abort();
        processor_handle.abort();
    }
} 