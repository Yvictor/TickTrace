use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, BufReader};
use anyhow::Result;
use crossbeam_queue::SegQueue;
use std::sync::Arc;
use crate::message::QuoteData;
use tracing::error;

const BUFFER_SIZE: usize = 1024 * 1024; // 1MB buffer

#[derive(Clone)]
pub struct QuoteServer {
    queue: Arc<SegQueue<QuoteData>>,
}

impl QuoteServer {
    pub fn new() -> Self {
        Self {
            queue: Arc::new(SegQueue::new()),
        }
    }

    pub async fn run(&self, addr: &str) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        println!("Server listening on {}", addr);

        loop {
            let (socket, _) = listener.accept().await?;
            self.configure_socket(&socket)?;
            
            let queue = self.queue.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket, queue).await {
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
}

async fn handle_connection(
    socket: TcpStream,
    queue: Arc<SegQueue<QuoteData>>,
) -> Result<()> {
    let mut reader = BufReader::with_capacity(BUFFER_SIZE, socket);
    let mut len_buf = [0u8; 1];

    loop {
        // Read data length
        reader.read_exact(&mut len_buf).await?;
        let data_len = len_buf[0];

        // Read MessagePack data
        let mut data = vec![0u8; data_len as usize];
        reader.read_exact(&mut data).await?;

        // Try to decode the QuoteData
        match rmp_serde::from_slice::<QuoteData>(&data) {
            Ok(quote_data) => {
                queue.push(quote_data);
            }
            Err(e) => {
                error!("Failed to decode QuoteData: {}", e);
                continue;
            }
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

    #[tokio::test]
    async fn test_server_connection_and_data_handling() {
        // Start server
        let server = QuoteServer::new();
        let server_addr = "127.0.0.1:8899";
        
        let server_handle = tokio::spawn({
            let server = server.clone();
            async move {
                server.run(server_addr).await.unwrap();
            }
        });

        // Give server time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create test data
        let quote = QuoteData {
            code: CompactString::from("TEST"),
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
        };

        // Connect to server and send data
        let mut stream = TcpStream::connect(server_addr).await.unwrap();
        
        // Serialize and send data
        let encoded = rmp_serde::to_vec(&quote).unwrap();
        let len = encoded.len() as u8;
        
        stream.write_all(&[len]).await.unwrap();
        stream.write_all(&encoded).await.unwrap();

        // Give some time for server to process
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify data was received and queued
        let received = server.queue.pop().unwrap();
        assert_eq!(received.code, "TEST");
        assert_eq!(received.open, dec!(100.00));
        assert_eq!(received.close, dec!(101.00));
        
        // Cleanup
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_invalid_data_handling() {
        let server = QuoteServer::new();
        let server_addr = "127.0.0.1:8900";
        
        let server_handle = tokio::spawn({
            let server = server.clone();
            async move {
                server.run(server_addr).await.unwrap();
            }
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Connect and send invalid data
        let mut stream = TcpStream::connect(server_addr).await.unwrap();
        let invalid_data = vec![1, 2, 3, 4]; // Invalid MessagePack data
        
        stream.write_all(&[invalid_data.len() as u8]).await.unwrap();
        stream.write_all(&invalid_data).await.unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify nothing was queued
        assert!(server.queue.is_empty());
        
        server_handle.abort();
    }
} 