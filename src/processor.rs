use anyhow::Result;
use crate::message::QuoteData;
use flume::{Receiver, Sender};
use tracing::error;

pub struct QuoteProcessor {
    raw_receiver: Receiver<Vec<u8>>,
    quote_sender: Sender<QuoteData>,
}

impl QuoteProcessor {
    pub fn new(
        raw_receiver: Receiver<Vec<u8>>, 
        quote_sender: Sender<QuoteData>
    ) -> Self {
        Self {
            raw_receiver,
            quote_sender,
        }
    }

    pub async fn run(&self) -> Result<()> {
        let mut batch = Vec::with_capacity(100);
        
        while let Ok(data) = self.raw_receiver.recv_async().await {
            match rmp_serde::from_slice::<QuoteData>(&data) {
                Ok(quote) => {
                    batch.push(quote);
                    if batch.len() >= 100 {
                        for quote in batch.drain(..) {
                            if let Err(e) = self.quote_sender.send_async(quote).await {
                                error!("Failed to send parsed quote: {}", e);
                            }
                        }
                    }
                }
                Err(e) => error!("Failed to parse quote data: {}", e),
            }
        }
        Ok(())
    }
} 