use rsolace::solclient::{SessionProps, SolClient};

use rsolace::types::{SolClientLogLevel, SolClientSubscribeFlags};
use tracing_subscriber;
use std::error::Error;
use serde::{Deserialize, Serialize};
use rust_decimal::Decimal;
use compact_str::CompactString;
use tokio::net::TcpStream;
use tokio::io::AsyncWriteExt;


#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct QuoteData {
    pub code: CompactString,
    pub date: CompactString, // NaiveDateTime,
    pub time: CompactString, // NaiveDateTime,
    pub target_kind_price: Decimal,
    pub open: Decimal,
    pub avg_price: Decimal,
    pub close: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub amount: Decimal,
    pub amount_sum: Decimal,
    pub volume: i64,
    pub vol_sum: i64,
    pub tick_type: i32,
    pub diff_type: i32,
    pub diff_price: Decimal,
    pub diff_rate: Decimal,
    pub trade_bid_vol_sum: i64,
    pub trade_ask_vol_sum: i64,
    pub trade_bid_cnt: i64,
    pub trade_ask_cnt: i64,
    pub bid_price: [Decimal; 5],
    pub bid_volume: [i64; 5],
    pub diff_bid_vol: [i64; 5],
    pub ask_price: [Decimal; 5],
    pub ask_volume: [i64; 5],
    pub diff_ask_vol: [i64; 5],
    pub first_derived_bid_price: Decimal,
    pub first_derived_ask_price: Decimal,
    pub first_derived_bid_volume: i64,
    pub first_derived_ask_volume: i64,
    pub simtrade: i32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv()?;
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    let mut solclient = SolClient::new(SolClientLogLevel::Notice)?;
    let props = SessionProps::default()
    .host(&std::env::var("SOLACE_HOST")?)
    .vpn(&std::env::var("SOLACE_VPN")?)
    .username(&std::env::var("SOLACE_USERNAME")?)
    .password(&std::env::var("SOLACE_PASSWORD")?)
    .reapply_subscriptions(true)
    .connect_retries(1)
    .connect_timeout_ms(3000)
    .compression_level(5);

    let client = TcpStream::connect("127.0.0.1:5678").await?;
    let (_reader, mut writer) = client.into_split();
    
    // Create a channel for quotes
    let (quote_sender, quote_receiver) = flume::bounded(1024);

    let event_recv = solclient.get_event_receiver();
    let _th_event = std::thread::spawn(move || loop {
        match event_recv.recv() {
            Ok(event) => {
                tracing::info!("{:?}", event);
            }
            Err(e) => {
                tracing::error!("recv event error: {:?}", e);
                break;
            }
        }
    });

    let msg_recv = solclient.get_msg_receiver();
    let _th_msg = std::thread::spawn(move || loop {
        match msg_recv.recv() {
            Ok(msg) => {
                let topic = msg.get_topic().unwrap();
                let data = msg.get_binary_attachment().unwrap();
                tracing::info!(
                    "msg1 {} {} {:?}",
                    msg.get_topic().unwrap(),
                    msg.get_sender_dt()
                        .unwrap_or(chrono::prelude::Utc::now())
                        .to_rfc3339(),
                    data
                );
                let quote: QuoteData = rmp_serde::from_slice(&data).unwrap();
                tracing::info!("quote: {:?}", quote);
                let topic_len = topic.len() as u8;
                let data_len = data.len() as u8;
                let topic_bytes = topic.as_bytes();
                let mut quote_formated = Vec::new();
                quote_formated.push(topic_len);
                quote_formated.extend_from_slice(topic_bytes);
                quote_formated.push(data_len);
                quote_formated.extend_from_slice(&data);

                // Send quote to channel instead of directly writing
                if let Err(e) = quote_sender.send(quote_formated) {
                    tracing::error!("Failed to send quote to channel: {}", e);
                }
            }
            Err(e) => {
                tracing::error!("recv msg error: {:?}", e);
                break;
            }
        }
    });

    let r = solclient.connect(props);
    tracing::info!("connect: {}", r);

    solclient.subscribe_ext(
        "QUO/v2/FOP/*/TFE/TXF*",
        SolClientSubscribeFlags::RequestConfirm,
    );

    let mut count = 0;
    // Handle sending quotes in the main task
    tokio::spawn(async move {
        while let Ok(quote) = quote_receiver.recv_async().await {
            match writer.write_all(&quote).await {
                Ok(_) => {
                    count += 1;
                    tracing::info!("quote sent: {}", count);
                },
                Err(e) => {
                    tracing::error!("Failed to write to TCP stream: {}", e);
                }
            }
        }
    });

    std::thread::sleep(std::time::Duration::from_secs(30));
    Ok(())
}
