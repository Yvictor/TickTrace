use rsolace::solclient::{SessionProps, SolClient};

use rsolace::types::{SolClientLogLevel, SolClientSubscribeFlags};
use tracing_subscriber;
use std::error::Error;
use tokio::net::TcpStream;
use tokio::io::AsyncWriteExt;
use tick_trace::message::QuoFOPv2;


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
                let quote: QuoFOPv2 = rmp_serde::from_slice(&data).unwrap();
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


    solclient.subscribe_ext(
        "QUO/v2/FOP/*/TFE/RTFC5*",
        SolClientSubscribeFlags::RequestConfirm,
    );

    let mut count = 0;
    // Handle sending quotes in the main task
    // tokio::spawn(async move {
      
    // });
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
    match writer.shutdown().await {
        Ok(_) => {
            tracing::info!("TCP stream shutdown");
        }
        Err(e) => {
            tracing::error!("Failed to shutdown TCP stream: {}", e);
        }
    }
    // std::thread::sleep(std::time::Duration::from_secs(30));    
    Ok(())
}
