use crate::message::QuoteData;
use anyhow::Result;
use arrow::datatypes::{Field, FieldRef, Schema};
use flume::Receiver;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use tracing::info;
use compact_str::CompactString;
use rust_decimal_macros::dec;
use serde_arrow::schema::{SchemaLike, TracingOptions};

pub struct QuoteConsumer {
    receiver: Receiver<QuoteData>,
    batch_size: usize,
    output_dir: String,
    current_file: Option<String>,
    fields: Vec<FieldRef>,
    schema: Arc<Schema>,
}

impl QuoteConsumer {
    pub fn new(
        receiver: Receiver<QuoteData>,
        batch_size: usize,
        output_dir: String,
    ) -> Result<Self> {
        // Create sample data for schema inference
        //let fields = Vec::<FieldRef>::from_type::<QuoteData>(TracingOptions::default())?;
        let sample = QuoteData {
            code: CompactString::from(""),
            datetime: CompactString::from(""),
            open: dec!(0),
            target_kind_price: dec!(0),
            trade_bid_vol_sum: 0,
            trade_ask_vol_sum: 0,
            avg_price: dec!(0),
            close: dec!(0),
            high: dec!(0),
            low: dec!(0),
            amount: dec!(0),
            amount_sum: dec!(0),
            volume: 0,
            vol_sum: 0,
            tick_type: 0,
            diff_type: 0,
            diff_price: dec!(0),
            diff_rate: dec!(0),
            simtrade: 0,
        };

        // Create schema from sample data
        let fields = Vec::<FieldRef>::from_samples(&[&sample], TracingOptions::default())?;
        let schema = Arc::new(Schema::new(
            fields
                .clone()
                .into_iter()
                .map(|f| f.as_ref().clone())
                .collect::<Vec<Field>>(),
        ));

        Ok(Self {
            receiver,
            batch_size,
            output_dir,
            current_file: None,
            fields,
            schema,
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        let mut quotes = Vec::with_capacity(self.batch_size);

        while let Ok(quote) = self.receiver.recv_async().await {
            quotes.push(quote);

            if quotes.len() >= self.batch_size {
                self.write_batch(&quotes)?;
                quotes.clear();
            }
        }

        if !quotes.is_empty() {
            self.write_batch(&quotes)?;
        }

        Ok(())
    }

    fn write_batch(&mut self, quotes: &[QuoteData]) -> Result<()> {
        // Convert quotes to Arrow arrays using serde_arrow
        let record_batch = serde_arrow::to_record_batch(&self.fields, &quotes)?;

        // Create new file for each batch
        let filename = format!(
            "quotes_{}.parquet",
            chrono::Local::now().format("%Y%m%d_%H%M%S")
        );
        let path = Path::new(&self.output_dir).join(&filename);

        let file = File::create(path)?;
        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(file, self.schema.clone(), Some(props))?;
        writer.write(&record_batch)?;
        writer.close()?;

        self.current_file = Some(filename.clone());
        info!("Wrote {} quotes to {}", quotes.len(), filename);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::QuoteData;
    use compact_str::CompactString;
    use rust_decimal_macros::dec;
    use std::fs;
    use tempfile::tempdir;

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

    #[tokio::test]
    async fn test_consumer_batch_writing() -> Result<()> {
        // Create temp directory for test files
        let temp_dir = tempdir()?;
        let output_dir = temp_dir.path().to_str().unwrap().to_string();

        // Setup channels and consumer
        let (sender, receiver) = flume::bounded(100);
        let mut consumer = QuoteConsumer::new(receiver, 2, output_dir.clone())?;

        // Start consumer in background
        let consumer_handle = tokio::spawn(async move {
            consumer.run().await
        });

        // Send test data
        sender.send_async(create_test_quote()).await?;
        sender.send_async(create_test_quote()).await?;
        
        // Close channel to stop consumer
        drop(sender);
        
        // Wait for consumer to finish
        consumer_handle.await??;

        // Verify output
        let files: Vec<_> = fs::read_dir(&output_dir)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry.path()
                    .extension()
                    .map_or(false, |ext| ext == "parquet")
            })
            .collect();

        assert_eq!(files.len(), 1, "Should create exactly one parquet file");

        // Verify file content using arrow-parquet
        let file = File::open(&files[0].path())?;
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(file, 128)?;
        let batches: Vec<arrow::record_batch::RecordBatch> = reader
            .collect::<Result<Vec<arrow::record_batch::RecordBatch>, _>>()?
            .into_iter()
            .collect();
        
        assert_eq!(batches.len(), 1, "Should have one batch");
        assert_eq!(batches[0].num_rows(), 2, "Batch should have 2 rows");

        Ok(())
    }

    #[tokio::test]
    async fn test_consumer_partial_batch() -> Result<()> {
        let temp_dir = tempdir()?;
        let output_dir = temp_dir.path().to_str().unwrap().to_string();
        let (sender, receiver) = flume::bounded(100);
        let mut consumer = QuoteConsumer::new(receiver, 3, output_dir.clone())?;

        let consumer_handle = tokio::spawn(async move {
            consumer.run().await
        });

        // Send only 1 quote (less than batch size)
        sender.send_async(create_test_quote()).await?;
        drop(sender);
        
        consumer_handle.await??;

        let files: Vec<_> = fs::read_dir(&output_dir)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry.path()
                    .extension()
                    .map_or(false, |ext| ext == "parquet")
            })
            .collect();

        assert_eq!(files.len(), 1, "Should write partial batch");

        let file = File::open(&files[0].path())?;
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(file, 128)?;
        let batches: Vec<arrow::record_batch::RecordBatch> = reader
            .collect::<Result<Vec<arrow::record_batch::RecordBatch>, _>>()?
            .into_iter()
            .collect();
        
        assert_eq!(batches[0].num_rows(), 1, "Should have 1 row");

        Ok(())
    }
}
