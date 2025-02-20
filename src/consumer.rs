use crate::message::QuoFOPv2;
use anyhow::Result;
use arrow_schema::{Field, FieldRef, Schema, DataType};
use flume::Receiver;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
// use serde_arrow::schema::{SchemaLike, TracingOptions};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use tracing::info;


pub struct QuoteConsumer {
    receiver: Receiver<QuoFOPv2>,
    batch_size: usize,
    output_dir: String,
    current_file: Option<String>,
    fields: Vec<FieldRef>,
    schema: Arc<Schema>,
}


impl QuoteConsumer {
    fn create_schema() -> Result<(Vec<FieldRef>, Arc<Schema>)> {
        let precision = 20;
        let scale = 4;
        let fields: Vec<FieldRef> = vec![
            Field::new("code", DataType::Utf8, false),
            Field::new("date", DataType::Utf8, false),
            Field::new("time", DataType::Utf8, false),
            Field::new("target_kind_price", DataType::Decimal128(precision, scale), false),
            Field::new("open", DataType::Decimal128(precision, scale), false),
            Field::new("avg_price", DataType::Decimal128(precision, scale), false),
            Field::new("close", DataType::Decimal128(precision, scale), false),
            Field::new("high", DataType::Decimal128(precision, scale), false),
            Field::new("low", DataType::Decimal128(precision, scale), false),
            Field::new("amount", DataType::Decimal128(precision, scale), false),
            Field::new("amount_sum", DataType::Decimal128(precision, scale), false),
            Field::new("volume", DataType::Int64, false),
            Field::new("vol_sum", DataType::Int64, false),
            Field::new("tick_type", DataType::Int32, false),
            Field::new("diff_type", DataType::Int32, false),
            Field::new("diff_price", DataType::Decimal128(precision, scale), false),
            Field::new("diff_rate", DataType::Decimal128(precision, scale), false),
            Field::new("trade_bid_vol_sum", DataType::Int64, false),
            Field::new("trade_ask_vol_sum", DataType::Int64, false),
            Field::new("trade_bid_cnt", DataType::Int64, false),
            Field::new("trade_ask_cnt", DataType::Int64, false),
            Field::new(
                "bid_price",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Decimal128(precision, scale), false)),
                    5,
                ),
                false,
            ),
            Field::new(
                "bid_volume",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int64, false)), 5),
                false,
            ),
            Field::new(
                "diff_bid_vol",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int64, false)), 5),
                false,
            ),
            Field::new(
                "ask_price",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Decimal128(precision, scale), false)),
                    5,
                ),
                false,
            ),
            Field::new(
                "ask_volume",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int64, false)), 5),
                false,
            ),
            Field::new(
                "diff_ask_vol",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int64, false)), 5),
                false,
            ),
            Field::new("first_derived_bid_price", DataType::Decimal128(precision, scale), false),
            Field::new("first_derived_ask_price", DataType::Decimal128(precision, scale), false),
            Field::new("first_derived_bid_volume", DataType::Int64, false),
            Field::new("first_derived_ask_volume", DataType::Int64, false),
            Field::new("simtrade", DataType::Int32, false),
        ].into_iter().map(|f| Arc::new(f)).collect();   

        // let fields = Vec::<FieldRef>::from_type::<QuoFOPv2>(TracingOptions::default())?;
        // let fields: Vec<FieldRef> = SchemaLike::from_type::<QuoFOPv2>(TracingOptions::default())?.into_iter().map(|f| Arc::new(f)).collect();
        // let sample = QuoFOPv2::default();
        // let fields = Vec::<FieldRef>::from_samples(&[&sample], TracingOptions::default())?;
        // let fields = Vec::<FieldRef>::from_type::<Record>(TracingOptions::default())?;
        // let fields: Vec<FieldRef> = SchemaLike::from_samples(&[&sample], TracingOptions::default())?;
        let schema = Arc::new(Schema::new(
            fields
                .clone()
                .into_iter()
                .map(|f| f.as_ref().clone())
                .collect::<Vec<Field>>(),
        ));

        // let field_refs = fields.iter().map(|f| Arc::new(f.clone())).collect();
        // let schema = Arc::new(Schema::new(fields));

        Ok((fields, schema))
    }

    pub fn new(
        receiver: Receiver<QuoFOPv2>,
        batch_size: usize,
        output_dir: String,
    ) -> Result<Self> {
        let (fields, schema) = Self::create_schema()?;

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

    fn write_batch(&mut self, quotes: &[QuoFOPv2]) -> Result<()> {
        // Convert quotes to Arrow arrays using serde_arrow
        let record_batch = serde_arrow::to_record_batch(self.fields.as_slice(), &quotes)?;

        // Create new file for each batch
        let filename = format!(
            "quotes_{}.parquet",
            chrono::Local::now().format("%Y%m%d_%H%M%S_%6f")
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
    use crate::message::QuoFOPv2;
    use compact_str::CompactString;
    use rust_decimal_macros::dec;
    use std::fs;
    use tempfile::tempdir;
    use arrow::datatypes::DataType;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::basic::LogicalType;
    use parquet::basic::Type;

    fn create_test_quote() -> QuoFOPv2 {
        QuoFOPv2 {
            code: CompactString::from("2330"),
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
        let consumer_handle = tokio::spawn(async move { consumer.run().await });

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
                entry
                    .path()
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

        let consumer_handle = tokio::spawn(async move { consumer.run().await });

        // Send only 1 quote (less than batch size)
        sender.send_async(create_test_quote()).await?;
        drop(sender);

        consumer_handle.await??;

        let files: Vec<_> = fs::read_dir(&output_dir)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .path()
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

    #[test]
    fn test_schema_creation() -> Result<()> {
        let expected_precision = 20;
        let expected_scale = 4;
        let (_fields, schema) = QuoteConsumer::create_schema()?;

        // Helper function to check decimal type with precision and scale
        let is_decimal_type = |field_name: &str, precision: u8, scale: i8| {
            let field = schema.field_with_name(field_name).unwrap();
            matches!(
                field.data_type(),
                DataType::Decimal128(p, s) if *p == precision && *s == scale
            )
        };

        // Test decimal fields
        assert!(is_decimal_type("target_kind_price", expected_precision, expected_scale));
        assert!(is_decimal_type("open", expected_precision, expected_scale));
        assert!(is_decimal_type("avg_price", expected_precision, expected_scale));
        assert!(is_decimal_type("close", expected_precision, expected_scale));
        assert!(is_decimal_type("high", expected_precision, expected_scale));
        assert!(is_decimal_type("low", expected_precision, expected_scale));
        assert!(is_decimal_type("amount", expected_precision, expected_scale));
        assert!(is_decimal_type("amount_sum", expected_precision, expected_scale));
        assert!(is_decimal_type("diff_price", expected_precision, expected_scale));
        assert!(is_decimal_type("diff_rate", expected_precision, expected_scale));

        // Test array fields
        let test_fixed_size_list = |field_name: &str, expected_type: DataType| {
            let field = schema.field_with_name(field_name).unwrap();
            match field.data_type() {
                DataType::FixedSizeList(item_field, size) => {
                    assert_eq!(*size, 5, "Fixed size list should have size 5");
                    assert_eq!(item_field.data_type(), &expected_type);
                }
                _ => panic!("Field {} should be a FixedSizeList", field_name),
            }
        };

        test_fixed_size_list("bid_price", DataType::Decimal128(expected_precision, expected_scale));
        test_fixed_size_list("ask_price", DataType::Decimal128(expected_precision, expected_scale));
        test_fixed_size_list("bid_volume", DataType::Int64);
        test_fixed_size_list("ask_volume", DataType::Int64);

        // Test string fields
        assert!(matches!(
            schema.field_with_name("code").unwrap().data_type(),
            DataType::Utf8
        ));
        assert!(matches!(
            schema.field_with_name("date").unwrap().data_type(),
            DataType::Utf8
        ));
        assert!(matches!(
            schema.field_with_name("time").unwrap().data_type(),
            DataType::Utf8
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_parquet_file_schema() -> Result<()> {
        let expected_precision = 20;
        let expected_scale = 4;
        // Setup test environment
        let temp_dir = tempdir()?;
        let output_dir = temp_dir.path().to_str().unwrap().to_string();
        let (sender, receiver) = flume::bounded(100);
        let mut consumer = QuoteConsumer::new(receiver, 1, output_dir.clone())?;

        // Write one quote to create a parquet file
        let consumer_handle = tokio::spawn(async move { consumer.run().await });
        sender.send_async(create_test_quote()).await?;
        drop(sender);
        consumer_handle.await??;

        // Get the written parquet file
        let files: Vec<_> = fs::read_dir(&output_dir)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.path().extension().map_or(false, |ext| ext == "parquet"))
            .collect();
        assert_eq!(files.len(), 1);

        // let file = tokio::fs::File::open(files[0].path()).await.unwrap();
        let file = File::open(files[0].path()).unwrap();
        // Read the parquet file schema
        // Configure options for reading from the async souce
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
                .unwrap();
        let schema = builder.parquet_schema();

        // Helper function to check decimal type
        let is_decimal_type = |field_name: &str, p: i32, s: i32| {
            let field = schema.columns().iter().find(|f| f.name() == field_name).unwrap();
            println!("field: {:?}", field);
            matches!(
                field.logical_type(),
                Some(LogicalType::Decimal{scale, precision} )if scale == s && precision == p
            )
        };

        // Verify decimal fields
        assert!(is_decimal_type("target_kind_price", expected_precision, expected_scale), "target_kind_price has wrong type");
        // assert!(false);
        assert!(is_decimal_type("open", expected_precision, expected_scale), "open has wrong type");
        assert!(is_decimal_type("avg_price", expected_precision, expected_scale), "avg_price has wrong type");
        assert!(is_decimal_type("close", expected_precision, expected_scale), "close has wrong type");
        assert!(is_decimal_type("high", expected_precision, expected_scale), "high has wrong type");
        assert!(is_decimal_type("low", expected_precision, expected_scale), "low has wrong type");
        assert!(is_decimal_type("amount", expected_precision, expected_scale), "amount has wrong type");
        assert!(is_decimal_type("amount_sum", expected_precision, expected_scale), "amount_sum has wrong type");
        assert!(is_decimal_type("diff_price", expected_precision, expected_scale), "diff_price has wrong type");
        assert!(is_decimal_type("diff_rate", expected_precision, expected_scale), "diff_rate has wrong type");

        // Test array fields
        // let test_fixed_size_list = |field_name: &str, expected_type: DataType| {
        //     let field = schema.columns().iter().find(|f| f.name() == field_name).unwrap();
        //     match field.physical_type() {
        //         PhysicalType::FixedSizeList(item_field, size) => {
        //             assert_eq!(*size, 5, "Fixed size list should have size 5");
        //             assert_eq!(
        //                 item_field.data_type(),
        //                 &expected_type,
        //                 "Field {} has wrong element type",
        //                 field_name
        //             );
        //         }
        //         _ => panic!("Field {} should be a FixedSizeList", field_name),
        //     }
        // };

        // test_fixed_size_list("bid_price", DataType::Decimal128(16_u8, 6_i8));
        // test_fixed_size_list("ask_price", DataType::Decimal128(16_u8, 6_i8));
        // test_fixed_size_list("bid_volume", DataType::Int64);
        // test_fixed_size_list("ask_volume", DataType::Int64);
        // test_fixed_size_list("diff_bid_vol", DataType::Int64);
        // test_fixed_size_list("diff_ask_vol", DataType::Int64);

        // Test string fields
        assert!(
            matches!(schema.columns().iter().find(|f| f.name() == "code").unwrap().physical_type(), Type::BYTE_ARRAY),
            "code should be Utf8"
        );
        assert!(
            matches!(schema.columns().iter().find(|f| f.name() == "date").unwrap().physical_type(), Type::BYTE_ARRAY),
            "date should be Utf8"
        );
        assert!(
            matches!(schema.columns().iter().find(|f| f.name() == "time").unwrap().physical_type(), Type::BYTE_ARRAY),
            "time should be Utf8"
        );

        // Test integer fields
        assert!(
            matches!(schema.columns().iter().find(|f| f.name() == "volume").unwrap().physical_type(), Type::INT64),
            "volume should be Int64"
        );
        assert!(
            matches!(schema.columns().iter().find(|f| f.name() == "vol_sum").unwrap().physical_type(), Type::INT64),
            "vol_sum should be Int64"
        );
        assert!(
            matches!(schema.columns().iter().find(|f| f.name() == "tick_type").unwrap().physical_type(), Type::INT32),
            "tick_type should be Int32"
        );
        assert!(
            matches!(schema.columns().iter().find(|f| f.name() == "simtrade").unwrap().physical_type(), Type::INT32),
            "simtrade should be Int32"
        );

        Ok(())
    }
}
