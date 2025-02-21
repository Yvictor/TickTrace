use crate::message::QuoFOPv2;
use crate::writer::QuoteWriter;
use anyhow::Result;
use arrow_schema::{FieldRef, Schema};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use tracing::info;
pub struct ParquetQuoteWriter {
    output_dir: String,
    current_file: Option<String>,
    fields: Vec<FieldRef>,
    schema: Arc<Schema>,
    current_writer: Option<ArrowWriter<File>>,
    current_records: usize,
    max_records_per_file: usize,
}

impl ParquetQuoteWriter {
    pub fn new(
        output_dir: String,
        fields: Vec<FieldRef>,
        schema: Arc<Schema>,
        max_records_per_file: usize,
    ) -> Self {
        ParquetQuoteWriter {
            output_dir,
            current_file: None,
            fields,
            schema,
            current_writer: None,
            current_records: 0,
            max_records_per_file,
        }
    }

    fn create_new_writer(&mut self) -> Result<()> {
        let filename = format!(
            "quotes_{}.parquet",
            chrono::Local::now().format("%Y%m%d_%H%M%S")
        );
        let path = Path::new(&self.output_dir).join(&filename);

        let file = File::create(path)?;
        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::SNAPPY)
            .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
            .set_created_by("quote-consumer".to_string())
            .build();

        self.current_writer = Some(ArrowWriter::try_new(
            file,
            self.schema.clone(),
            Some(props),
        )?);
        self.current_file = Some(filename);
        self.current_records = 0;

        Ok(())
    }
}

impl QuoteWriter for ParquetQuoteWriter {
    fn write_batch(&mut self, quotes: &[QuoFOPv2]) -> Result<()> {
        // Convert quotes to Arrow arrays using serde_arrow
        let record_batch = serde_arrow::to_record_batch(self.fields.as_slice(), &quotes)?;

        // Create new writer if we don't have one
        if self.current_writer.is_none() {
            self.create_new_writer()?;
        }

        // Write the batch
        if let Some(writer) = &mut self.current_writer {
            writer.write(&record_batch)?;
            self.current_records += quotes.len();

            // If we've exceeded the max records per file, finish this file and create a new one
            if self.current_records >= self.max_records_per_file {
                let filename = self.current_file.clone().unwrap();
                // Take ownership of the writer before closing
                let writer = self.current_writer.take().unwrap();
                writer.close()?;
                info!(
                    "Completed file {} with {} records",
                    filename, self.current_records
                );
            }
        }

        Ok(()) // Existing write_batch implementation
    }

    fn close(&mut self) -> Result<()> {
        if let Some(writer) = self.current_writer.take() {
            writer.close()?;
        }
        Ok(())
    }
}
