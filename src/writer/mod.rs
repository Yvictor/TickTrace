pub mod parquet;

use anyhow::Result;
use crate::message::QuoFOPv2;

pub trait QuoteWriter: Send {
    fn write_batch(&mut self, quotes: &[QuoFOPv2]) -> Result<()>;
    fn close(&mut self) -> Result<()>;
}
