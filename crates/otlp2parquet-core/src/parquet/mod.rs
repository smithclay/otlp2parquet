// Parquet file writing
//
// This module handles writing Arrow RecordBatches to Parquet format.

pub mod writer;

pub use writer::write_parquet;
