// Parquet file writing and partitioning
//
// This module handles writing Arrow RecordBatches to Parquet format
// and generating partition paths for time-based organization.

pub mod partition;
pub mod writer;

pub use partition::generate_partition_path;
pub use writer::write_parquet;
