// otlp2parquet-storage - I/O and persistence layer
//
// This crate handles all storage concerns (the "accident" of distribution):
// - Parquet serialization (Arrow → Parquet bytes)
// - Content hashing (Blake3 for deduplication)
// - Partition path generation (time-based Hive-style partitioning)
// - OpenDAL storage abstraction (unified S3/R2/Filesystem access)
//
// Platform-specific entry points are in separate crates:
// - otlp2parquet-cloudflare (Cloudflare Workers + R2)
// - otlp2parquet-lambda (AWS Lambda + S3)
// - otlp2parquet-server (Axum server + multi-backend)
//
// Core transformation is in: otlp2parquet-core (OTLP → Arrow)
// Batching utilities are in: otlp2parquet-batch

pub mod opendal_storage;
pub mod parquet_writer;
pub mod partition;

// Re-export iceberg module from separate crate (optional, behind feature flag)
#[cfg(feature = "iceberg")]
pub use otlp2parquet_iceberg as iceberg;

// Re-export commonly used types
pub use opendal_storage::OpenDalStorage;
pub use otlp2parquet_core::parquet::{set_parquet_row_group_size, writer_properties};
pub use otlp2parquet_core::{Blake3Hash, ParquetWriteResult};
#[cfg(feature = "iceberg")]
pub use otlp2parquet_iceberg::{IcebergCatalog, IcebergCommitter};
pub use parquet_writer::ParquetWriter;
