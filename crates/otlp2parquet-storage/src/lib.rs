// otlp2parquet-storage - I/O and persistence layer
//
// This crate provides storage abstractions used across all platform implementations:
// - OpenDAL storage abstraction (unified S3/R2/Filesystem access)
// - Partition path generation (time-based Hive-style partitioning)
//
// Platform-specific entry points are in separate crates:
// - otlp2parquet-cloudflare (Cloudflare Workers + R2)
// - otlp2parquet-lambda (AWS Lambda + S3)
// - otlp2parquet-server (Axum server + multi-backend)
//
// Batching utilities are in: otlp2parquet-batch

pub mod opendal_storage;
pub mod partition;
