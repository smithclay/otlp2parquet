// otlp2parquet-runtime - Shared runtime utilities
//
// This crate provides shared utilities used across all platform implementations:
// - Partition path generation (time-based Hive-style partitioning)
// - Batch management (optional batching for efficiency)
// - OpenDAL storage abstraction (unified S3/R2/Filesystem access)
//
// Platform-specific entry points are in separate crates:
// - otlp2parquet-cloudflare (Cloudflare Workers + R2)
// - otlp2parquet-lambda (AWS Lambda + S3)
// - otlp2parquet-server (Axum server + multi-backend)

pub mod batcher;
pub mod opendal_storage;
pub mod partition;
