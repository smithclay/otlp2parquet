// otlp2parquet-core - Platform-agnostic core logic
//
// This crate contains the core processing logic for converting
// OTLP logs to Parquet files. It is platform-agnostic and can be
// used by different runtime adapters (Cloudflare Workers, Lambda, etc.)

pub mod otlp;
pub mod parquet;
pub mod schema;
pub mod storage;

// Re-export commonly used types
pub use schema::otel_logs_schema;
pub use storage::Storage;
